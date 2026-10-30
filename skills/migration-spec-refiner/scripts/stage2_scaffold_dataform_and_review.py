"""
Stage 2: Deterministic Dataform Scaffolding, 5 Coder Artifacts, Canonical DAG YAML & T2.9 Auto-Healing.

Generates 100% deterministically via Python:
1. definitions/01_sources/*.sqlx:
   - Reuses declarations already on main;
   - Injects database: utils.projectId(...) when source is Cross-Project (p-*/i-* vs a-*).
2. definitions/03_silver/<domain>/<table>.sqlx (or 04_gold):
   - References sources exclusively via ${ref("schema", "table")} (2 arguments).
   - No hardcoded project or dataset names in SQL body.
3. definitions/05_assertions/<domain>/assert_<table>.sqlx:
   - Mandatory dependencies: ["<target_table>"] in config {} so assertion runs after table materialization.
4. definitions/06_legacy_contracts/<domain>/vw_<table>.sqlx:
   - Mandatory legacy contract view for backwards compatibility.
5. dags/<region>/<dag_id>.yaml:
   - Canonical Airflow DAG Factory schema (dag_id, schedule, tags, tasks with operator/bash_command).
   - Injects "vars": {"project_id": "<canonical_source_project>"} when running in analytic project (a-*) compiling from ingestion (p-*/i-*).

Anti-Placeholder Detector (NEEDS_SURGICAL_LLM):
- Scans SQL body for SELECT *, 1=1, TODO, PLACEHOLDER.
- If found, flags NEEDS_SURGICAL_LLM and generates ~5 KB surgical prompt for LLM.

Auto-Healing (Rule T2.9):
- Reconciles Section 3.2 Required Datasets table with exact union of Sources + Targets.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Set

logger = logging.getLogger("refiner.stage2-scaffold")


class DataformScaffolder:
    """Scaffolds all 5 Dataform coder artifacts and Airflow DAG Factory YAML."""

    def scaffold(
        self,
        locked_context: Dict[str, Any],
        existing_spec_markdown: Optional[str] = None,
        output_dir: str = "./refinement_artifacts",
        dataform_main_repo_path: Optional[str] = None
    ) -> Dict[str, Any]:
        target_project = locked_context["target_project"]
        target_dataset = locked_context["target_dataset"]
        target_table = locked_context["target_table"]
        table_type = locked_context.get("table_type", "incremental")
        unique_keys = locked_context.get("unique_keys", ["id"])
        partition_by = locked_context.get("partition_by")
        cluster_by = locked_context.get("cluster_by", [])
        canonical_sources = locked_context.get("canonical_sources", [])
        legacy_code = locked_context.get("legacy_source_code", "")

        domain = target_dataset.replace("silver_", "").replace("gold_", "").replace("bronze_", "")
        layer_folder = "04_gold" if "gold" in target_dataset else "03_silver"
        region = locked_context.get("region", "southamerica-east1")

        out_path = Path(output_dir)
        definitions_dir = out_path / "definitions"
        sources_dir = definitions_dir / "01_sources"
        layer_dir = definitions_dir / layer_folder / domain
        assertions_dir = definitions_dir / "05_assertions" / domain
        legacy_contracts_dir = definitions_dir / "06_legacy_contracts" / domain
        dags_dir = out_path / "dags" / region

        for d in [sources_dir, layer_dir, assertions_dir, legacy_contracts_dir, dags_dir]:
            d.mkdir(parents=True, exist_ok=True)

        scaffolded_files: Dict[str, str] = {}

        # -------------------------------------------------------------
        # ARTIFACT 1: definitions/01_sources/*.sqlx
        # -------------------------------------------------------------
        existing_sources_in_main = self._get_existing_sources_in_main(dataform_main_repo_path)

        for src in canonical_sources:
            s_proj = src.get("project_id", locked_context.get("upstream_project", "acme-lakehouse-raw"))
            s_dataset = src.get("dataset_id", "bronze_raw")
            s_table = src.get("table_id", "source_table")
            source_key = f"{s_dataset}.{s_table}"

            if source_key in existing_sources_in_main:
                logger.info(f"Source declaration '{source_key}' already exists in main repo; reusing without duplicating.")
                continue

            # Cross-project detection: if source is p-* / i-* and target is a-*
            is_cross_project = self._is_cross_project(target_project, s_proj)
            if is_cross_project:
                database_expr = f'database: utils.projectId("{s_proj}"),\n'
            else:
                database_expr = f'database: "{s_proj}",\n'

            file_name = f"declaration_{s_dataset}_{s_table}.sqlx"
            sqlx_decl = (
                f"config {{\n"
                f'  type: "declaration",\n'
                f"  {database_expr}"
                f'  schema: "{s_dataset}",\n'
                f'  name: "{s_table}",\n'
                f'  description: "Canonical source declaration managed by SSOT catalog"\n'
                f"}}\n"
            )
            file_p = sources_dir / file_name
            file_p.write_text(sqlx_decl, encoding="utf-8")
            scaffolded_files[str(file_p)] = sqlx_decl

        # -------------------------------------------------------------
        # ARTIFACT 2: definitions/03_silver/<domain>/<table>.sqlx (or 04_gold)
        # -------------------------------------------------------------
        unique_keys_formatted = json.dumps(unique_keys)
        cluster_by_formatted = json.dumps(cluster_by)
        partition_line = f'  partitionBy: "{partition_by}",\n' if partition_by else ""
        cluster_line = f"  clusterBy: {cluster_by_formatted},\n" if cluster_by else ""

        model_sqlx_header = (
            f"config {{\n"
            f'  type: "{table_type}",\n'
            f'  schema: "{target_dataset}",\n'
            f'  name: "{target_table}",\n'
            f'  description: "Target modernized BigQuery model for {target_table}",\n'
            f"  tags: [\"modernization\", \"{target_dataset}\"],\n"
            f"  uniqueKey: {unique_keys_formatted},\n"
            f"{partition_line}"
            f"{cluster_line}"
            f"}}\n\n"
        )

        # 2-arg ref: ${ref("schema", "table")}
        ref_snippets = []
        for s in canonical_sources:
            ref_snippets.append(f'${{ref("{s.get("dataset_id")}", "{s.get("table_id")}")}}')
        primary_ref = ref_snippets[0] if ref_snippets else f'${{ref("raw", "source_table")}}'

        model_sqlx_template = (
            model_sqlx_header +
            f"-- =============================================================================\n"
            f"-- SURGICAL LLM TRANSFORMATION SCOPE ONLY\n"
            f"-- Rule: Do NOT modify config {{}} above.\n"
            f"-- Rule: References must strictly use ${{ref('schema', 'table')}} with 2 arguments.\n"
            f"-- =============================================================================\n"
            f"SELECT\n"
            f"  *\n"
            f"FROM\n"
            f"  {primary_ref}\n"
        )
        model_p = layer_dir / f"{target_table}.sqlx"
        model_p.write_text(model_sqlx_template, encoding="utf-8")
        scaffolded_files[str(model_p)] = model_sqlx_template

        # -------------------------------------------------------------
        # ARTIFACT 3: definitions/05_assertions/<domain>/assert_<table>.sqlx
        # -------------------------------------------------------------
        key_expr = ", ".join(unique_keys)
        assertion_content = (
            f"config {{\n"
            f'  type: "assertion",\n'
            f"  tags: [\"uniqueness\", \"quality_gate\"],\n"
            f"  dependencies: [\"{target_table}\"]\n"
            f"}}\n\n"
            f"SELECT\n"
            f"  {key_expr},\n"
            f"  COUNT(1) AS duplicate_count\n"
            f"FROM\n"
            f"  ${{ref(\"{target_dataset}\", \"{target_table}\")}}\n"
            f"GROUP BY\n"
            f"  {key_expr}\n"
            f"HAVING\n"
            f"  duplicate_count > 1\n"
        )
        assertion_p = assertions_dir / f"assert_{target_table}.sqlx"
        assertion_p.write_text(assertion_content, encoding="utf-8")
        scaffolded_files[str(assertion_p)] = assertion_content

        # -------------------------------------------------------------
        # ARTIFACT 4: definitions/06_legacy_contracts/<domain>/vw_<table>.sqlx
        # -------------------------------------------------------------
        legacy_view_content = (
            f"config {{\n"
            f'  type: "view",\n'
            f'  schema: "legacy_contracts_{domain}",\n'
            f'  name: "vw_{target_table}",\n'
            f'  description: "Retrocompatibility contract view for legacy downstream consumers"\n'
            f"}}\n\n"
            f"SELECT\n"
            f"  *\n"
            f"FROM\n"
            f"  ${{ref(\"{target_dataset}\", \"{target_table}\")}}\n"
        )
        legacy_p = legacy_contracts_dir / f"vw_{target_table}.sqlx"
        legacy_p.write_text(legacy_view_content, encoding="utf-8")
        scaffolded_files[str(legacy_p)] = legacy_view_content

        # -------------------------------------------------------------
        # ARTIFACT 5: dags/<region>/<dag_id>.yaml (Airflow DAG Factory)
        # -------------------------------------------------------------
        dag_id = f"dag_modernized_{domain}_{target_table}"
        schedule_interval = locked_context.get("schedule", "0 4 * * *")
        
        # Check cross-project variable injection
        vars_dict = {}
        for s in canonical_sources:
            s_proj = s.get("project_id", "")
            if self._is_cross_project(target_project, s_proj):
                vars_dict["project_id"] = s_proj

        vars_block = ""
        if vars_dict:
            vars_json = json.dumps(vars_dict)
            vars_block = f"      vars: {vars_json}\n"

        dag_yaml_content = (
            f"{dag_id}:\n"
            f"  schedule_interval: \"{schedule_interval}\"\n"
            f"  catchup: False\n"
            f"  default_args:\n"
            f"    owner: \"data_platform\"\n"
            f"    retries: 2\n"
            f"    retry_delay_sec: 300\n"
            f"  tags:\n"
            f"    - \"modernization\"\n"
            f"    - \"{domain}\"\n"
            f"    - \"dataform\"\n"
            f"  tasks:\n"
            f"    run_dataform_{target_table}:\n"
            f"      operator: airflow.operators.bash.BashOperator\n"
            f"      bash_command: \"dataform run --tags {target_dataset}\"\n"
            f"{vars_block}"
        )
        dag_p = dags_dir / f"{dag_id}.yaml"
        dag_p.write_text(dag_yaml_content, encoding="utf-8")
        scaffolded_files[str(dag_p)] = dag_yaml_content

        # -------------------------------------------------------------
        # Anti-Placeholder Detection & Lean Surgical Prompt (~5 KB)
        # -------------------------------------------------------------
        needs_surgical_llm = self._check_needs_surgical_llm(model_sqlx_template)
        surgical_prompt = None
        if needs_surgical_llm:
            surgical_prompt = self._generate_surgical_prompt(locked_context, model_sqlx_template)

        # -------------------------------------------------------------
        # Rule T2.9: Auto-Healing of Section 3.2 Required Datasets
        # -------------------------------------------------------------
        auto_healed_spec = None
        if existing_spec_markdown:
            auto_healed_spec = self._auto_heal_spec_section_3_2(existing_spec_markdown, locked_context)

        mermaid_diag = self._generate_mermaid_lineage(locked_context)

        spec_skeleton = {
            "ticket_id": locked_context.get("ticket_id", "DATA-1001"),
            "target": f"{target_project}.{target_dataset}.{target_table}",
            "scaffolded_files": list(scaffolded_files.keys()),
            "needs_surgical_llm": needs_surgical_llm,
            "mermaid_diagram": mermaid_diag,
            "auto_healed_spec_applied": bool(auto_healed_spec),
            "surgical_prompt_for_llm": surgical_prompt
        }

        spec_skeleton_file = out_path / "spec_skeleton.json"
        spec_skeleton_file.write_text(json.dumps(spec_skeleton, indent=2), encoding="utf-8")
        logger.info(f"Scaffolding completed: 5 artifacts generated. Needs Surgical LLM: {needs_surgical_llm}")

        return spec_skeleton

    def _is_cross_project(self, target_proj: str, source_proj: str) -> bool:
        if not source_proj or source_proj == target_proj:
            return False
        t_is_analytic = target_proj.startswith("a-") or "-a-" in target_proj or "dwh" in target_proj
        s_is_ingestion = (
            source_proj.startswith("p-") or source_proj.startswith("i-") or
            "-p-" in source_proj or "-i-" in source_proj or
            "raw" in source_proj or "lake" in source_proj
        )
        return t_is_analytic and s_is_ingestion

    def _get_existing_sources_in_main(self, repo_path: Optional[str]) -> Set[str]:
        existing = set()
        if not repo_path:
            return existing
        p = Path(repo_path)
        if not p.exists():
            return existing
        for f in p.glob("definitions/01_sources/**/*.sqlx"):
            try:
                txt = f.read_text(encoding="utf-8")
                schema_m = re.search(r'schema:\s*["\']([^"\']+)["\']', txt)
                name_m = re.search(r'name:\s*["\']([^"\']+)["\']', txt)
                if schema_m and name_m:
                    existing.add(f"{schema_m.group(1)}.{name_m.group(1)}")
            except Exception:
                pass
        return existing

    def _check_needs_surgical_llm(self, sql_code: str) -> bool:
        body = sql_code
        cfg_m = re.search(r"config\s*\{[^}]+\}", sql_code, re.DOTALL)
        if cfg_m:
            body = sql_code[cfg_m.end():]

        patterns = [
            re.compile(r"SELECT\s+\*\s+FROM", re.IGNORECASE),
            re.compile(r"\b1\s*=\s*1\b"),
            re.compile(r"\bTODO\b", re.IGNORECASE),
            re.compile(r"\bPLACEHOLDER\b", re.IGNORECASE),
        ]
        for p in patterns:
            if p.search(body):
                return True
        return False

    def _generate_surgical_prompt(self, ctx: Dict[str, Any], scaffolded_sqlx: str) -> str:
        sources_list = ", ".join([f"${{ref('{s.get('dataset_id')}', '{s.get('table_id')}')}}" for s in ctx.get("canonical_sources", [])])
        legacy_code = ctx.get("legacy_source_code", "-- No legacy code provided --")
        return (
            f"### SURGICAL LLM TRANSLATION PROMPT (~5 KB)\n"
            f"Target: `{ctx['target_dataset']}.{ctx['target_table']}` (Type: `{ctx.get('table_type', 'incremental')}`)\n"
            f"Available Canonical Sources: {sources_list}\n\n"
            f"#### IMMUTABLE INSTRUCTIONS:\n"
            f"1. You MUST translate ONLY the `SELECT` query body and business transformations.\n"
            f"2. You MUST NOT modify or generate the `config {{}}` block, dataset names, or project IDs.\n"
            f"3. Prohibited: `SELECT *`, placeholders (`1=1`, `TODO`), or hardcoded GCP 3-part paths.\n"
            f"4. All source tables MUST be referenced strictly using `${{ref('schema', 'table')}}` with 2 arguments.\n\n"
            f"#### LEGACY CODE TO TRANSLATE:\n"
            f"```sql\n"
            f"{legacy_code[:4000]}\n"
            f"```\n"
        )

    def _generate_mermaid_lineage(self, ctx: Dict[str, Any]) -> str:
        lines = ["```mermaid", "flowchart LR", '  subgraph Upstream["Upstream Sources"]']
        for idx, s in enumerate(ctx.get("canonical_sources", []), start=1):
            s_name = s.get("table_id", f"src_{idx}")
            s_layer = s.get("dataset_id", "bronze")
            lines.append(f'    S{idx}["{s_layer}.{s_name}"]')
        lines.append("  end")

        lines.append(f'  subgraph Target["Modern Dataform Target"]')
        lines.append(f'    T["{ctx["target_dataset"]}.{ctx["target_table"]} ({ctx.get("table_type", "incremental")})"]')
        lines.append("  end")

        for idx in range(1, len(ctx.get("canonical_sources", [])) + 1):
            lines.append(f"  S{idx} --> T")

        lines.append("```")
        return "\n".join(lines)

    def _auto_heal_spec_section_3_2(self, markdown_text: str, ctx: Dict[str, Any]) -> str:
        required_datasets: Set[str] = {ctx["target_dataset"]}
        for s in ctx.get("canonical_sources", []):
            if s.get("dataset_id"):
                required_datasets.add(s["dataset_id"])

        healed_table = [
            "### 3.2 Required Datasets (Auto-Healed by T2.9)",
            "| Project ID | Dataset ID | Tier / Role | IAM ACL Required |",
            "| :--- | :--- | :--- | :--- |"
        ]
        for ds in sorted(list(required_datasets)):
            proj = ctx["target_project"] if ds == ctx["target_dataset"] else ctx.get("upstream_project", ctx["target_project"])
            tier = "Target" if ds == ctx["target_dataset"] else "Source"
            acl = "roles/bigquery.dataEditor" if tier == "Target" else "roles/bigquery.dataViewer"
            healed_table.append(f"| `{proj}` | `{ds}` | `{tier}` | `{acl}` |")

        healed_section_str = "\n".join(healed_table)

        if re.search(r"###?\s*3\.2\s+Required\s+Datasets", markdown_text, re.IGNORECASE):
            return re.sub(
                r"###?\s*3\.2\s+Required\s+Datasets.*?(?=\n###|\n##|\Z)",
                healed_section_str + "\n",
                markdown_text,
                flags=re.DOTALL | re.IGNORECASE
            )
        return markdown_text + "\n\n" + healed_section_str
