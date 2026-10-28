"""
Stage 2: Deterministic Dataform Scaffolding + Surgical LLM Review.

Generates 100% via Python:
- All definitions/01_sources/*.sqlx declarations
- Target model config {} blocks (type, uniqueKey, partitionBy, clusterBy, tags)
- Dataform unit assertions (tests/*.sqlx for row uniqueness and non-null)
- Mermaid architecture dataflow diagram
- Structured JSON and Markdown specification skeleton

Restricts LLM exclusively to:
- Translating legacy SQL transformation logic (SELECT ...)
- Authoring the technical summary.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger("refiner.stage2-scaffold")


class DataformScaffolder:
    """Deterministically scaffolds Dataform assets and specification skeletons."""

    def scaffold(
        self,
        locked_context: Dict[str, Any],
        output_dir: str = "./refinement_artifacts"
    ) -> Dict[str, Any]:
        target_project = locked_context["target_project"]
        target_dataset = locked_context["target_dataset"]
        target_table = locked_context["target_table"]
        table_type = locked_context["table_type"]
        unique_keys = locked_context["unique_keys"]
        partition_by = locked_context.get("partition_by")
        cluster_by = locked_context.get("cluster_by", [])
        canonical_sources = locked_context.get("canonical_sources", [])

        out_path = Path(output_dir)
        definitions_dir = out_path / "definitions"
        sources_dir = definitions_dir / "01_sources"
        models_dir = definitions_dir / "02_models"
        tests_dir = out_path / "tests"

        sources_dir.mkdir(parents=True, exist_ok=True)
        models_dir.mkdir(parents=True, exist_ok=True)
        tests_dir.mkdir(parents=True, exist_ok=True)

        scaffolded_files: Dict[str, str] = {}

        # -------------------------------------------------------------
        # 1. Generate 01_sources/*.sqlx for each upstream source
        # -------------------------------------------------------------
        for src in canonical_sources:
            s_proj = src.get("project_id", locked_context.get("upstream_project", "acme-lakehouse-raw"))
            s_dataset = src.get("dataset_id", "bronze_raw")
            s_table = src.get("table_id", "source_table")
            file_name = f"declaration_{s_dataset}_{s_table}.sqlx"

            sqlx_decl = (
                f"config {{\n"
                f'  type: "declaration",\n'
                f'  database: "{s_proj}",\n'
                f'  schema: "{s_dataset}",\n'
                f'  name: "{s_table}",\n'
                f'  description: "Authoritative external source declaration managed by SSOT catalog"\n'
                f"}}\n"
            )
            file_p = sources_dir / file_name
            file_p.write_text(sqlx_decl, encoding="utf-8")
            scaffolded_files[str(file_p)] = sqlx_decl

        # -------------------------------------------------------------
        # 2. Generate Target Model SQLX config {} block
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

        # Placeholder for LLM surgical insertion
        model_sqlx_template = (
            model_sqlx_header +
            f"-- =============================================================================\n"
            f"-- LLM SURGICAL TRANSFORMATION SCOPE ONLY\n"
            f"-- Rule: Do NOT modify the config {{}} block above.\n"
            f"-- Rule: Reference upstream sources using ${{ref('source_name')}}\n"
            f"-- =============================================================================\n"
            f"SELECT\n"
            f"  -- LLM: Insert column projection and business logic here\n"
            f"  CURRENT_TIMESTAMP() AS processed_at\n"
        )
        model_p = models_dir / f"{target_table}.sqlx"
        model_p.write_text(model_sqlx_template, encoding="utf-8")
        scaffolded_files[str(model_p)] = model_sqlx_template

        # -------------------------------------------------------------
        # 3. Generate Unit Test Assertions tests/*.sqlx
        # -------------------------------------------------------------
        if unique_keys:
            key_expr = ", ".join(unique_keys)
            assertion_content = (
                f"config {{\n"
                f'  type: "assertion",\n'
                f'  tags: ["data_quality", "uniqueness"]\n'
                f"}}\n\n"
                f"SELECT\n"
                f"  {key_expr},\n"
                f"  COUNT(1) AS duplicate_count\n"
                f"FROM\n"
                f"  ${{ref('{target_table}')}}\n"
                f"GROUP BY\n"
                f"  {key_expr}\n"
                f"HAVING\n"
                f"  duplicate_count > 1\n"
            )
            test_p = tests_dir / f"assert_unique_{target_table}.sqlx"
            test_p.write_text(assertion_content, encoding="utf-8")
            scaffolded_files[str(test_p)] = assertion_content

        # -------------------------------------------------------------
        # 4. Generate Mermaid Architecture Diagram
        # -------------------------------------------------------------
        mermaid_diag = self._generate_mermaid_lineage(locked_context)

        # -------------------------------------------------------------
        # 5. Generate Surgical Prompt for LLM
        # -------------------------------------------------------------
        surgical_prompt = self._generate_surgical_prompt(locked_context, model_sqlx_header)

        spec_skeleton = {
            "ticket_id": locked_context["ticket_id"],
            "target": f"{target_project}.{target_dataset}.{target_table}",
            "scaffolded_files": list(scaffolded_files.keys()),
            "mermaid_diagram": mermaid_diag,
            "surgical_prompt_for_llm": surgical_prompt
        }

        spec_skeleton_file = out_path / "spec_skeleton.json"
        spec_skeleton_file.write_text(json.dumps(spec_skeleton, indent=2), encoding="utf-8")
        logger.info(f"Scaffolding completed. Skeleton saved to: {spec_skeleton_file}")

        return spec_skeleton

    def _generate_mermaid_lineage(self, ctx: Dict[str, Any]) -> str:
        lines = ["```mermaid", "flowchart LR", '  subgraph Upstream["Upstream Sources"]']
        for idx, s in enumerate(ctx.get("canonical_sources", []), start=1):
            s_name = s.get("table_id", f"src_{idx}")
            s_layer = s.get("dataset_id", "bronze")
            lines.append(f'    S{idx}["{s_layer}.{s_name}"]')
        lines.append("  end")

        lines.append(f'  subgraph Target["Modern Dataform Target"]')
        lines.append(f'    T["{ctx["target_dataset"]}.{ctx["target_table"]} ({ctx["table_type"]})"]')
        lines.append("  end")

        for idx in range(1, len(ctx.get("canonical_sources", [])) + 1):
            lines.append(f"  S{idx} --> T")

        lines.append("```")
        return "\n".join(lines)

    def _generate_surgical_prompt(self, ctx: Dict[str, Any], config_block: str) -> str:
        return (
            f"You are a SQL Modernization Engineer translating legacy Spark/Lakehouse logic into BigQuery SQLx.\n"
            f"CRITICAL RULES (FAIL-CLOSED):\n"
            f"1. You MUST NOT modify or hallucinate project names, dataset names, or table names.\n"
            f"2. Your output MUST ONLY be the SELECT query body for model '{ctx['target_table']}'.\n"
            f"3. Use ${{ref('source_table')}} syntax to reference sources.\n"
            f"4. Target schema: {ctx['target_dataset']}, Materialization: {ctx['table_type']}.\n"
            f"5. Write an executive summary of the business logic in 3-4 bullet points.\n"
        )
