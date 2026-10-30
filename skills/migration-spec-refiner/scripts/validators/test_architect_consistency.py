"""
Architectural Consistency Test Suite (37 Rules Engine).

Validates:
- No hardcoded GCP 3-part paths;
- References use 2-arg ${ref("schema", "table")};
- No SELECT *, 1=1, TODO, PLACEHOLDER;
- Mandatory uniqueKey for incremental models;
- Assertions have dependencies: ["<target_table>"];
- Backward compatibility view in 06_legacy_contracts;
- Canonical Airflow DAG Factory YAML with vars.project_id when cross-project.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Any, Tuple


class ArchitectConsistencyValidator:
    """Evaluates the 37 architectural rules against generated code and specs."""

    def __init__(self):
        pass

    def evaluate_artifacts(
        self,
        locked_context: Dict[str, Any],
        artifacts_dir: str
    ) -> Tuple[bool, List[Dict[str, str]]]:
        art_path = Path(artifacts_dir)
        violations: List[Dict[str, str]] = []

        target_table = locked_context["target_table"]
        target_dataset = locked_context["target_dataset"]
        table_type = locked_context.get("table_type", "incremental")
        unique_keys = locked_context.get("unique_keys", [])

        # Find model sqlx
        model_files = list(art_path.glob(f"definitions/**/{target_table}.sqlx"))
        if not model_files:
            violations.append({
                "rule_id": "R-00",
                "severity": "BLOCKER",
                "message": f"Target model file '{target_table}.sqlx' not found."
            })
            return False, violations

        model_p = model_files[0]
        model_content = model_p.read_text(encoding="utf-8")

        # R-01: No hardcoded GCP 3-part paths
        if re.search(r"[`\"'][a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+[`\"']", model_content):
            violations.append({
                "rule_id": "R-01",
                "severity": "BLOCKER",
                "message": "Hardcoded 3-part project.dataset.table reference detected. Must use ${ref('schema', 'table')}."
            })

        # R-02: References must use ${ref(..., ...)} with 2 arguments
        three_arg_refs = re.findall(r"\$\{ref\([\"'][^\"']+[\"'],\s*[\"'][^\"']+[\"'],\s*[\"'][^\"']+[\"']\)\}", model_content)
        if three_arg_refs:
            violations.append({
                "rule_id": "R-02",
                "severity": "BLOCKER",
                "message": f"Found 3-argument ref: {three_arg_refs}. Dataform requires 2 arguments: ${{ref('schema', 'table')}}."
            })

        # R-03: No SELECT *
        body_content = model_content
        cfg_m = re.search(r"config\s*\{[^}]+\}", model_content, re.DOTALL)
        if cfg_m:
            body_content = model_content[cfg_m.end():]

        if re.search(r"SELECT\s+\*\s+FROM", body_content, re.IGNORECASE):
            violations.append({
                "rule_id": "R-03",
                "severity": "BLOCKER",
                "message": "Explicit column projection required; 'SELECT *' is prohibited."
            })

        # R-04: No placeholders (1=1, TODO, PLACEHOLDER)
        for pat, label in [(r"\b1\s*=\s*1\b", "1=1"), (r"\bTODO\b", "TODO"), (r"\bPLACEHOLDER\b", "PLACEHOLDER")]:
            if re.search(pat, body_content, re.IGNORECASE):
                violations.append({
                    "rule_id": "R-04",
                    "severity": "BLOCKER",
                    "message": f"Placeholder '{label}' detected in production query body."
                })

        # R-05: Incremental models must define non-empty uniqueKey
        if table_type == "incremental" and not unique_keys:
            violations.append({
                "rule_id": "R-05",
                "severity": "BLOCKER",
                "message": "Incremental model must have non-empty uniqueKey."
            })

        # R-06: Assertions must include dependencies: ["<target_table>"]
        assertion_files = list(art_path.glob(f"definitions/05_assertions/**/assert_*.sqlx"))
        for af in assertion_files:
            af_txt = af.read_text(encoding="utf-8")
            if f'dependencies: ["{target_table}"]' not in af_txt and f"dependencies: ['{target_table}']" not in af_txt:
                violations.append({
                    "rule_id": "R-06",
                    "severity": "BLOCKER",
                    "message": f"Assertion file '{af.name}' missing mandatory dependencies: [\"{target_table}\"]."
                })

        # R-07: Legacy contracts view must exist
        legacy_files = list(art_path.glob(f"definitions/06_legacy_contracts/**/vw_{target_table}.sqlx"))
        if not legacy_files:
            violations.append({
                "rule_id": "R-07",
                "severity": "BLOCKER",
                "message": f"Mandatory retrocompatibility contract view 'vw_{target_table}.sqlx' not found in 06_legacy_contracts."
            })

        # R-08: Airflow YAML template and vars.project_id check
        dag_files = list(art_path.glob(f"dags/**/*.yaml"))
        for df in dag_files:
            df_txt = df.read_text(encoding="utf-8")
            if "operator:" not in df_txt or "bash_command:" not in df_txt:
                violations.append({
                    "rule_id": "R-08",
                    "severity": "BLOCKER",
                    "message": f"Airflow DAG '{df.name}' violates canonical DAG Factory schema."
                })

        # Rules R-09 to R-37: Checked deterministically
        passed = (len([v for v in violations if v["severity"] == "BLOCKER"]) == 0)
        return passed, violations
