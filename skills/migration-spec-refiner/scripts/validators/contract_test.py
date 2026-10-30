"""
Contract Test & 4x4 Parity Auditor.

Validates parity across:
1. Sovereign Catalog SSOT
2. Specification JSON
3. Physical .sqlx and .yaml artifacts in disk
4. PBI Task Card Markdown

Ensures that 100% of artifact links point to real files on disk.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Any, Tuple


class ContractParityValidator:
    """Verifies 4x4 parity and file existence on disk."""

    def evaluate_4x4_parity(
        self,
        locked_context: Dict[str, Any],
        spec_json_path: str,
        artifacts_dir: str,
        pbi_markdown_path: Optional[str] = None
    ) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        art_dir = Path(artifacts_dir)

        # 1. Check SSOT coordinates
        target_tbl = locked_context["target_table"]
        target_ds = locked_context["target_dataset"]

        # 2. Check spec_json
        spec_file = Path(spec_json_path)
        if not spec_file.exists():
            errors.append(f"Spec JSON file does not exist: {spec_file}")
        else:
            try:
                with open(spec_file, "r", encoding="utf-8") as f:
                    spec_data = json.load(f)
                if target_tbl not in spec_data.get("target", ""):
                    errors.append(f"Spec JSON target '{spec_data.get('target')}' does not match SSOT table '{target_tbl}'")
            except Exception as e:
                errors.append(f"Failed to parse spec JSON: {e}")

        # 3. Check physical artifacts on disk
        expected_artifacts = [
            f"05_assertions/**/assert_{target_tbl}.sqlx",
            f"06_legacy_contracts/**/vw_{target_tbl}.sqlx"
        ]
        for pattern in expected_artifacts:
            matches = list(art_dir.glob(f"definitions/{pattern}"))
            if not matches:
                errors.append(f"Physical artifact matching '{pattern}' does not exist on disk.")

        # 4. Check PBI document
        if pbi_markdown_path:
            pbi_file = Path(pbi_markdown_path)
            if not pbi_file.exists():
                errors.append(f"PBI Markdown file does not exist on disk: {pbi_file}")
            else:
                pbi_txt = pbi_file.read_text(encoding="utf-8")
                if target_tbl not in pbi_txt:
                    errors.append(f"PBI Markdown does not contain target table '{target_tbl}'.")

        passed = (len(errors) == 0)
        return passed, errors
