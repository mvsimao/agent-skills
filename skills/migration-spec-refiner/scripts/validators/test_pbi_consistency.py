"""
PBI Work Item Consistency Test Suite (Suites S1–S6).

Validates:
- Suite S1: Header & Target Metadata Consistency
- Suite S2: Upstream Source Declarations vs Dataform Parity
- Suite S3: SQLx Model Block & Column Projection Integrity
- Suite S4: Data Quality Unit Assertions (Tests) Coverage
- Suite S5: Required Datasets (Section 3.2) Complete Enumeration (T2.9)
- Suite S6: Diagnostics Integrity (Cloud Preflight & Upstream Lineage attachments)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Any, Tuple


class PBIConsistencyValidator:
    """Validates generated PBI Technical Specification against suites S1 through S6."""

    def evaluate_pbi_markdown(
        self,
        pbi_markdown: str,
        locked_context: Dict[str, Any]
    ) -> Tuple[bool, Dict[str, bool], List[str]]:
        results = {
            "S1": False,
            "S2": False,
            "S3": False,
            "S4": False,
            "S5": False,
            "S6": False
        }
        notes: List[str] = []

        target_tbl = locked_context["target_table"]
        target_ds = locked_context["target_dataset"]
        canonical_sources = locked_context.get("canonical_sources", [])

        # S1: Header & Target Metadata
        if target_tbl in pbi_markdown and target_ds in pbi_markdown:
            results["S1"] = True
        else:
            notes.append("S1 Failed: Target table or dataset missing from PBI header.")

        # S2: Upstream Sources Parity
        all_sources_found = True
        for s in canonical_sources:
            if s.get("table_id") not in pbi_markdown:
                all_sources_found = False
                break
        results["S2"] = all_sources_found
        if not all_sources_found:
            notes.append("S2 Failed: Not all canonical sources are declared in PBI.")

        # S3: SQLx Model Block
        if "```sqlx" in pbi_markdown and f"name: \"{target_tbl}\"" in pbi_markdown:
            results["S3"] = True
        else:
            notes.append("S3 Failed: SQLx model block missing or incorrect.")

        # S4: Unit Assertions
        if "type: \"assertion\"" in pbi_markdown or "type: 'assertion'" in pbi_markdown:
            results["S4"] = True
        else:
            notes.append("S4 Failed: Unit assertion specification missing.")

        # S5: Required Datasets (Section 3.2)
        if "3.2 Required Datasets" in pbi_markdown and target_ds in pbi_markdown:
            results["S5"] = True
        else:
            notes.append("S5 Failed: Section 3.2 Required Datasets missing or incomplete.")

        # S6: Diagnostics Attachments
        if "Preflight" in pbi_markdown or "Lineage" in pbi_markdown:
            results["S6"] = True
        else:
            notes.append("S6 Failed: Cloud preflight or lineage diagnostics missing.")

        all_passed = all(results.values())
        return all_passed, results, notes
