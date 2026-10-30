"""
Stage 3: Fail-Closed Gate of 37 Architectural Rules, Local Suites S1–S6 & 4x4 Parity Validator.

Responsibilities:
- Auto-restores any coordinate or config the LLM attempted to alter against context_locked.json.
- Runs local validators in scripts/validators/:
    * test_architect_consistency.py (37 rules engine, physical disk inspection)
    * test_pbi_consistency.py (Suites S1–S6)
    * contract_test.py (4x4 Parity across SSOT, Spec JSON, Physical Artifacts, and PBI)
- Completely self-contained without external repository dependencies.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Any, Set, Tuple, Optional

# Ensure scripts and scripts/validators are in import path
curr_dir = Path(__file__).resolve().parent
validators_dir = curr_dir / "validators"
if str(curr_dir) not in sys.path:
    sys.path.insert(0, str(curr_dir))
if str(validators_dir) not in sys.path:
    sys.path.insert(0, str(validators_dir))

from test_architect_consistency import ArchitectConsistencyValidator
from test_pbi_consistency import PBIConsistencyValidator
from contract_test import ContractParityValidator

logger = logging.getLogger("refiner.stage3-gate")


@dataclass
class RuleViolation:
    rule_id: str
    rule_name: str
    severity: str  # BLOCKER, WARNING
    details: str


@dataclass
class GateAuditResult:
    passed: bool
    total_rules_evaluated: int = 37
    violations: List[RuleViolation] = field(default_factory=list)
    restored_coordinates: List[str] = field(default_factory=list)
    compiled_graph: Dict[str, Any] = field(default_factory=dict)
    parity_4x4_verified: bool = False
    suite_results: Dict[str, bool] = field(default_factory=dict)


class RefinementGateValidator:
    """Enforces 37 deterministic architectural rules and audits 4x4 parity."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.architect_validator = ArchitectConsistencyValidator()
        self.pbi_validator = PBIConsistencyValidator()
        self.contract_validator = ContractParityValidator()

    def validate(
        self,
        locked_context_path: str,
        artifacts_dir: str,
        sqlx_model_path: Optional[str] = None,
        spec_json_path: Optional[str] = None,
        pbi_doc_path: Optional[str] = None
    ) -> GateAuditResult:
        with open(locked_context_path, "r", encoding="utf-8") as f:
            locked = json.load(f)

        target_table = locked["target_table"]
        target_dataset = locked["target_dataset"]
        target_project = locked["target_project"]
        table_type = locked.get("table_type", "incremental")
        unique_keys = locked.get("unique_keys", ["id"])

        art_dir = Path(artifacts_dir)
        violations: List[RuleViolation] = []
        restored = []

        # Find or derive sqlx_model_path
        if not sqlx_model_path:
            candidates = list(art_dir.glob(f"definitions/**/{target_table}.sqlx"))
            if candidates:
                sqlx_model_path = str(candidates[0])
            else:
                sqlx_model_path = str(art_dir / "definitions" / "03_silver" / target_table / f"{target_table}.sqlx")

        sqlx_file = Path(sqlx_model_path)
        if not sqlx_file.exists():
            return GateAuditResult(
                passed=False,
                violations=[RuleViolation("R-00", "File Existence", "BLOCKER", f"Model SQLx file '{sqlx_file}' not found.")]
            )

        sqlx_content = sqlx_file.read_text(encoding="utf-8")

        # -------------------------------------------------------------
        # 1. Coordinate Auto-Restoration
        # -------------------------------------------------------------
        clean_config = (
            f"config {{\n"
            f'  type: "{table_type}",\n'
            f'  schema: "{target_dataset}",\n'
            f'  name: "{target_table}",\n'
            f'  description: "Target modernized BigQuery model for {target_table}",\n'
            f"  tags: [\"modernization\", \"{target_dataset}\"],\n"
            f"  uniqueKey: {json.dumps(unique_keys)}\n"
            f"}}"
        )
        config_match = re.search(r"config\s*\{[^}]+\}", sqlx_content, re.DOTALL)
        if config_match:
            cfg_body = config_match.group(0)
            if f'schema: "{target_dataset}"' not in cfg_body or f'name: "{target_table}"' not in cfg_body:
                logger.warning("Coordinates altered by LLM! Auto-restoring from SSOT...")
                sqlx_content = sqlx_content.replace(cfg_body, clean_config)
                sqlx_file.write_text(sqlx_content, encoding="utf-8")
                restored.append(f"Auto-restored SQLx config for `{target_table}` to enforce `{target_dataset}.{target_table}`.")

        # -------------------------------------------------------------
        # 2. Evaluate 37 Architectural Rules via Validator
        # -------------------------------------------------------------
        arch_ok, arch_violations = self.architect_validator.evaluate_artifacts(locked, artifacts_dir)
        for av in arch_violations:
            violations.append(RuleViolation(
                rule_id=av["rule_id"],
                rule_name="Architectural Rule Violation",
                severity=av["severity"],
                details=av["message"]
            ))

        # -------------------------------------------------------------
        # 3. Evaluate Suites S1–S6 on PBI Document if available
        # -------------------------------------------------------------
        suite_res = {}
        if pbi_doc_path and Path(pbi_doc_path).exists():
            pbi_txt = Path(pbi_doc_path).read_text(encoding="utf-8")
            s_ok, s_map, s_notes = self.pbi_validator.evaluate_pbi_markdown(pbi_txt, locked)
            suite_res = s_map
            for sn in s_notes:
                violations.append(RuleViolation("SUITE-PBI", "PBI Consistency", "BLOCKER", sn))

        # -------------------------------------------------------------
        # 4. Evaluate 4x4 Parity
        # -------------------------------------------------------------
        actual_spec_json = spec_json_path or str(art_dir / "spec_skeleton.json")
        parity_ok, parity_errors = self.contract_validator.evaluate_4x4_parity(
            locked_context=locked,
            spec_json_path=actual_spec_json,
            artifacts_dir=artifacts_dir,
            pbi_markdown_path=pbi_doc_path
        )
        for pe in parity_errors:
            violations.append(RuleViolation("PARITY-4X4", "Contract 4x4 Mismatch", "BLOCKER", pe))

        overall_passed = (len([v for v in violations if v.severity == "BLOCKER"]) == 0)

        return GateAuditResult(
            passed=overall_passed,
            total_rules_evaluated=37,
            violations=violations,
            restored_coordinates=restored,
            parity_4x4_verified=parity_ok,
            suite_results=suite_res
        )
