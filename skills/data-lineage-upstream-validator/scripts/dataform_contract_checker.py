"""
Dataform Contract Checker and Wave Fallback Guardrail.

Layers Evaluated:
- Layer 1: Live BigQuery Physical Verification (num_rows, last_modified) & Spec Alignment
- Layer 2: Dataform Repository Contract Fallback with Draft Guardrail
    * If table is not yet in prod DW but the .sqlx contract is merged in main branch: DATAFORM_VALIDATED (Pass for wave migration)
    * If contract exists only in an unmerged draft workspace: UNSTARTED_PREDECESSOR (Fail/Block)
"""

from __future__ import annotations

import logging
import re
import subprocess
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger("lineage-validator.dataform")


@dataclass
class UpstreamValidationStatus:
    project_id: str
    dataset_id: str
    table_id: str
    status: str  # LIVE_VERIFIED, DATAFORM_VALIDATED, UNSTARTED_PREDECESSOR, MISSING_UNRESOLVED
    layer_found: str  # DWH_LIVE, DATAFORM_REPO, NONE
    num_rows: Optional[int] = None
    last_modified: Optional[str] = None
    contract_file: Optional[str] = None
    details: str = ""


class DataformContractChecker:
    """Checks live BigQuery status and Dataform contracts with draft guardrails."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def check_upstream_table(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        dataform_repo_path: Optional[str] = None,
        is_draft_workspace: bool = False
    ) -> UpstreamValidationStatus:
        full_id = f"{project_id}.{dataset_id}.{table_id}"

        # -------------------------------------------------------------
        # CAMADA 1: Live BigQuery Table Check
        # -------------------------------------------------------------
        live_res = self._check_live_dw(project_id, dataset_id, table_id)
        if live_res is not None:
            return live_res

        # -------------------------------------------------------------
        # CAMADA 2: Dataform Contract Fallback (Wave-Based Migrations)
        # -------------------------------------------------------------
        if dataform_repo_path:
            contract_res = self._check_dataform_sources(
                dataform_repo_path=dataform_repo_path,
                dataset_id=dataset_id,
                table_id=table_id,
                is_draft_workspace=is_draft_workspace
            )
            if contract_res is not None:
                return contract_res

        return UpstreamValidationStatus(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            status="MISSING_UNRESOLVED",
            layer_found="NONE",
            details=f"Upstream table `{full_id}` does not exist in DW and no merged Dataform contract was found."
        )

    def _check_live_dw(self, project: str, dataset: str, table: str) -> Optional[UpstreamValidationStatus]:
        full_table = f"{project}:{dataset}.{table}"
        if self.dry_run:
            # Deterministic simulation for demo
            if "erp_customers" in table or "orders_cleansed" in table:
                return UpstreamValidationStatus(
                    project_id=project,
                    dataset_id=dataset,
                    table_id=table,
                    status="LIVE_VERIFIED",
                    layer_found="DWH_LIVE",
                    num_rows=1450200,
                    last_modified="2026-10-27T23:15:00Z",
                    details=f"Physical table `{full_table}` verified live with 1,450,200 rows."
                )
            return None

        cmd = ["bq", "show", "--format=json", full_table]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if res.returncode == 0:
                data = json.loads(res.stdout)
                num_rows = int(data.get("numRows", 0))
                last_mod = data.get("lastModifiedTime")
                return UpstreamValidationStatus(
                    project_id=project,
                    dataset_id=dataset,
                    table_id=table,
                    status="LIVE_VERIFIED",
                    layer_found="DWH_LIVE",
                    num_rows=num_rows,
                    last_modified=last_mod,
                    details=f"Table `{full_table}` is live with {num_rows:,} rows."
                )
        except Exception as e:
            logger.debug(f"Live DW check error for {full_table}: {e}")

        return None

    def _check_dataform_sources(
        self,
        dataform_repo_path: str,
        dataset_id: str,
        table_id: str,
        is_draft_workspace: bool
    ) -> Optional[UpstreamValidationStatus]:
        repo_dir = Path(dataform_repo_path)
        if not repo_dir.exists():
            return None

        # Search in definitions/01_sources/ and definitions/
        source_files = list(repo_dir.glob("definitions/**/*.sqlx"))
        source_decl_regex = re.compile(
            r'config\s*\{[^}]*type:\s*["\']declaration["\'][^}]*name:\s*["\']' + re.escape(table_id) + r'["\']',
            re.DOTALL | re.IGNORECASE
        )
        table_decl_regex = re.compile(
            r'config\s*\{[^}]*type:\s*["\'](?:table|incremental|view)["\'][^}]*name:\s*["\']' + re.escape(table_id) + r'["\']',
            re.DOTALL | re.IGNORECASE
        )

        for fpath in source_files:
            try:
                content = fpath.read_text(encoding="utf-8")
                if source_decl_regex.search(content) or table_decl_regex.search(content):
                    # Found contract
                    rel_path = str(fpath.relative_to(repo_dir))
                    if is_draft_workspace:
                        return UpstreamValidationStatus(
                            project_id="dataform-repo",
                            dataset_id=dataset_id,
                            table_id=table_id,
                            status="UNSTARTED_PREDECESSOR",
                            layer_found="DATAFORM_REPO",
                            contract_file=rel_path,
                            details=f"Contract found in `{rel_path}`, but workspace is marked as DRAFT. Predecessor task is still unmerged."
                        )
                    else:
                        return UpstreamValidationStatus(
                            project_id="dataform-repo",
                            dataset_id=dataset_id,
                            table_id=table_id,
                            status="DATAFORM_VALIDATED",
                            layer_found="DATAFORM_REPO",
                            contract_file=rel_path,
                            details=f"Contract `{table_id}` merged into Dataform repository (`{rel_path}`). Approved for wave migration."
                        )
            except Exception as e:
                logger.debug(f"Error reading {fpath}: {e}")

        return None
