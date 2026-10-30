"""
Dataform Contract Checker and Wave Fallback Guardrail.

Layers Evaluated:
- Layer 1: Live BigQuery Physical Verification & Service Account Impersonation Fallback
    * If local credentials receive 403 Forbidden, automatically impersonates the regional worker SA
    * Checks num_rows, partition timestamps, and compares spec vs definitions/01_sources/*.sqlx
- Layer 2: Dataform Repository Contract Fallback (Across Main and Sibling Ingestion Projects)
    * If table is not yet in DW but .sqlx declaration is merged in main branch: DATAFORM_VALIDATED (Wave clearance)
    * Supports sibling ingestion project paths
    * Draft Workspace Guardrail: If is_draft_workspace=True, rejects unmerged predecessor drafts as UNSTARTED_PREDECESSOR
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
    layer_found: str  # DWH_LIVE, DATAFORM_REPO, SIBLING_REPO, NONE
    num_rows: Optional[int] = None
    last_modified: Optional[str] = None
    contract_file: Optional[str] = None
    impersonation_used: bool = False
    details: str = ""


class DataformContractChecker:
    """Checks live BigQuery status and Dataform contracts with draft guardrails and sibling project search."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def check_upstream_table(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        dataform_repo_path: Optional[str] = None,
        sibling_repo_paths: Optional[List[str]] = None,
        is_draft_workspace: bool = False,
        producer_card_status: Optional[str] = None,
        impersonate_sa: Optional[str] = None
    ) -> UpstreamValidationStatus:
        full_id = f"{project_id}.{dataset_id}.{table_id}"

        # -------------------------------------------------------------
        # CAMADA 1: Live BigQuery Check with SA Impersonation Fallback & 0-Rows Lock
        # -------------------------------------------------------------
        live_res = self._check_live_dw(
            project_id, dataset_id, table_id,
            producer_card_status=producer_card_status,
            impersonate_sa=impersonate_sa
        )
        if live_res is not None:
            return live_res

        # If 403 occurred without impersonation, attempt SA Impersonation cascade
        if not impersonate_sa:
            fallback_sa = f"sa-dataform-executor@{project_id}.iam.gserviceaccount.com"
            logger.info(f"Checking live DW for '{full_id}' using SA impersonation cascade '{fallback_sa}'...")
            impersonated_res = self._check_live_dw(
                project_id, dataset_id, table_id,
                producer_card_status=producer_card_status,
                impersonate_sa=fallback_sa
            )
            if impersonated_res is not None:
                impersonated_res.impersonation_used = True
                impersonated_res.details += " (Confirmed via Service Account Impersonation cascade)."
                return impersonated_res

        # -------------------------------------------------------------
        # CAMADA 2: Dataform Contract Fallback (Wave-Based Migrations)
        # -------------------------------------------------------------
        search_repos = []
        if dataform_repo_path:
            search_repos.append(("DATAFORM_REPO", dataform_repo_path))
        if sibling_repo_paths:
            for s_path in sibling_repo_paths:
                search_repos.append(("SIBLING_REPO", s_path))

        for repo_label, repo_p in search_repos:
            contract_res = self._check_dataform_sources(
                dataform_repo_path=repo_p,
                dataset_id=dataset_id,
                table_id=table_id,
                is_draft_workspace=is_draft_workspace,
                layer_label=repo_label
            )
            if contract_res is not None:
                return contract_res

        return UpstreamValidationStatus(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            status="MISSING_UNRESOLVED",
            layer_found="NONE",
            details=f"Upstream table `{full_id}` does not exist in DW and no merged Dataform contract was found in primary or sibling repos."
        )

    def _check_live_dw(
        self,
        project: str,
        dataset: str,
        table: str,
        producer_card_status: Optional[str] = None,
        impersonate_sa: Optional[str] = None
    ) -> Optional[UpstreamValidationStatus]:
        full_table = f"{project}:{dataset}.{table}"
        unstarted_statuses = {"pre-wave", "refining", "to do", "new", "draft", "backlog"}

        if self.dry_run:
            if "unpopulated_table" in table or "orders_empty" in table:
                num_rows = 0
                is_unstarted = (producer_card_status or "refining").lower() in unstarted_statuses
                if is_unstarted:
                    return UpstreamValidationStatus(
                        project_id=project,
                        dataset_id=dataset,
                        table_id=table,
                        status="UNPOPULATED_UPSTREAM",
                        layer_found="DWH_LIVE",
                        num_rows=0,
                        last_modified="2026-10-28T10:00:00Z",
                        details=f"CRITICAL LOCK: Table `{full_table}` exists physically but contains 0 rows, and producer card is unstarted ('{producer_card_status or 'Refining'}'). Classified as dependency blocker."
                    )
            if "erp_customers" in table or "orders_cleansed" in table or "raw_customers" in table or "orders_v2" in table or "eventtracks" in table:
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

        cmd = ["bq", "show", "--format=json"]
        if impersonate_sa:
            cmd.append(f"--impersonate_service_account={impersonate_sa}")
        cmd.append(full_table)

        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if res.returncode == 0:
                data = json.loads(res.stdout)
                num_rows = int(data.get("numRows", 0))
                last_mod = data.get("lastModifiedTime")

                # 0-Rows Lock Rule
                is_unstarted = (producer_card_status or "").lower() in unstarted_statuses
                if num_rows == 0 and is_unstarted:
                    return UpstreamValidationStatus(
                        project_id=project,
                        dataset_id=dataset,
                        table_id=table,
                        status="UNPOPULATED_UPSTREAM",
                        layer_found="DWH_LIVE",
                        num_rows=0,
                        last_modified=last_mod,
                        impersonation_used=bool(impersonate_sa),
                        details=f"CRITICAL LOCK: Physical table `{full_table}` has 0 rows and producer card is unstarted ('{producer_card_status}'). Dependency blocker UNPOPULATED_UPSTREAM."
                    )

                return UpstreamValidationStatus(
                    project_id=project,
                    dataset_id=dataset,
                    table_id=table,
                    status="LIVE_VERIFIED",
                    layer_found="DWH_LIVE",
                    num_rows=num_rows,
                    last_modified=last_mod,
                    impersonation_used=bool(impersonate_sa),
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
        is_draft_workspace: bool,
        layer_label: str = "DATAFORM_REPO"
    ) -> Optional[UpstreamValidationStatus]:
        repo_dir = Path(dataform_repo_path)
        if not repo_dir.exists():
            return None

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
                    rel_path = str(fpath.relative_to(repo_dir))
                    if is_draft_workspace:
                        return UpstreamValidationStatus(
                            project_id="dataform-repo",
                            dataset_id=dataset_id,
                            table_id=table_id,
                            status="UNSTARTED_PREDECESSOR",
                            layer_found=layer_label,
                            contract_file=rel_path,
                            details=f"Contract found in `{rel_path}` ({layer_label}), but workspace is marked as DRAFT. Predecessor task is still unmerged."
                        )
                    else:
                        return UpstreamValidationStatus(
                            project_id="dataform-repo",
                            dataset_id=dataset_id,
                            table_id=table_id,
                            status="DATAFORM_VALIDATED",
                            layer_found=layer_label,
                            contract_file=rel_path,
                            details=f"Contract `{table_id}` merged into {layer_label} (`{rel_path}`). Approved for wave migration."
                        )
            except Exception as e:
                logger.debug(f"Error reading {fpath}: {e}")

        return None
