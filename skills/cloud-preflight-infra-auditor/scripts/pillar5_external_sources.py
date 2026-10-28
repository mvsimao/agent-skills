"""
Pillar 5: External Sources (Drive/Sheets), Git Repositories, and DML-Only Target Tables.

Validates:
- Real test read query (SELECT 1 FROM `table` LIMIT 1) on Google Drive/Sheets external tables to capture 403 Drive credentials errors.
- Generates exact Drive sharing instructions for the pipeline Service Account.
- Target Git repository existence and branch accessibility.
- Target tables operated exclusively via DML (MERGE/UPDATE/DELETE) to ensure they are physically pre-created.
"""

from __future__ import annotations

import logging
import subprocess
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pillar1_projects_sa import AuditFinding

logger = logging.getLogger("cloud-preflight.pillar5")


@dataclass
class ExternalTableDefinition:
    project_id: str
    dataset_id: str
    table_id: str
    source_type: str  # GOOGLE_SHEETS, GOOGLE_DRIVE, BIGTABLE, FEDERATED_CSV
    drive_uri: Optional[str] = None
    service_account: Optional[str] = None


@dataclass
class Pillar5Result:
    passed: bool
    findings: List[AuditFinding] = field(default_factory=list)
    sharing_instructions: List[str] = field(default_factory=list)


class ExternalSourcesAndGitAuditor:
    """Audits Drive-backed external tables, git repositories, and DML target tables."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def audit(
        self,
        external_tables: List[Dict[str, Any]],
        git_repo_url: Optional[str] = None,
        dml_target_tables: Optional[List[Dict[str, str]]] = None,
        service_account_email: Optional[str] = None
    ) -> Pillar5Result:
        findings: List[AuditFinding] = []
        instructions: List[str] = []
        overall_pass = True

        sa_email = service_account_email or "sa-pipeline-worker@acme-dwh-prod.iam.gserviceaccount.com"

        # 1. Audit External Tables (Sheets/Drive)
        for ext in external_tables:
            finding, instr = self._audit_external_table(ext, sa_email)
            findings.append(finding)
            if instr:
                instructions.append(instr)
            if finding.severity == "BLOCKER":
                overall_pass = False

        # 2. Audit Git Repository
        if git_repo_url:
            git_finding = self._audit_git_repo(git_repo_url)
            findings.append(git_finding)
            if git_finding.severity == "BLOCKER":
                overall_pass = False

        # 3. Audit DML-Only Target Tables
        if dml_target_tables:
            for tbl in dml_target_tables:
                dml_finding = self._audit_dml_target(tbl)
                findings.append(dml_finding)
                if dml_finding.severity == "BLOCKER":
                    overall_pass = False

        return Pillar5Result(
            passed=overall_pass,
            findings=findings,
            sharing_instructions=instructions
        )

    def _audit_external_table(self, ext_info: Dict[str, Any], sa_email: str) -> tuple[AuditFinding, Optional[str]]:
        project = ext_info.get("project_id", "acme-dwh-prod")
        dataset = ext_info.get("dataset_id", "")
        table = ext_info.get("table_id", "")
        source_type = ext_info.get("source_type", "GOOGLE_SHEETS").upper()
        drive_uri = ext_info.get("drive_uri", "https://docs.google.com/spreadsheets/d/UNKNOWN")
        full_table_id = f"{project}.{dataset}.{table}"

        if self.dry_run:
            return (
                AuditFinding(
                    pillar="Pillar 5 - External Sources & Git",
                    resource_type=f"External Table ({source_type})",
                    resource_id=full_table_id,
                    status="PASS",
                    severity="INFO",
                    details=f"[DRY-RUN] External table '{full_table_id}' read test succeeded."
                ),
                None
            )

        # Execute test query: SELECT 1 FROM `project.dataset.table` LIMIT 1
        query = f"SELECT 1 AS probe FROM `{project}.{dataset}.{table}` LIMIT 1"
        cmd = [
            "bq", "query",
            f"--project_id={project}",
            "--use_legacy_sql=false",
            "--format=json",
            query
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            if res.returncode == 0:
                return (
                    AuditFinding(
                        pillar="Pillar 5 - External Sources & Git",
                        resource_type=f"External Table ({source_type})",
                        resource_id=full_table_id,
                        status="PASS",
                        severity="INFO",
                        details=f"Read test passed for external table '{full_table_id}'. Google Drive OAuth & SA access verified."
                    ),
                    None
                )
            else:
                stderr = res.stderr.lower()
                if "drive" in stderr or "403" in stderr or "denied" in stderr:
                    instr = (
                        f"Action required for external table '{full_table_id}':\n"
                        f"1. Open the underlying Google Sheet: {drive_uri}\n"
                        f"2. Click 'Share' and grant Viewer role to Service Account: '{sa_email}'\n"
                        f"3. Ensure Cloud IAM role 'roles/bigquery.admin' or custom role includes Google Drive OAuth scopes."
                    )
                    return (
                        AuditFinding(
                            pillar="Pillar 5 - External Sources & Git",
                            resource_type=f"External Table ({source_type})",
                            resource_id=full_table_id,
                            status="MISSING_INFRA",
                            severity="BLOCKER",
                            details=f"Failed to read external table '{full_table_id}': 403 Google Drive access denied to service account '{sa_email}'.",
                            remediation_cli=f"# Share Google Sheet at {drive_uri} with {sa_email} as Viewer"
                        ),
                        instr
                    )
                elif "not found" in stderr or "404" in stderr:
                    return (
                        AuditFinding(
                            pillar="Pillar 5 - External Sources & Git",
                            resource_type=f"External Table ({source_type})",
                            resource_id=full_table_id,
                            status="MISSING_INFRA",
                            severity="BLOCKER",
                            details=f"External table definition '{full_table_id}' does not exist (404 Not Found)."
                        ),
                        None
                    )
                return (
                    AuditFinding(
                        pillar="Pillar 5 - External Sources & Git",
                        resource_type=f"External Table ({source_type})",
                        resource_id=full_table_id,
                        status="WARN",
                        severity="WARNING",
                        details=f"External table probe returned: {res.stderr.strip()}"
                    ),
                    None
                )
        except Exception as e:
            return (
                AuditFinding(
                    pillar="Pillar 5 - External Sources & Git",
                    resource_type=f"External Table ({source_type})",
                    resource_id=full_table_id,
                    status="WARN",
                    severity="WARNING",
                    details=f"External table query probe error: {str(e)}"
                ),
                None
            )

    def _audit_git_repo(self, repo_url: str) -> AuditFinding:
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 5 - External Sources & Git",
                resource_type="Git Repository",
                resource_id=repo_url,
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Git repo '{repo_url}' reachable."
            )

        cmd = ["git", "ls-remote", "--heads", repo_url]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if res.returncode == 0:
                return AuditFinding(
                    pillar="Pillar 5 - External Sources & Git",
                    resource_type="Git Repository",
                    resource_id=repo_url,
                    status="PASS",
                    severity="INFO",
                    details=f"Git repository '{repo_url}' is accessible and remote heads were resolved."
                )
            else:
                stderr = res.stderr.lower()
                if "authentication failed" in stderr or "fatal" in stderr or "not found" in stderr:
                    return AuditFinding(
                        pillar="Pillar 5 - External Sources & Git",
                        resource_type="Git Repository",
                        resource_id=repo_url,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Git repository '{repo_url}' unreachable or runner lacks SSH/HTTPS credentials: {res.stderr.strip()}"
                    )
                return AuditFinding(
                    pillar="Pillar 5 - External Sources & Git",
                    resource_type="Git Repository",
                    resource_id=repo_url,
                    status="WARN",
                    severity="WARNING",
                    details=f"Git check warning: {res.stderr.strip()}"
                )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 5 - External Sources & Git",
                resource_type="Git Repository",
                resource_id=repo_url,
                status="WARN",
                severity="WARNING",
                details=f"Git repository check exception: {str(e)}"
            )

    def _audit_dml_target(self, tbl_info: Dict[str, str]) -> AuditFinding:
        project = tbl_info.get("project_id", "acme-dwh-prod")
        dataset = tbl_info.get("dataset_id", "")
        table = tbl_info.get("table_id", "")
        target_id = f"{project}:{dataset}.{table}"

        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 5 - External Sources & Git",
                resource_type="DML-Only Target Table",
                resource_id=target_id,
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Target table '{target_id}' pre-created for DML (MERGE/UPDATE) operations."
            )

        cmd = ["bq", "show", "--format=json", target_id]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if res.returncode == 0:
                return AuditFinding(
                    pillar="Pillar 5 - External Sources & Git",
                    resource_type="DML-Only Target Table",
                    resource_id=target_id,
                    status="PASS",
                    severity="INFO",
                    details=f"DML target table '{target_id}' physically exists. Safe for MERGE/UPDATE operations."
                )
            else:
                stderr = res.stderr.lower()
                if "not found" in stderr or "404" in stderr:
                    return AuditFinding(
                        pillar="Pillar 5 - External Sources & Git",
                        resource_type="DML-Only Target Table",
                        resource_id=target_id,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Target table '{target_id}' operates via DML (MERGE) but does not exist (404 Not Found). Must be pre-created via DDL before pipeline execution.",
                        remediation_iac=f'resource "google_bigquery_table" "{table.replace("-", "_")}" {{\n  project    = "{project}"\n  dataset_id = "{dataset}"\n  table_id   = "{table}"\n  schema     = jsonencode([])\n}}'
                    )
                return AuditFinding(
                    pillar="Pillar 5 - External Sources & Git",
                    resource_type="DML-Only Target Table",
                    resource_id=target_id,
                    status="EXISTS_RESTRICTED_RUNNER",
                    severity="WARNING",
                    details=f"Runner cannot verify target table '{target_id}'. Resource assumed present."
                )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 5 - External Sources & Git",
                resource_type="DML-Only Target Table",
                resource_id=target_id,
                status="WARN",
                severity="WARNING",
                details=f"DML target check error: {str(e)}"
            )
