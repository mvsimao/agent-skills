"""
Pillar 1: Cloud Projects, Service Accounts, and Live Smoke Query.

Validates:
- ACTIVE status for GCP Projects (Target, Upstream, Secrets, Staging)
- Existence and status of Service Accounts
- Live Smoke Query (SELECT 1) on BigQuery verifying API, Billing, and Quotas
Differentiates 404 (MISSING_INFRA) vs 403 (EXISTS_RESTRICTED_RUNNER).
"""

from __future__ import annotations

import logging
import subprocess
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

logger = logging.getLogger("cloud-preflight.pillar1")


@dataclass
class AuditFinding:
    pillar: str
    resource_type: str
    resource_id: str
    status: str  # PASS, MISSING_INFRA, EXISTS_RESTRICTED_RUNNER, WARN
    severity: str  # BLOCKER, WARNING, INFO
    details: str
    remediation_iac: Optional[str] = None
    remediation_cli: Optional[str] = None


@dataclass
class Pillar1Result:
    passed: bool
    findings: List[AuditFinding] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ProjectAndServiceAccountAuditor:
    """Audits GCP project readiness, service account existence, and smoke query."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def audit(
        self,
        projects: Dict[str, str],
        service_accounts: List[str],
        execution_project: Optional[str] = None,
        smoke_location: str = "us-central1"
    ) -> Pillar1Result:
        findings: List[AuditFinding] = []
        overall_pass = True

        exec_proj = execution_project or projects.get("target") or "acme-dwh-prod"

        # 1. Audit Projects
        for role, project_id in projects.items():
            res = self._check_project(project_id, role)
            findings.append(res)
            if res.severity == "BLOCKER":
                overall_pass = False

        # 2. Audit Service Accounts
        for sa_email in service_accounts:
            res = self._check_service_account(sa_email)
            findings.append(res)
            if res.severity == "BLOCKER":
                overall_pass = False

        # 3. Smoke Query on BigQuery
        smoke_res = self._execute_smoke_query(exec_proj, location=smoke_location)
        findings.append(smoke_res)
        if smoke_res.severity == "BLOCKER":
            overall_pass = False

        return Pillar1Result(passed=overall_pass, findings=findings)

    def _check_project(self, project_id: str, role: str) -> AuditFinding:
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="GCP Project",
                resource_id=project_id,
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Project '{project_id}' ({role}) verified ACTIVE with billing enabled."
            )

        cmd = ["gcloud", "projects", "describe", project_id, "--format=json"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                state = data.get("lifecycleState", "UNKNOWN")
                if state == "ACTIVE":
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="GCP Project",
                        resource_id=project_id,
                        status="PASS",
                        severity="INFO",
                        details=f"Project '{project_id}' ({role}) is ACTIVE."
                    )
                else:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="GCP Project",
                        resource_id=project_id,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Project '{project_id}' ({role}) is in state '{state}', not ACTIVE.",
                        remediation_cli=f"gcloud projects undelete {project_id}"
                    )
            else:
                stderr = result.stderr.lower()
                if "not found" in stderr or "404" in stderr:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="GCP Project",
                        resource_id=project_id,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Project '{project_id}' does not exist (404 Not Found).",
                        remediation_iac=f'resource "google_project" "{project_id.replace("-", "_")}" {{\n  name       = "{project_id}"\n  project_id = "{project_id}"\n}}',
                        remediation_cli=f"gcloud projects create {project_id} --set-as-default"
                    )
                elif "permission denied" in stderr or "403" in stderr:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="GCP Project",
                        resource_id=project_id,
                        status="EXISTS_RESTRICTED_RUNNER",
                        severity="WARNING",
                        details=f"Runner lacks permissions (403 Forbidden) to describe project '{project_id}'. Resource likely exists in Cloud."
                    )
                else:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="GCP Project",
                        resource_id=project_id,
                        status="WARN",
                        severity="WARNING",
                        details=f"Could not verify project '{project_id}': {result.stderr.strip()}"
                    )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="GCP Project",
                resource_id=project_id,
                status="WARN",
                severity="WARNING",
                details=f"Inspection exception for project '{project_id}': {str(e)}"
            )

    def _check_service_account(self, sa_email: str) -> AuditFinding:
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="Service Account",
                resource_id=sa_email,
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Service Account '{sa_email}' verified."
            )

        cmd = ["gcloud", "iam", "service-accounts", "describe", sa_email, "--format=json"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                disabled = data.get("disabled", False)
                if disabled:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="Service Account",
                        resource_id=sa_email,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Service Account '{sa_email}' is DISABLED.",
                        remediation_cli=f"gcloud iam service-accounts enable {sa_email}"
                    )
                return AuditFinding(
                    pillar="Pillar 1 - Projects & SAs",
                    resource_type="Service Account",
                    resource_id=sa_email,
                    status="PASS",
                    severity="INFO",
                    details=f"Service Account '{sa_email}' is active and valid."
                )
            else:
                stderr = result.stderr.lower()
                if "not found" in stderr or "404" in stderr:
                    sa_name = sa_email.split("@")[0]
                    proj = sa_email.split("@")[1].replace(".iam.gserviceaccount.com", "") if "@" in sa_email else "acme-dwh-prod"
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="Service Account",
                        resource_id=sa_email,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Service Account '{sa_email}' does not exist (404 Not Found).",
                        remediation_iac=f'resource "google_service_account" "{sa_name}" {{\n  account_id   = "{sa_name}"\n  display_name = "Pipeline Orchestration Service Account"\n  project      = "{proj}"\n}}',
                        remediation_cli=f"gcloud iam service-accounts create {sa_name} --project={proj}"
                    )
                elif "permission denied" in stderr or "403" in stderr:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="Service Account",
                        resource_id=sa_email,
                        status="EXISTS_RESTRICTED_RUNNER",
                        severity="WARNING",
                        details=f"Runner lacks permissions (403) to inspect Service Account '{sa_email}'. Existence assumed."
                    )
                return AuditFinding(
                    pillar="Pillar 1 - Projects & SAs",
                    resource_type="Service Account",
                    resource_id=sa_email,
                    status="WARN",
                    severity="WARNING",
                    details=f"SA verification warning: {result.stderr.strip()}"
                )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="Service Account",
                resource_id=sa_email,
                status="WARN",
                severity="WARNING",
                details=f"SA check exception: {str(e)}"
            )

    def _execute_smoke_query(self, project_id: str, location: str = "us-central1") -> AuditFinding:
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="BigQuery Smoke Query",
                resource_id=f"{project_id}:SELECT 1",
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Smoke query executed successfully against project '{project_id}' ({location}). Quota and API OK."
            )

        cmd = [
            "bq", "query",
            f"--project_id={project_id}",
            f"--location={location}",
            "--use_legacy_sql=false",
            "--format=json",
            "SELECT 1 AS readiness_check"
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            if result.returncode == 0:
                return AuditFinding(
                    pillar="Pillar 1 - Projects & SAs",
                    resource_type="BigQuery Smoke Query",
                    resource_id=f"{project_id}:SELECT 1",
                    status="PASS",
                    severity="INFO",
                    details=f"Smoke Query successful on '{project_id}'. BigQuery API, Billing & Job Quotas healthy."
                )
            else:
                stderr = result.stderr.lower()
                if "billingnotenabled" in stderr or "billing" in stderr:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="BigQuery Smoke Query",
                        resource_id=f"{project_id}:SELECT 1",
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"BigQuery execution failed: Billing is not enabled on project '{project_id}'.",
                        remediation_cli=f"gcloud beta billing projects link {project_id} --billing-account=YOUR_ACCOUNT_ID"
                    )
                elif "accessdenied" in stderr or "403" in stderr:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="BigQuery Smoke Query",
                        resource_id=f"{project_id}:SELECT 1",
                        status="EXISTS_RESTRICTED_RUNNER",
                        severity="WARNING",
                        details=f"Runner account lacks 'bigquery.jobs.create' on project '{project_id}'.",
                        remediation_iac=f'resource "google_project_iam_member" "bq_user" {{\n  project = "{project_id}"\n  role    = "roles/bigquery.jobUser"\n  member  = "serviceAccount:YOUR_RUNNER_SA@{project_id}.iam.gserviceaccount.com"\n}}'
                    )
                return AuditFinding(
                    pillar="Pillar 1 - Projects & SAs",
                    resource_type="BigQuery Smoke Query",
                    resource_id=f"{project_id}:SELECT 1",
                    status="MISSING_INFRA",
                    severity="BLOCKER",
                    details=f"Smoke Query failed on project '{project_id}': {result.stderr.strip()}"
                )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="BigQuery Smoke Query",
                resource_id=f"{project_id}:SELECT 1",
                status="WARN",
                severity="WARNING",
                details=f"Smoke query exception: {str(e)}"
            )
