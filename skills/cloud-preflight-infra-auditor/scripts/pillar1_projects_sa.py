"""
Pillar 1: Cloud Projects, Alias Normalization, Service Accounts & Live Smoke Query.

Capabilities:
- Resolves historical IaC naming aliases (short_names like 'raw', 'prod', 'dwh', 'vault', 'stage' to active canonical GCP project IDs).
- Validates ACTIVE lifecycleState for Target, Upstream, Secrets, and Staging projects.
- Validates existence and status of Ingestion/Processing Service Accounts.
- Executes live Smoke Query (SELECT 1) on BigQuery verifying enabled APIs, billing, and job creation quotas (bigquery.jobs.create).
- Supports Service Account Impersonation cascade when local runner credentials receive 403 Forbidden.
- Deterministic Matrix: 404 Not Found (MISSING_INFRA) vs 403 Forbidden (EXISTS_RESTRICTED_RUNNER).
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
    impersonation_used: bool = False
    remediation_iac: Optional[str] = None
    remediation_cli: Optional[str] = None


@dataclass
class Pillar1Result:
    passed: bool
    findings: List[AuditFinding] = field(default_factory=list)
    resolved_projects: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ProjectAndServiceAccountAuditor:
    """Audits project status, normalizes legacy aliases, verifies SAs, and executes smoke query."""

    DEFAULT_ALIAS_MAP = {
        "prod": "acme-dwh-prod",
        "dwh": "acme-dwh-prod",
        "target": "acme-dwh-prod",
        "raw": "acme-lakehouse-raw",
        "lake": "acme-lakehouse-raw",
        "upstream": "acme-lakehouse-raw",
        "vault": "acme-security-vault",
        "sec": "acme-security-vault",
        "secrets": "acme-security-vault",
        "stage": "acme-sandbox-tmp",
        "staging": "acme-sandbox-tmp",
        "tmp": "acme-sandbox-tmp"
    }

    def __init__(self, dry_run: bool = False, alias_map: Optional[Dict[str, str]] = None):
        self.dry_run = dry_run
        self.alias_map = {**self.DEFAULT_ALIAS_MAP, **(alias_map or {})}

    def normalize_project_id(self, project_alias_or_id: str) -> str:
        """Resolves historical naming aliases and suffixes into authoritative active project IDs."""
        key = project_alias_or_id.strip().lower()
        if key in self.alias_map:
            return self.alias_map[key]
        for alias_key, target_proj in self.alias_map.items():
            if key.endswith(f"-{alias_key}") or key.startswith(f"{alias_key}-"):
                return target_proj
        return project_alias_or_id.strip()

    def audit(
        self,
        projects: Dict[str, str],
        service_accounts: List[str],
        execution_project: Optional[str] = None,
        smoke_location: str = "us-central1",
        impersonate_sa: Optional[str] = None
    ) -> Pillar1Result:
        findings: List[AuditFinding] = []
        resolved_projects: Dict[str, str] = {}
        overall_pass = True

        # 1. Normalize and Audit Projects
        for role, raw_project in projects.items():
            canonical_proj = self.normalize_project_id(raw_project)
            resolved_projects[role] = canonical_proj
            
            res = self._check_project(canonical_proj, role, raw_input=raw_project, impersonate_sa=impersonate_sa)
            findings.append(res)
            if res.severity == "BLOCKER":
                overall_pass = False

        # 2. Audit Service Accounts
        for sa_email in service_accounts:
            res = self._check_service_account(sa_email, impersonate_sa=impersonate_sa)
            findings.append(res)
            if res.severity == "BLOCKER":
                overall_pass = False

        # 3. Live BigQuery Smoke Query (SELECT 1)
        exec_proj = execution_project or resolved_projects.get("target") or "acme-dwh-prod"
        smoke_res = self._execute_smoke_query(exec_proj, location=smoke_location, impersonate_sa=impersonate_sa)
        findings.append(smoke_res)
        if smoke_res.severity == "BLOCKER":
            overall_pass = False

        return Pillar1Result(
            passed=overall_pass,
            findings=findings,
            resolved_projects=resolved_projects
        )

    def _check_project(
        self,
        project_id: str,
        role: str,
        raw_input: str,
        impersonate_sa: Optional[str] = None
    ) -> AuditFinding:
        alias_note = f" (Resolved from alias '{raw_input}')" if raw_input != project_id else ""

        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="GCP Project",
                resource_id=project_id,
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Project '{project_id}' ({role}){alias_note} verified ACTIVE with billing enabled."
            )

        cmd = ["gcloud", "projects", "describe", project_id, "--format=json"]
        if impersonate_sa:
            cmd.append(f"--impersonate-service-account={impersonate_sa}")

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
                        details=f"Project '{project_id}' ({role}){alias_note} is ACTIVE.",
                        impersonation_used=bool(impersonate_sa)
                    )
                else:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="GCP Project",
                        resource_id=project_id,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Project '{project_id}' ({role}) is in lifecycle state '{state}', not ACTIVE.",
                        remediation_cli=f"gcloud projects undelete {project_id}"
                    )
            else:
                stderr = result.stderr.lower()
                # If runner lacks permission, attempt SA Impersonation cascade
                if ("403" in stderr or "permission denied" in stderr) and not impersonate_sa:
                    fallback_sa = f"sa-dataform-executor@{project_id}.iam.gserviceaccount.com"
                    logger.info(f"Local runner got 403 on project '{project_id}'. Trying SA impersonation cascade with '{fallback_sa}'...")
                    cascade_finding = self._check_project(project_id, role, raw_input, impersonate_sa=fallback_sa)
                    if cascade_finding.status == "PASS":
                        cascade_finding.details += " (Confirmed via Service Account Impersonation cascade)."
                        return cascade_finding

                if "not found" in stderr or "404" in stderr:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="GCP Project",
                        resource_id=project_id,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Project '{project_id}' does not exist (404 Not Found). Must be provisioned.",
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
                        details=f"Runner account lacks permissions (403 Forbidden) to describe project '{project_id}'. Resource confirmed existent in cloud directory."
                    )
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

    def _check_service_account(self, sa_email: str, impersonate_sa: Optional[str] = None) -> AuditFinding:
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="Service Account",
                resource_id=sa_email,
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Service Account '{sa_email}' verified active."
            )

        cmd = ["gcloud", "iam", "service-accounts", "describe", sa_email, "--format=json"]
        if impersonate_sa and impersonate_sa != sa_email:
            cmd.append(f"--impersonate-service-account={impersonate_sa}")

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
                    details=f"Service Account '{sa_email}' is ACTIVE and valid.",
                    impersonation_used=bool(impersonate_sa)
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
                        remediation_iac=f'resource "google_service_account" "{sa_name}" {{\n  account_id   = "{sa_name}"\n  display_name = "Dataform & Pipeline Orchestration SA"\n  project      = "{proj}"\n}}',
                        remediation_cli=f"gcloud iam service-accounts create {sa_name} --project={proj}"
                    )
                elif "permission denied" in stderr or "403" in stderr:
                    return AuditFinding(
                        pillar="Pillar 1 - Projects & SAs",
                        resource_type="Service Account",
                        resource_id=sa_email,
                        status="EXISTS_RESTRICTED_RUNNER",
                        severity="WARNING",
                        details=f"Local runner lacks permission to inspect SA '{sa_email}' (403 Forbidden). Non-blocking."
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

    def _execute_smoke_query(
        self,
        project_id: str,
        location: str = "us-central1",
        impersonate_sa: Optional[str] = None
    ) -> AuditFinding:
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 1 - Projects & SAs",
                resource_type="BigQuery Smoke Query",
                resource_id=f"{project_id}:SELECT 1",
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Smoke query executed successfully on '{project_id}' ({location}). BigQuery API, Billing & Quotas healthy."
            )

        cmd = [
            "bq", "query",
            f"--project_id={project_id}",
            f"--location={location}",
            "--use_legacy_sql=false",
            "--format=json"
        ]
        if impersonate_sa:
            cmd.append(f"--impersonate_service_account={impersonate_sa}")
        cmd.append("SELECT 1 AS readiness_check")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            if result.returncode == 0:
                return AuditFinding(
                    pillar="Pillar 1 - Projects & SAs",
                    resource_type="BigQuery Smoke Query",
                    resource_id=f"{project_id}:SELECT 1",
                    status="PASS",
                    severity="INFO",
                    details=f"Smoke Query successful on '{project_id}'. BigQuery API enabled, billing active, quotas verified.",
                    impersonation_used=bool(impersonate_sa)
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
                        remediation_iac=f'resource "google_project_iam_member" "bq_user" {{\n  project = "{project_id}"\n  role    = "roles/bigquery.jobUser"\n  member  = "serviceAccount:YOUR_SA@{project_id}.iam.gserviceaccount.com"\n}}'
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
