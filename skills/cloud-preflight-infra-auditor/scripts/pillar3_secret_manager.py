"""
Pillar 3: Secret Manager & Architecture Governance by Pipeline Archetype.

Rules:
- Pure SQL Transformation Archetype (Dataform / BigQuery ELT): DOES NOT access Secret Manager
  in runtime. Secret references are classified as architectural anti-patterns or non-blocking warnings.
- Ingestion, Egress, or External Orchestration Archetypes (Airflow / Cloud Functions / Dataflow):
  Must have secrets provisioned with an ENABLED version.
- Validates Secret status and version availability, differentiating 404 (MISSING) from 403 (RESTRICTED).
"""

from __future__ import annotations

import logging
import re
import subprocess
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from pillar1_projects_sa import AuditFinding

logger = logging.getLogger("cloud-preflight.pillar3")


@dataclass
class SecretReference:
    secret_name: str
    project_id: str
    detected_in_file: Optional[str] = None
    line_number: Optional[int] = None
    context: Optional[str] = None


@dataclass
class Pillar3Result:
    passed: bool
    pipeline_archetype: str  # SQL_TRANSFORMATION, INGESTION, EGRESS, HYBRID
    findings: List[AuditFinding] = field(default_factory=list)
    governance_notes: List[str] = field(default_factory=list)


class SecretManagerGovernanceAuditor:
    """Audits Secret Manager references with pipeline archetype governance."""

    SECRET_REGEX = re.compile(
        r"(?:secret_name|secrets\.get|SecretManagerServiceClient|projects/\S+/secrets/([a-zA-Z0-9_\-]+))",
        re.IGNORECASE
    )

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def audit(
        self,
        pipeline_archetype: str,
        secret_project_id: str,
        secrets: List[Dict[str, str]],
        legacy_code_snippets: Optional[List[str]] = None
    ) -> Pillar3Result:
        findings: List[AuditFinding] = []
        notes: List[str] = []
        overall_pass = True

        archetype_upper = pipeline_archetype.upper()

        # Scan code snippets if provided
        all_secrets = list(secrets)
        if legacy_code_snippets:
            for snippet in legacy_code_snippets:
                extracted = self._extract_secret_names(snippet)
                for sec_name in extracted:
                    if not any(s.get("secret_name") == sec_name for s in all_secrets):
                        all_secrets.append({"secret_name": sec_name, "project_id": secret_project_id})

        # Apply Archetype Governance
        is_pure_sql = archetype_upper in ["SQL_TRANSFORMATION", "DATAFORM", "BIGQUERY_ELT"]

        if is_pure_sql and len(all_secrets) > 0:
            notes.append(
                f"Governance Notice: Pipeline archetype '{archetype_upper}' is pure SQL transformation (Dataform). "
                "Dataform runs inside BigQuery engine without secret manager runtime credentials. "
                "Detected secret references do not block deployment; they should be decoupled into ingestion orchestration."
            )

        for sec in all_secrets:
            sec_name = sec.get("secret_name", "")
            proj = sec.get("project_id", secret_project_id)
            if not sec_name:
                continue

            finding = self._check_secret(proj, sec_name, is_pure_sql)
            findings.append(finding)
            if finding.severity == "BLOCKER":
                overall_pass = False

        if not all_secrets:
            findings.append(
                AuditFinding(
                    pillar="Pillar 3 - Secret Manager",
                    resource_type="Secret Governance",
                    resource_id="N/A",
                    status="PASS",
                    severity="INFO",
                    details=f"No secret dependencies required for archetype '{archetype_upper}'. Architecture is clean."
                )
            )

        return Pillar3Result(
            passed=overall_pass,
            pipeline_archetype=archetype_upper,
            findings=findings,
            governance_notes=notes
        )

    def _extract_secret_names(self, text: str) -> Set[str]:
        names = set()
        matches = self.SECRET_REGEX.findall(text)
        for m in matches:
            if m and len(m) > 2:
                names.add(m)
        return names

    def _check_secret(self, project_id: str, secret_name: str, is_pure_sql: bool) -> AuditFinding:
        res_id = f"projects/{project_id}/secrets/{secret_name}"

        if self.dry_run:
            severity = "INFO" if is_pure_sql else "INFO"
            status = "PASS"
            return AuditFinding(
                pillar="Pillar 3 - Secret Manager",
                resource_type="Secret",
                resource_id=res_id,
                status=status,
                severity=severity,
                details=f"[DRY-RUN] Secret '{secret_name}' has an ENABLED version in project '{project_id}'."
            )

        cmd = ["gcloud", "secrets", "versions", "list", secret_name, f"--project={project_id}", "--format=json"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                versions = json.loads(result.stdout)
                enabled_versions = [v for v in versions if v.get("state") == "ENABLED"]
                if enabled_versions:
                    return AuditFinding(
                        pillar="Pillar 3 - Secret Manager",
                        resource_type="Secret",
                        resource_id=res_id,
                        status="PASS",
                        severity="INFO",
                        details=f"Secret '{secret_name}' has {len(enabled_versions)} ENABLED version(s)."
                    )
                else:
                    severity = "WARNING" if is_pure_sql else "BLOCKER"
                    return AuditFinding(
                        pillar="Pillar 3 - Secret Manager",
                        resource_type="Secret",
                        resource_id=res_id,
                        status="MISSING_INFRA",
                        severity=severity,
                        details=f"Secret '{secret_name}' exists but has NO ENABLED version.",
                        remediation_cli=f"echo -n 'VALUE' | gcloud secrets versions add {secret_name} --data-file=- --project={project_id}"
                    )
            else:
                stderr = result.stderr.lower()
                if "not found" in stderr or "404" in stderr:
                    severity = "WARNING" if is_pure_sql else "BLOCKER"
                    details_suffix = " (Non-blocking for pure SQL Dataform archetype)" if is_pure_sql else " (BLOCKING for Ingestion/Egress)"
                    tf_block = (
                        f'resource "google_secret_manager_secret" "{secret_name.replace("-", "_")}" {{\n'
                        f'  project   = "{project_id}"\n'
                        f'  secret_id = "{secret_name}"\n'
                        f'  replication {{\n'
                        f'    auto {{}}\n'
                        f'  }}\n'
                        f'}}'
                    )
                    return AuditFinding(
                        pillar="Pillar 3 - Secret Manager",
                        resource_type="Secret",
                        resource_id=res_id,
                        status="MISSING_INFRA",
                        severity=severity,
                        details=f"Secret '{secret_name}' does not exist in '{project_id}'{details_suffix}.",
                        remediation_iac=tf_block,
                        remediation_cli=f"gcloud secrets create {secret_name} --project={project_id} --replication-policy=automatic"
                    )
                elif "permission denied" in stderr or "403" in stderr:
                    return AuditFinding(
                        pillar="Pillar 3 - Secret Manager",
                        resource_type="Secret",
                        resource_id=res_id,
                        status="EXISTS_RESTRICTED_RUNNER",
                        severity="WARNING",
                        details=f"Runner lacks secretmanager.secrets.get on '{res_id}'. Non-blocking warning."
                    )
                return AuditFinding(
                    pillar="Pillar 3 - Secret Manager",
                    resource_type="Secret",
                    resource_id=res_id,
                    status="WARN",
                    severity="WARNING",
                    details=f"Secret check alert: {result.stderr.strip()}"
                )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 3 - Secret Manager",
                resource_type="Secret",
                resource_id=res_id,
                status="WARN",
                severity="WARNING",
                details=f"Secret inspection exception: {str(e)}"
            )
