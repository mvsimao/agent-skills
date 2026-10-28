"""
Pillar 2: BigQuery Datasets & Cross-Project IAM ACL Auditor.

Validates:
- Physical existence of datasets across data lakehouse tiers (bronze, silver, gold, staging)
- Cross-project read dependencies
- Dataset IAM policy verification ensuring consuming Service Account has roles/bigquery.dataViewer
- Automatic generation of Terraform blocks (google_bigquery_dataset_iam_member) and bq CLI commands for self-remediation.
"""

from __future__ import annotations

import logging
import subprocess
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from pillar1_projects_sa import AuditFinding

logger = logging.getLogger("cloud-preflight.pillar2")


@dataclass
class DatasetDependency:
    project_id: str
    dataset_id: str
    layer: str  # bronze, silver, gold, staging
    consumer_sa: str
    is_cross_project: bool = False


@dataclass
class Pillar2Result:
    passed: bool
    findings: List[AuditFinding] = field(default_factory=list)
    remediations_terraform: List[str] = field(default_factory=list)
    remediations_cli: List[str] = field(default_factory=list)


class DatasetAndIAMAuditor:
    """Audits BigQuery datasets and cross-project ACL permissions."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def audit(
        self,
        target_project: str,
        datasets: List[Dict[str, str]],
        consumer_sa: str
    ) -> Pillar2Result:
        findings: List[AuditFinding] = []
        tf_remediations: List[str] = []
        cli_remediations: List[str] = []
        overall_pass = True

        for ds_info in datasets:
            project_id = ds_info.get("project_id", target_project)
            dataset_id = ds_info.get("dataset_id", "")
            layer = ds_info.get("layer", self._infer_layer(dataset_id))
            is_cross = (project_id != target_project)

            # 1. Check physical existence
            exist_finding = self._check_dataset_exists(project_id, dataset_id, layer)
            findings.append(exist_finding)
            if exist_finding.severity == "BLOCKER":
                overall_pass = False
                if exist_finding.remediation_iac:
                    tf_remediations.append(exist_finding.remediation_iac)
                if exist_finding.remediation_cli:
                    cli_remediations.append(exist_finding.remediation_cli)
                continue  # Cannot check IAM if dataset doesn't exist

            # 2. Check ACL / IAM for consumer Service Account
            iam_finding = self._check_dataset_iam(project_id, dataset_id, consumer_sa, is_cross)
            findings.append(iam_finding)
            if iam_finding.severity == "BLOCKER":
                overall_pass = False
                if iam_finding.remediation_iac:
                    tf_remediations.append(iam_finding.remediation_iac)
                if iam_finding.remediation_cli:
                    cli_remediations.append(iam_finding.remediation_cli)

        return Pillar2Result(
            passed=overall_pass,
            findings=findings,
            remediations_terraform=tf_remediations,
            remediations_cli=cli_remediations
        )

    def _infer_layer(self, dataset_id: str) -> str:
        d = dataset_id.lower()
        if "bronze" in d or "raw" in d:
            return "bronze"
        if "silver" in d or "cleansed" in d or "conformed" in d:
            return "silver"
        if "gold" in d or "curated" in d or "mart" in d or "dw" in d:
            return "gold"
        if "stage" in d or "staging" in d or "temp" in d:
            return "staging"
        return "custom"

    def _check_dataset_exists(self, project_id: str, dataset_id: str, layer: str) -> AuditFinding:
        full_id = f"{project_id}:{dataset_id}"
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 2 - Datasets & IAM",
                resource_type=f"BigQuery Dataset ({layer})",
                resource_id=full_id,
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Dataset '{full_id}' exists in location 'us-central1'."
            )

        cmd = ["bq", "show", "--format=json", full_id]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                return AuditFinding(
                    pillar="Pillar 2 - Datasets & IAM",
                    resource_type=f"BigQuery Dataset ({layer})",
                    resource_id=full_id,
                    status="PASS",
                    severity="INFO",
                    details=f"Dataset '{full_id}' exists and is physically accessible."
                )
            else:
                stderr = result.stderr.lower()
                if "not found" in stderr or "404" in stderr:
                    tf_block = (
                        f'resource "google_bigquery_dataset" "{dataset_id.replace("-", "_")}" {{\n'
                        f'  project                    = "{project_id}"\n'
                        f'  dataset_id                 = "{dataset_id}"\n'
                        f'  friendly_name              = "{dataset_id}"\n'
                        f'  description                = "Managed dataset for {layer} data tier"\n'
                        f'  location                   = "us-central1"\n'
                        f'  delete_contents_on_destroy = false\n'
                        f'}}'
                    )
                    cli_cmd = f"bq --project_id={project_id} mk --dataset --location=us-central1 {project_id}:{dataset_id}"
                    return AuditFinding(
                        pillar="Pillar 2 - Datasets & IAM",
                        resource_type=f"BigQuery Dataset ({layer})",
                        resource_id=full_id,
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Dataset '{full_id}' not found (404 Not Found). Must be provisioned.",
                        remediation_iac=tf_block,
                        remediation_cli=cli_cmd
                    )
                elif "permission denied" in stderr or "403" in stderr:
                    return AuditFinding(
                        pillar="Pillar 2 - Datasets & IAM",
                        resource_type=f"BigQuery Dataset ({layer})",
                        resource_id=full_id,
                        status="EXISTS_RESTRICTED_RUNNER",
                        severity="WARNING",
                        details=f"Local runner cannot describe '{full_id}' due to limited IAM (403 Forbidden). Non-blocking."
                    )
                return AuditFinding(
                    pillar="Pillar 2 - Datasets & IAM",
                    resource_type=f"BigQuery Dataset ({layer})",
                    resource_id=full_id,
                    status="WARN",
                    severity="WARNING",
                    details=f"Dataset verification alert: {result.stderr.strip()}"
                )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 2 - Datasets & IAM",
                resource_type=f"BigQuery Dataset ({layer})",
                resource_id=full_id,
                status="WARN",
                severity="WARNING",
                details=f"Dataset check exception: {str(e)}"
            )

    def _check_dataset_iam(
        self,
        project_id: str,
        dataset_id: str,
        consumer_sa: str,
        is_cross_project: bool
    ) -> AuditFinding:
        full_id = f"{project_id}:{dataset_id}"
        if self.dry_run:
            return AuditFinding(
                pillar="Pillar 2 - Datasets & IAM",
                resource_type="Dataset IAM ACL",
                resource_id=f"{full_id} -> {consumer_sa}",
                status="PASS",
                severity="INFO",
                details=f"[DRY-RUN] Service Account '{consumer_sa}' has roles/bigquery.dataViewer on '{full_id}'."
            )

        cmd = ["bq", "show", "--format=json", full_id]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                access_list = data.get("access", [])
                has_viewer = False

                for entry in access_list:
                    role = entry.get("role", "")
                    user_by_email = entry.get("userByEmail", "")
                    iam_member = entry.get("iamMember", "")
                    if (
                        consumer_sa in user_by_email
                        or f"serviceAccount:{consumer_sa}" in iam_member
                    ):
                        if role in ["READER", "roles/bigquery.dataViewer", "WRITER", "OWNER", "roles/bigquery.dataEditor", "roles/bigquery.admin"]:
                            has_viewer = True
                            break

                if has_viewer:
                    return AuditFinding(
                        pillar="Pillar 2 - Datasets & IAM",
                        resource_type="Dataset IAM ACL",
                        resource_id=f"{full_id} -> {consumer_sa}",
                        status="PASS",
                        severity="INFO",
                        details=f"Access verified: '{consumer_sa}' holds reader/viewer role on '{full_id}'."
                    )
                else:
                    tf_res_name = f"{dataset_id}_{consumer_sa.split('@')[0]}".replace("-", "_").replace(".", "_")
                    tf_block = (
                        f'resource "google_bigquery_dataset_iam_member" "{tf_res_name}" {{\n'
                        f'  project    = "{project_id}"\n'
                        f'  dataset_id = "{dataset_id}"\n'
                        f'  role       = "roles/bigquery.dataViewer"\n'
                        f'  member     = "serviceAccount:{consumer_sa}"\n'
                        f'}}'
                    )
                    cli_cmd = (
                        f"bq add-iam-policy-binding "
                        f"--member=serviceAccount:{consumer_sa} "
                        f"--role=roles/bigquery.dataViewer "
                        f"{project_id}:{dataset_id}"
                    )
                    cross_note = " (Cross-Project Dependency)" if is_cross_project else ""
                    return AuditFinding(
                        pillar="Pillar 2 - Datasets & IAM",
                        resource_type="Dataset IAM ACL",
                        resource_id=f"{full_id} -> {consumer_sa}",
                        status="MISSING_INFRA",
                        severity="BLOCKER",
                        details=f"Missing read permissions for '{consumer_sa}' on '{full_id}'{cross_note}.",
                        remediation_iac=tf_block,
                        remediation_cli=cli_cmd
                    )
            else:
                stderr = result.stderr.lower()
                if "403" in stderr or "permission denied" in stderr:
                    return AuditFinding(
                        pillar="Pillar 2 - Datasets & IAM",
                        resource_type="Dataset IAM ACL",
                        resource_id=f"{full_id} -> {consumer_sa}",
                        status="EXISTS_RESTRICTED_RUNNER",
                        severity="WARNING",
                        details=f"Runner account lacks permission to inspect IAM on '{full_id}'."
                    )
                return AuditFinding(
                    pillar="Pillar 2 - Datasets & IAM",
                    resource_type="Dataset IAM ACL",
                    resource_id=f"{full_id} -> {consumer_sa}",
                    status="WARN",
                    severity="WARNING",
                    details=f"Could not inspect IAM for '{full_id}': {result.stderr.strip()}"
                )
        except Exception as e:
            return AuditFinding(
                pillar="Pillar 2 - Datasets & IAM",
                resource_type="Dataset IAM ACL",
                resource_id=f"{full_id} -> {consumer_sa}",
                status="WARN",
                severity="WARNING",
                details=f"IAM inspection error: {str(e)}"
            )
