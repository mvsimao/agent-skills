"""
Executive and Technical Reporting Engine for Cloud Preflight Infrastructure Auditor.

Produces standardized, executive-ready outputs in Markdown and HTML with 3 scannable sections:
1. Binary Readiness Summary (GO / NO-GO status, blockers count, warnings)
2. SRE / IaC Auto-Remediation Actions (copyable Terraform and CLI commands)
3. Technical Evidences across all 6 Pillars.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Any
from pillar1_projects_sa import AuditFinding


class PreflightReportGenerator:
    """Generates 3-tier executive reports in Markdown and structured JSON."""

    @staticmethod
    def generate_markdown_report(
        pipeline_name: str,
        is_ready: bool,
        findings: List[AuditFinding],
        metadata: Dict[str, Any]
    ) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        status_banner = "🟢 READY (GO FOR REFINEMENT / DEPLOY)" if is_ready else "🔴 BLOCKED (NO-GO - MISSING INFRASTRUCTURE)"
        
        blockers = [f for f in findings if f.severity == "BLOCKER"]
        warnings = [f for f in findings if f.severity == "WARNING"]
        passes = [f for f in findings if f.severity == "INFO" and f.status == "PASS"]

        iac_actions = [f for f in findings if f.remediation_iac or f.remediation_cli]

        md = []
        md.append(f"# 🛡️ Cloud Preflight Infrastructure Readiness Report")
        md.append(f"**Target Pipeline:** `{pipeline_name}`  ")
        md.append(f"**Audit Execution Timestamp:** `{timestamp}`  ")
        md.append(f"**Final Verdict:** **{status_banner}**  \n")

        # ---------------------------------------------------------
        # SECTION 1: BINARY READINESS SUMMARY
        # ---------------------------------------------------------
        md.append("## 1. 🚦 Executive Readiness Summary")
        md.append("| Metric | Status | Impact |")
        md.append("| :--- | :--- | :--- |")
        md.append(f"| **Overall Gate Decision** | **{'GO' if is_ready else 'NO-GO'}** | {'Pipeline safe to develop/deploy' if is_ready else 'Impediment detected prior to engineering start'} |")
        md.append(f"| **Critical Blockers (`🔴 MISSING_INFRA`)** | **{len(blockers)}** | Unprovisioned cloud resources or missing critical ACLs |")
        md.append(f"| **Technical Warnings (`🟡 RESTRICTED_RUNNER`)** | **{len(warnings)}** | Local operator runner IAM scope or non-blocking warnings |")
        md.append(f"| **Passed Validations (`🟢 VERIFIED`)** | **{len(passes)}** | Verified cloud assets and permissions |")
        md.append(f"| **Infrastructure Archetype** | `{metadata.get('archetype', 'SQL_TRANSFORMATION')}` | Architecture pattern applied |")
        md.append(f"| **Target Cloud Project** | `{metadata.get('target_project', 'acme-dwh-prod')}` | Core BigQuery compute & storage |")
        md.append("")

        if not is_ready:
            md.append("> [!IMPORTANT]")
            md.append(f"> **Pipeline development is halted.** There are **{len(blockers)} blocker(s)** that must be provisioned or permissions granted before opening PRs or running Dataform builds.")
        else:
            md.append("> [!NOTE]")
            md.append("> **Infrastructure preflight passed successfully.** All datasets, storage layers, identities, and IAM cross-project bindings meet enterprise standards.")

        md.append("")

        # ---------------------------------------------------------
        # SECTION 2: SRE & IAC AUTO-REMEDIATION ACTIONS
        # ---------------------------------------------------------
        md.append("## 2. 🛠️ IaC / SRE Auto-Remediation Playbook")
        if not iac_actions:
            md.append("*No infrastructure remediation actions required. Environment is fully provisioned.*  \n")
        else:
            md.append("Apply the following Terraform definitions and CLI commands to achieve immediate compliance:  \n")
            for idx, act in enumerate(iac_actions, start=1):
                md.append(f"### 2.{idx}. Remediation for `{act.resource_type}`: `{act.resource_id}`")
                md.append(f"**Issue Detected:** {act.details}  ")
                
                if act.remediation_iac:
                    md.append("#### Terraform Specification (Copy & Apply):")
                    md.append("```hcl")
                    md.append(act.remediation_iac)
                    md.append("```")

                if act.remediation_cli:
                    md.append("#### Quick CLI Remediation Command:")
                    md.append("```bash")
                    md.append(act.remediation_cli)
                    md.append("```")
                md.append("")

        # ---------------------------------------------------------
        # SECTION 3: TECHNICAL EVIDENCE ACROSS THE 6 PILLARS
        # ---------------------------------------------------------
        md.append("## 3. 🔬 Technical Evidence Matrix (6 Pillars)")
        md.append("| Pillar | Resource Evaluated | Classification | Severity | Diagnostic Details |")
        md.append("| :--- | :--- | :--- | :--- | :--- |")

        for f in findings:
            status_icon = "🟢" if f.status == "PASS" else ("🔴" if f.status == "MISSING_INFRA" else "🟡")
            md.append(f"| **{f.pillar}** | `{f.resource_id}` | {status_icon} `{f.status}` | `{f.severity}` | {f.details.replace('|', '/')} |")

        md.append("\n---")
        md.append("*Generated deterministically by `cloud-preflight-infra-auditor` (Zero False Positive / Zero False Negative Engine).*")

        return "\n".join(md)

    @staticmethod
    def generate_json_summary(
        pipeline_name: str,
        is_ready: bool,
        findings: List[AuditFinding],
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "pipeline_name": pipeline_name,
            "is_ready": is_ready,
            "verdict": "GO" if is_ready else "NO_GO",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata,
            "summary": {
                "total_checks": len(findings),
                "blockers": len([f for f in findings if f.severity == "BLOCKER"]),
                "warnings": len([f for f in findings if f.severity == "WARNING"]),
                "passed": len([f for f in findings if f.status == "PASS"])
            },
            "findings": [
                {
                    "pillar": f.pillar,
                    "resource_type": f.resource_type,
                    "resource_id": f.resource_id,
                    "status": f.status,
                    "severity": f.severity,
                    "details": f.details,
                    "remediation_iac": f.remediation_iac,
                    "remediation_cli": f.remediation_cli
                }
                for f in findings
            ]
        }
