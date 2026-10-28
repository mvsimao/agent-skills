#!/usr/bin/env python3
"""
Cloud Preflight Infrastructure Auditor - Main Orchestrator.

Audits cloud readiness across 6 Pillars before data migration refinement or deployment:
1. Cloud Projects, SAs, and Live Smoke Query (BigQuery SELECT 1)
2. Datasets & Cross-Project IAM ACLs (roles/bigquery.dataViewer)
3. Secret Manager & Governance by Pipeline Archetype
4. GCS Buckets & Layer Separation (Landing vs Managed Lakehouse)
5. External Tables (Drive/Sheets 403 probe), Git Repos, and DML-Only Target Tables
6. External Network Connectivity (Cloud NAT Egress IP discovery & Handshakes)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

from pillar1_projects_sa import ProjectAndServiceAccountAuditor, AuditFinding
from pillar2_datasets_iam import DatasetAndIAMAuditor
from pillar3_secret_manager import SecretManagerGovernanceAuditor
from pillar4_object_storage import ObjectStorageAuditor
from pillar5_external_sources import ExternalSourcesAndGitAuditor
from pillar6_network_connectivity import NetworkConnectivityAuditor
from reporters import PreflightReportGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("cloud-preflight-auditor")


def run_preflight_audit(manifest: Dict[str, Any], dry_run: bool = False) -> tuple[bool, str, Dict[str, Any]]:
    """Runs all 6 pillars sequentially and compiles the results."""
    pipeline_name = manifest.get("pipeline_name", "unnamed-pipeline")
    archetype = manifest.get("archetype", "SQL_TRANSFORMATION")
    target_project = manifest.get("projects", {}).get("target", "acme-dwh-prod")
    consumer_sa = manifest.get("service_accounts", [f"sa-dataform-executor@{target_project}.iam.gserviceaccount.com"])[0]

    all_findings: list[AuditFinding] = []
    overall_ready = True

    logger.info("Executing Pillar 1: Projects, Service Accounts & Smoke Query...")
    p1 = ProjectAndServiceAccountAuditor(dry_run=dry_run)
    res1 = p1.audit(
        projects=manifest.get("projects", {"target": target_project}),
        service_accounts=manifest.get("service_accounts", [consumer_sa]),
        execution_project=target_project
    )
    all_findings.extend(res1.findings)
    if not res1.passed:
        overall_ready = False

    logger.info("Executing Pillar 2: Datasets & Cross-Project IAM ACLs...")
    p2 = DatasetAndIAMAuditor(dry_run=dry_run)
    res2 = p2.audit(
        target_project=target_project,
        datasets=manifest.get("datasets", []),
        consumer_sa=consumer_sa
    )
    all_findings.extend(res2.findings)
    if not res2.passed:
        overall_ready = False

    logger.info("Executing Pillar 3: Secret Manager & Archetype Governance...")
    p3 = SecretManagerGovernanceAuditor(dry_run=dry_run)
    res3 = p3.audit(
        pipeline_archetype=archetype,
        secret_project_id=manifest.get("projects", {}).get("secrets", target_project),
        secrets=manifest.get("secrets", []),
        legacy_code_snippets=manifest.get("legacy_code_snippets", [])
    )
    all_findings.extend(res3.findings)
    if not res3.passed:
        overall_ready = False

    logger.info("Executing Pillar 4: Object Storage & Tier Separation...")
    p4 = ObjectStorageAuditor(dry_run=dry_run)
    res4 = p4.audit(
        target_project=target_project,
        declared_buckets=manifest.get("buckets", []),
        code_snippets=manifest.get("legacy_code_snippets", [])
    )
    all_findings.extend(res4.findings)
    if not res4.passed:
        overall_ready = False

    logger.info("Executing Pillar 5: External Sources, Git Repos & DML Targets...")
    p5 = ExternalSourcesAndGitAuditor(dry_run=dry_run)
    res5 = p5.audit(
        external_tables=manifest.get("external_tables", []),
        git_repo_url=manifest.get("git_repo_url"),
        dml_target_tables=manifest.get("dml_target_tables", []),
        service_account_email=consumer_sa
    )
    all_findings.extend(res5.findings)
    if not res5.passed:
        overall_ready = False

    logger.info("Executing Pillar 6: Network Connectivity & Cloud NAT Egress...")
    p6 = NetworkConnectivityAuditor(dry_run=dry_run)
    res6 = p6.audit(
        endpoints=manifest.get("network_endpoints", []),
        code_snippets=manifest.get("legacy_code_snippets", []),
        egress_ip_override=manifest.get("egress_ip_override")
    )
    all_findings.extend(res6.findings)
    if not res6.passed:
        overall_ready = False

    metadata = {
        "pipeline_name": pipeline_name,
        "archetype": archetype,
        "target_project": target_project,
        "consumer_sa": consumer_sa,
        "egress_nat_ip": res6.egress_nat_ip,
        "dry_run": dry_run
    }

    markdown_report = PreflightReportGenerator.generate_markdown_report(
        pipeline_name=pipeline_name,
        is_ready=overall_ready,
        findings=all_findings,
        metadata=metadata
    )

    json_report = PreflightReportGenerator.generate_json_summary(
        pipeline_name=pipeline_name,
        is_ready=overall_ready,
        findings=all_findings,
        metadata=metadata
    )

    return overall_ready, markdown_report, json_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Cloud Preflight Infrastructure Auditor (6 Pillars)")
    parser.add_argument("--manifest", "-m", type=str, required=True, help="Path to preflight input JSON manifest")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Simulate cloud checks without running gcloud/bq CLI")
    parser.add_argument("--output-md", "-o", type=str, default=None, help="Path to write the executive Markdown report")
    parser.add_argument("--output-json", "-j", type=str, default=None, help="Path to write the structured JSON report")
    parser.add_argument("--fail-on-blocker", action="store_true", default=False, help="Exit with non-zero code if blockers exist")

    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        logger.error(f"Manifest file not found: {manifest_path}")
        sys.exit(1)

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    is_ready, md_report, json_report = run_preflight_audit(manifest, dry_run=args.dry_run)

    if args.output_md:
        out_p = Path(args.output_md)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(md_report, encoding="utf-8")
        logger.info(f"Wrote executive Markdown report to: {out_p}")
    else:
        print("\n" + md_report + "\n")

    if args.output_json:
        out_j = Path(args.output_json)
        out_j.parent.mkdir(parents=True, exist_ok=True)
        out_j.write_text(json.dumps(json_report, indent=2), encoding="utf-8")
        logger.info(f"Wrote JSON report to: {out_j}")

    if args.fail_on_blocker and not is_ready:
        logger.error("Preflight audit failed with blockers (FAIL-CLOSED gate).")
        sys.exit(2)


if __name__ == "__main__":
    main()
