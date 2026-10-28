#!/usr/bin/env python3
"""
Cloud Preflight Infrastructure Auditor - Main Orchestrator.

Audits cloud readiness across 6 Pillars:
- Pre-refinement mode: uses central catalog and legacy code to assert cloud readiness before work begins.
- Post-refinement mode: enforces AND logic validating consistency across (1) Specification Card, (2) .sqlx files, AND (3) Sovereign Metadata Graph.
- Includes automated Service Account Impersonation cascade to eliminate false 403 blockers.
- Deterministic 404 vs 403 Matrix with copyable Terraform IaC auto-remediation.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List, Set

from pillar1_projects_sa import ProjectAndServiceAccountAuditor, AuditFinding
from pillar2_datasets_iam import DatasetAndIAMAuditor
from pillar3_secret_manager import SecretManagerGovernanceAuditor
from pillar4_object_storage import ObjectStorageAuditor
from pillar5_external_sources import ExternalSourcesAndGitAuditor
from pillar6_network_connectivity import NetworkConnectivityAuditor
from reporters import PreflightReportGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("cloud-preflight-auditor")


def extract_datasets_from_sqlx(sqlx_dir: Path) -> Set[str]:
    """Scans .sqlx files to discover declared schemas/datasets."""
    datasets = set()
    if not sqlx_dir.exists():
        return datasets

    schema_regex = re.compile(r'schema:\s*["\']([a-zA-Z0-9_]+)["\']', re.IGNORECASE)
    for f in sqlx_dir.glob("**/*.sqlx"):
        try:
            content = f.read_text(encoding="utf-8")
            for m in schema_regex.finditer(content):
                datasets.add(m.group(1))
        except Exception:
            pass
    return datasets


def run_preflight_audit(
    manifest: Dict[str, Any],
    phase: str = "pre_refinement",
    sqlx_dir_path: Optional[str] = None,
    spec_data: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
    impersonate_sa: Optional[str] = None
) -> tuple[bool, str, Dict[str, Any]]:
    pipeline_name = manifest.get("pipeline_name", "unnamed-pipeline")
    archetype = manifest.get("archetype", "SQL_TRANSFORMATION")
    
    # 1. Alias Normalization on Projects
    p1 = ProjectAndServiceAccountAuditor(dry_run=dry_run)
    raw_projects = manifest.get("projects", {})
    resolved_projects = {k: p1.normalize_project_id(v) for k, v in raw_projects.items()}
    target_project = resolved_projects.get("target", "acme-dwh-prod")
    consumer_sa = manifest.get("service_accounts", [f"sa-dataform-executor@{target_project}.iam.gserviceaccount.com"])[0]

    all_findings: list[AuditFinding] = []
    overall_ready = True

    # ------------------------------------------------------------------
    # AND-Logic Validation for Post-Refinement:
    # Cross-references: (1) Manifest/Catalog + (2) Spec + (3) .sqlx files
    # ------------------------------------------------------------------
    datasets_to_audit = list(manifest.get("datasets", []))

    if phase == "post_refinement":
        logger.info("Executing Post-Refinement cross-validation (AND-logic between Spec, .sqlx, and Metadata Graph)...")
        seen_datasets = {d.get("dataset_id") for d in datasets_to_audit if d.get("dataset_id")}

        # Scan .sqlx files
        if sqlx_dir_path:
            discovered_sqlx = extract_datasets_from_sqlx(Path(sqlx_dir_path))
            for ds in discovered_sqlx:
                if ds not in seen_datasets:
                    datasets_to_audit.append({"dataset_id": ds, "project_id": target_project, "layer": "target"})
                    seen_datasets.add(ds)

        # Cross-reference with Spec Data
        if spec_data:
            spec_datasets = spec_data.get("required_datasets", [])
            for ds in spec_datasets:
                if ds not in seen_datasets:
                    datasets_to_audit.append({"dataset_id": ds, "project_id": target_project, "layer": "spec"})
                    seen_datasets.add(ds)

    logger.info("Executing Pillar 1: Projects (with Alias Normalization), SAs & Smoke Query...")
    res1 = p1.audit(
        projects=resolved_projects,
        service_accounts=manifest.get("service_accounts", [consumer_sa]),
        execution_project=target_project,
        impersonate_sa=impersonate_sa
    )
    all_findings.extend(res1.findings)
    if not res1.passed:
        overall_ready = False

    logger.info("Executing Pillar 2: BigQuery Datasets & Cross-Project IAM ACLs...")
    p2 = DatasetAndIAMAuditor(dry_run=dry_run)
    res2 = p2.audit(
        target_project=target_project,
        datasets=datasets_to_audit,
        consumer_sa=consumer_sa,
        impersonate_sa=impersonate_sa
    )
    all_findings.extend(res2.findings)
    if not res2.passed:
        overall_ready = False

    logger.info("Executing Pillar 3: Secret Manager & Archetype Governance...")
    p3 = SecretManagerGovernanceAuditor(dry_run=dry_run)
    res3 = p3.audit(
        pipeline_archetype=archetype,
        secret_project_id=resolved_projects.get("secrets", target_project),
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

    logger.info("Executing Pillar 5: External Sources, Dataform Git Repos & DML Targets...")
    p5 = ExternalSourcesAndGitAuditor(dry_run=dry_run)
    res5 = p5.audit(
        external_tables=manifest.get("external_tables", []),
        git_repo_url=manifest.get("git_repo_url"),
        dml_target_tables=manifest.get("dml_target_tables", []),
        service_account_email=consumer_sa,
        impersonate_sa=impersonate_sa
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
        "phase": phase,
        "archetype": archetype,
        "target_project": target_project,
        "resolved_projects": resolved_projects,
        "consumer_sa": consumer_sa,
        "egress_nat_ip": res6.egress_nat_ip,
        "impersonate_sa_used": impersonate_sa,
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
    parser.add_argument("--manifest", "-m", type=str, required=True, help="Path to input manifest JSON")
    parser.add_argument("--phase", type=str, choices=["pre_refinement", "post_refinement"], default="pre_refinement", help="Audit phase")
    parser.add_argument("--sqlx-dir", type=str, default=None, help="Directory containing .sqlx files for post-refinement cross-check")
    parser.add_argument("--spec-file", type=str, default=None, help="Path to specification JSON for post-refinement cross-check")
    parser.add_argument("--impersonate-sa", type=str, default=None, help="Service Account to impersonate via IAM Credentials API")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Simulate cloud checks without running live CLI")
    parser.add_argument("--output-md", "-o", type=str, default=None, help="Path to write the Markdown report")
    parser.add_argument("--output-json", "-j", type=str, default=None, help="Path to write the JSON report")
    parser.add_argument("--fail-on-blocker", action="store_true", default=False, help="Exit with code 2 if blockers exist")

    args = parser.parse_args()

    manifest_p = Path(args.manifest)
    if not manifest_p.exists():
        logger.error(f"Manifest not found: {manifest_p}")
        sys.exit(1)

    with open(manifest_p, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    spec_data = None
    if args.spec_file and Path(args.spec_file).exists():
        with open(args.spec_file, "r", encoding="utf-8") as f:
            spec_data = json.load(f)

    is_ready, md_rep, j_rep = run_preflight_audit(
        manifest=manifest,
        phase=args.phase,
        sqlx_dir_path=args.sqlx_dir,
        spec_data=spec_data,
        dry_run=args.dry_run,
        impersonate_sa=args.impersonate_sa
    )

    if args.output_md:
        out_p = Path(args.output_md)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(md_rep, encoding="utf-8")
        logger.info(f"Wrote report to: {out_p}")
    else:
        print("\n" + md_rep + "\n")

    if args.output_json:
        out_j = Path(args.output_json)
        out_j.parent.mkdir(parents=True, exist_ok=True)
        out_j.write_text(json.dumps(j_rep, indent=2), encoding="utf-8")
        logger.info(f"Wrote JSON to: {out_j}")

    if args.fail_on_blocker and not is_ready:
        logger.error("Preflight audit failed with blockers (FAIL-CLOSED gate).")
        sys.exit(2)


if __name__ == "__main__":
    main()
