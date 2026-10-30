#!/usr/bin/env python3
"""
Data Lineage Upstream Validator - Main Orchestrator.

Validates upstream data dependencies across 4 layers:
- Layer 1: Data Warehouse Live Physical Verification with Service Account Impersonation Fallback
- Layer 2: Dataform Repository Contract Fallback (Primary & Sibling Projects with Draft Guardrail)
- Layer 3: 6-Layer Cascaded Legacy Lineage Resolver with Sovereign Graph & Architecture Overrides
- Layer 4: Agile Task Graph & Backlog Auditor (Detects INVERTED_LINK, UNLINKED_PREDECESSOR, PREDECESSOR_WITHOUT_PBI, OMITTED_SOURCES)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

from source_target_extractor import SourceTargetExtractor, LineageExtractionResult
from dataform_contract_checker import DataformContractChecker, UpstreamValidationStatus
from resolve_legacy_lineage import LegacyLineageResolver, ProducerDiscovery
from backlog_task_auditor import BacklogTaskAuditor, PredecessorRecommendation

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("lineage-validator")


def run_lineage_validation(
    config: Dict[str, Any],
    dry_run: bool = False,
    impersonate_sa: Optional[str] = None
) -> tuple[bool, str, Dict[str, Any]]:
    ticket_id = config.get("ticket_id", "DATA-1001")
    code_text = config.get("code_text", "")
    dataform_repo_path = config.get("dataform_repo_path")
    sibling_repo_paths = config.get("sibling_repo_paths", [])
    is_draft_workspace = config.get("is_draft_workspace", False)
    legacy_context = config.get("legacy_context", {})
    backlog_items = config.get("backlog_items", [])
    documented_sources = config.get("documented_sources", [])
    default_project = config.get("default_project", "acme-dwh-prod")
    non_as_is_consolidations = config.get("non_as_is_consolidations", {})

    # 1. Structured Lineage Extraction
    logger.info("Extracting read vs write boundaries with consolidation awareness...")
    extractor = SourceTargetExtractor()
    extracted: LineageExtractionResult = extractor.extract(
        code_text=code_text,
        explicit_targets=config.get("explicit_targets"),
        non_as_is_consolidations=non_as_is_consolidations,
        default_project=default_project
    )

    # 2. Check Live DW with SA Impersonation & Dataform Fallback
    logger.info(f"Auditing {len(extracted.upstream_sources)} upstream source(s)...")
    checker = DataformContractChecker(dry_run=dry_run)
    statuses: List[UpstreamValidationStatus] = []
    missing_for_resolver: List[Dict[str, str]] = []

    for src in extracted.upstream_sources:
        p_id = src.get("project_id", default_project)
        d_id = src.get("dataset_id", "default")
        t_id = src.get("table_id", "")
        status = checker.check_upstream_table(
            project_id=p_id,
            dataset_id=d_id,
            table_id=t_id,
            dataform_repo_path=dataform_repo_path,
            sibling_repo_paths=sibling_repo_paths,
            is_draft_workspace=is_draft_workspace,
            impersonate_sa=impersonate_sa
        )
        statuses.append(status)
        if status.status in ["MISSING_UNRESOLVED", "UNSTARTED_PREDECESSOR"]:
            missing_for_resolver.append({"table_name": t_id, "project_id": p_id, "dataset_id": d_id})

    # 3. 6-Layer Cascaded Legacy Lineage Resolution
    discovered_producers: List[ProducerDiscovery] = []
    resolver = LegacyLineageResolver(dry_run=dry_run)
    missing_producers_for_backlog = []

    for missing in missing_for_resolver:
        t_name = missing["table_name"]
        discovery = resolver.resolve(t_name, legacy_context)
        if discovery:
            discovered_producers.append(discovery)
            missing_producers_for_backlog.append({
                "table_name": t_name,
                "producer_job": discovery.discovered_producer_job
            })
        else:
            missing_producers_for_backlog.append({
                "table_name": t_name,
                "producer_job": f"UNKNOWN_JOB_FOR_{t_name}"
            })

    # 4. Backlog Task Graph Audit with 4 Classes of Governance Anomalies
    logger.info("Auditing backlog dependencies and detecting governance anomalies...")
    task_auditor = BacklogTaskAuditor()
    phys_sources_list = [s["table_id"] for s in extracted.upstream_sources]

    backlog_recs: List[PredecessorRecommendation] = task_auditor.audit_predecessor_tasks(
        current_ticket_id=ticket_id,
        missing_tables_producers=missing_producers_for_backlog,
        backlog_items=backlog_items,
        physically_extracted_sources=phys_sources_list if documented_sources else None,
        documented_sources=documented_sources if documented_sources else None
    )

    # Determine Gate Approval and 3-Color Traffic Light Classification
    has_unstarted_task_dep = any(s.status == "UNSTARTED_PREDECESSOR" for s in statuses)
    has_unpopulated = any(s.status == "UNPOPULATED_UPSTREAM" for s in statuses)
    has_missing_unresolved = any(s.status == "MISSING_UNRESOLVED" for s in statuses)
    has_link_anomalies = any(len(br.link_anomalies) > 0 for br in backlog_recs)
    has_unlinked_predecessors = any(br.predecessor_ticket_id is None and br.required_table for br in backlog_recs)

    if not has_unstarted_task_dep and not has_unpopulated and not has_missing_unresolved and not has_link_anomalies and not has_unlinked_predecessors:
        traffic_light = "🟢 VERDE (READY)"
        gate_verdict = "APPROVED (100% of sources exist/populated & zero link issues)"
        is_ready = True
    elif has_unstarted_task_dep and not has_missing_unresolved and not has_link_anomalies:
        traffic_light = "🟡 AMARELO (TASK DEPENDENCY)"
        gate_verdict = "WAITING FOR PREDECESSOR TASK (Upstream card mapped but not yet completed)"
        is_ready = False
    else:
        traffic_light = "🔴 VERMELHO (BLOCKED / GOVERNANCE GAP)"
        gate_verdict = "HARD BLOCKER (Orphan source without producer, unpopulated table, or link anomaly)"
        is_ready = False

    # Format Markdown Report
    md = []
    md.append(f"# 🔍 Upstream Data Lineage & Dependency Pre-Flight Report")
    md.append(f"**Migration Ticket ID:** `{ticket_id}`  ")
    md.append(f"**Pipeline Archetype:** `{extracted.pipeline_type}`  ")
    md.append(f"**Executive Traffic Light:** **{traffic_light}** — `{gate_verdict}`  ")
    md.append(f"**Final Lineage Clearance:** **{'🟢 APPROVED FOR DEVELOPMENT' if is_ready else '🔴 BLOCKED BY UPSTREAM DEPENDENCY'}**  \n")

    md.append("## 1. 📊 Lineage Extraction Boundaries")
    targets_str = ", ".join([f"{t['dataset_id']}.{t['table_id']}" for t in extracted.target_tables]) or "None"
    sources_str = ", ".join([f"{s['dataset_id']}.{s['table_id']}" for s in extracted.upstream_sources]) or "None (External Ingestion Flow)"
    md.append(f"- **Target Tables (Write):** `{targets_str}`")
    md.append(f"- **Upstream DW Tables (Read):** `{sources_str}`")
    if extracted.retired_consolidated_tables:
        md.append(f"- **Consolidated Non-AS-IS Tables (Retired):** {', '.join(extracted.retired_consolidated_tables)}")
    if extracted.external_sources:
        md.append(f"- **External Source Endpoints:** {', '.join([e['type'] + ' (' + e['uri'] + ')' for e in extracted.external_sources])}")
    md.append("")

    md.append("## 2. 🏛️ Upstream Verification Status (Layers 1 & 2)")
    md.append("| Upstream Table | Source Layer | Status | Metric / Evidence | Details |")
    md.append("| :--- | :--- | :--- | :--- | :--- |")
    for s in statuses:
        status_icon = "🟢" if s.status in ["LIVE_VERIFIED", "DATAFORM_VALIDATED"] else "🔴"
        metric = f"{s.num_rows:,} rows" if s.num_rows is not None else (s.contract_file or "N/A")
        imp_note = " [SA Impersonated]" if s.impersonation_used else ""
        md.append(f"| `{s.project_id}.{s.dataset_id}.{s.table_id}` | `{s.layer_found}` | {status_icon} `{s.status}`{imp_note} | `{metric}` | {s.details} |")
    md.append("")

    if discovered_producers:
        md.append("## 3. 🧩 6-Layer Cascaded Legacy Lineage Resolution (Layer 3)")
        md.append("| Missing Upstream Table | Discovered Legacy Producer | Resolution Layer | Confidence | Diagnostic Evidence |")
        md.append("| :--- | :--- | :--- | :--- | :--- |")
        for dp in discovered_producers:
            override_flag = " [Architectural Override]" if dp.architecture_override_applied else ""
            md.append(f"| `{dp.target_table}` | **`{dp.discovered_producer_job}`**{override_flag} | `{dp.resolution_layer}` | `{int(dp.confidence_score * 100)}%` | {dp.details} |")
        md.append("")

    if backlog_recs:
        md.append("## 4. 📋 Backlog Prioritization & Governance Anomalies (Layer 4)")
        md.append("| Required Table | Legacy Producer | Predecessor Ticket | Status | Action Item |")
        md.append("| :--- | :--- | :--- | :--- | :--- |")
        for br in backlog_recs:
            status_tag = f"`{br.ticket_status}`"
            md.append(f"| `{br.required_table}` | `{br.producer_job}` | **`{br.predecessor_ticket_id or 'UNLINKED'}`** | {status_tag} | {br.action_item} |")

        md.append("\n### ⚠️ Detected Governance Anomalies:")
        has_anomalies = any(len(br.link_anomalies) > 0 for br in backlog_recs)
        if not has_anomalies:
            md.append("*No governance link anomalies detected. Backlog graph is synchronized.*")
        else:
            for br in backlog_recs:
                for a in br.link_anomalies:
                    md.append(f"- **`{a.anomaly_type}` (Ticket `{a.ticket_id}`, Resource `{a.affected_resource}`):** {a.description}")
                    md.append(f"  *Suggested Remediation:* {a.suggested_action}")
        md.append("")

    md.append("\n---")
    md.append("*Generated deterministically by `data-lineage-upstream-validator`.*")

    markdown_report = "\n".join(md)
    json_report = {
        "ticket_id": ticket_id,
        "is_ready": is_ready,
        "pipeline_type": extracted.pipeline_type,
        "upstream_sources": extracted.upstream_sources,
        "target_tables": extracted.target_tables,
        "retired_consolidated_tables": extracted.retired_consolidated_tables,
        "upstream_statuses": [s.__dict__ for s in statuses],
        "discovered_producers": [dp.__dict__ for dp in discovered_producers],
        "backlog_recommendations": [
            {
                "required_table": r.required_table,
                "producer_job": r.producer_job,
                "predecessor_ticket_id": r.predecessor_ticket_id,
                "ticket_status": r.ticket_status,
                "action_item": r.action_item,
                "link_anomalies": [a.__dict__ for a in r.link_anomalies]
            }
            for r in backlog_recs
        ]
    }

    return is_ready, markdown_report, json_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Data Lineage Upstream Validator")
    parser.add_argument("--config", "-c", type=str, required=True, help="Path to config JSON")
    parser.add_argument("--impersonate-sa", type=str, default=None, help="Service Account for impersonation fallback")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Simulate cloud BigQuery checks")
    parser.add_argument("--output-md", "-o", type=str, default=None, help="Path to write Markdown report")
    parser.add_argument("--output-json", "-j", type=str, default=None, help="Path to write JSON report")
    parser.add_argument("--fail-on-missing", action="store_true", default=False, help="Exit with code 2 if upstream dependencies are missing")

    args = parser.parse_args()

    cfg_p = Path(args.config)
    if not cfg_p.exists():
        logger.error(f"Config file not found: {cfg_p}")
        sys.exit(1)

    with open(cfg_p, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    is_ready, md_rep, j_rep = run_lineage_validation(cfg, dry_run=args.dry_run, impersonate_sa=args.impersonate_sa)

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

    if args.fail_on_missing and not is_ready:
        logger.error("Lineage check failed with missing upstream dependencies (FAIL-CLOSED gate).")
        sys.exit(2)


if __name__ == "__main__":
    main()
