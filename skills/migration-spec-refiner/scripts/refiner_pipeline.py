#!/usr/bin/env python3
"""
Migration Spec Refiner - Master Orchestrator Pipeline (.py-First + Surgical LLM).

Executes the 4 deterministic stages:
  Stage 1: resolve_refinement_context.py (SSOT Lock & Preflight Integration)
  Stage 2: scaffold_dataform_and_review.py (Deterministic Scaffolding + Prompt Prep)
  Stage 3: validate_refinement_gate.py (Fail-Closed Gate & 4x4 Parity Audit)
  Stage 4: build_and_publish_pbi.py (Parity by Construction & Atomic Telemetry)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

from stage1_resolve_refinement_context import ContextLockResolver
from stage2_scaffold_dataform_and_review import DataformScaffolder
from stage3_validate_refinement_gate import RefinementGateValidator
from stage4_build_and_publish_pbi import SpecPublisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("migration-spec-refiner")


def run_pipeline(
    ticket_id: str,
    target_table: str,
    catalog_path: str,
    legacy_sql: str,
    output_dir: str = "./refinement_artifacts",
    dry_run: bool = False
) -> Dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------
    # STAGE 1: SSOT Lock & Pre-Flight
    # -------------------------------------------------------------
    logger.info(">>> Stage 1: Resolving SSOT Authoritative Coordinates & Locking Context...")
    stage1 = ContextLockResolver()
    locked_context = stage1.resolve_and_lock(
        ticket_id=ticket_id,
        target_table_name=target_table,
        canonical_catalog_path=catalog_path,
        legacy_sql_or_code=legacy_sql,
        output_dir=output_dir
    )

    # -------------------------------------------------------------
    # STAGE 2: Deterministic Dataform Scaffolding
    # -------------------------------------------------------------
    logger.info(">>> Stage 2: Scaffolding Dataform SQLx, Sources, Tests & Spec Skeleton...")
    stage2 = DataformScaffolder()
    skeleton = stage2.scaffold(locked_context, output_dir=output_dir)

    # Simulate LLM Surgical Step (in production, this invokes the LLM with surgical_prompt)
    model_sqlx_path = out_dir / "definitions" / "02_models" / f"{target_table}.sqlx"
    simulated_sql_select = (
        "SELECT\n"
        "  c.customer_id,\n"
        "  c.customer_name,\n"
        "  o.order_id,\n"
        "  o.order_amount,\n"
        "  CURRENT_TIMESTAMP() AS processed_at\n"
        f"FROM\n"
        f"  ${{ref('raw_customers')}} c\n"
        f"LEFT JOIN\n"
        f"  ${{ref('orders_v2')}} o ON c.customer_id = o.customer_id\n"
    )

    # Write model with config + surgical query
    config_header = (
        f"config {{\n"
        f'  type: "{locked_context["table_type"]}",\n'
        f'  schema: "{locked_context["target_dataset"]}",\n'
        f'  name: "{locked_context["target_table"]}",\n'
        f'  description: "Modernized model for {target_table}",\n'
        f"  tags: [\"modernization\", \"{locked_context['target_dataset']}\"],\n"
        f"  uniqueKey: {json.dumps(locked_context['unique_keys'])}\n"
        f"}}\n\n"
    )
    model_sqlx_path.write_text(config_header + simulated_sql_select, encoding="utf-8")

    # -------------------------------------------------------------
    # STAGE 3: Fail-Closed Gate & 4x4 Parity Audit
    # -------------------------------------------------------------
    logger.info(">>> Stage 3: Executing Fail-Closed Gate & 4x4 Parity Audit...")
    stage3 = RefinementGateValidator(dry_run=dry_run)
    gate_res = stage3.validate(
        locked_context_path=str(out_dir / "context_locked.json"),
        sqlx_model_path=str(model_sqlx_path),
        spec_json_path=str(out_dir / "spec_skeleton.json")
    )

    if not gate_res.passed:
        logger.error(f"Refinement Gate FAILED with violations: {gate_res.architectural_violations}")
        return {"passed": False, "violations": gate_res.architectural_violations}

    logger.info("Refinement Gate PASSED with 4x4 Parity verified.")

    # -------------------------------------------------------------
    # STAGE 4: Build & Publish PBI with Atomic Telemetry
    # -------------------------------------------------------------
    logger.info(">>> Stage 4: Publishing Finalized PBI Specification & Logging Telemetry...")
    stage4 = SpecPublisher()
    business_summary = (
        "- Modernized customer analytics dimension consolidating legacy Spark batch join.\n"
        "- Converted into BigQuery incremental merge pattern keyed on `customer_id`.\n"
        "- Replaces on-prem HDFS storage with cloud-native BigQuery storage & Dataform transformations."
    )
    pub_res = stage4.build_and_publish(
        locked_context=locked_context,
        business_summary=business_summary,
        sql_logic_select=simulated_sql_select,
        output_dir=output_dir,
        telemetry_log_path=str(out_dir / "telemetry_run.jsonl")
    )

    return {
        "passed": True,
        "spec_file": pub_res["spec_markdown_path"],
        "gate_result": gate_res.__dict__
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Migration Spec Refiner Pipeline (.py-First + Surgical LLM)")
    parser.add_argument("--ticket-id", "-t", type=str, default="MIG-502", help="Agile Work Item Ticket ID")
    parser.add_argument("--target-table", type=str, default="dim_customer_enriched", help="Target table name")
    parser.add_argument("--catalog", "-c", type=str, required=True, help="Path to SSOT canonical catalog JSON")
    parser.add_argument("--output-dir", "-o", type=str, default="./refinement_artifacts", help="Output directory")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Simulate compilation without live APIs")

    args = parser.parse_args()

    legacy_sql = "SELECT c.id, c.name, o.amount FROM raw_customers c JOIN orders_v2 o ON c.id = o.customer_id"

    result = run_pipeline(
        ticket_id=args.ticket_id,
        target_table=args.target_table,
        catalog_path=args.catalog,
        legacy_sql=legacy_sql,
        output_dir=args.output_dir,
        dry_run=args.dry_run
    )

    if not result.get("passed"):
        logger.error("Pipeline stopped at Refinement Gate.")
        sys.exit(2)

    logger.info(f"Pipeline executed successfully! Artifacts in: {args.output_dir}")


if __name__ == "__main__":
    main()
