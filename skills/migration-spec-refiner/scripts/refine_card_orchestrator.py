#!/usr/bin/env python3
"""
Refine Card Orchestrator: Chained Batch Queue Engine (.py-First + Surgical LLM).

Controls refinement in chained micro-batches of <=4 cards:
- Supports --enqueue <ids...>, --resume-queue, and --max-active 4 (default).
- Persists state in refinement_batch_queue.json.
- When a sub-batch requires SQL translation (NEEDS_SURGICAL_LLM), pauses with control
  instructions for the LLM to translate only the SQL of those <=4 cards, validates the
  gate, and automatically advances to the next 4 cards.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

curr_dir = Path(__file__).resolve().parent
if str(curr_dir) not in sys.path:
    sys.path.insert(0, str(curr_dir))

from stage1_resolve_refinement_context import ContextLockResolver
from stage2_scaffold_dataform_and_review import DataformScaffolder
from stage3_validate_refinement_gate import RefinementGateValidator
from stage4_build_and_publish_pbi import SpecPublisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("refiner.orchestrator")

QUEUE_FILE_DEFAULT = "refinement_batch_queue.json"


class RefineCardOrchestrator:
    """Manages chained micro-batch queue execution for high-throughput refinement."""

    def __init__(self, queue_file: str = QUEUE_FILE_DEFAULT, max_active: int = 4):
        self.queue_file = Path(queue_file)
        self.max_active = max_active

    def load_queue(self) -> Dict[str, Any]:
        if self.queue_file.exists():
            try:
                with open(self.queue_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading queue file, initializing new: {e}")
        return {
            "pending": [],
            "in_progress": [],
            "completed": [],
            "failed": [],
            "awaiting_surgical_llm": [],
            "max_active": self.max_active,
            "updated_at": time.time()
        }

    def save_queue(self, queue: Dict[str, Any]) -> None:
        queue["updated_at"] = time.time()
        with open(self.queue_file, "w", encoding="utf-8") as f:
            json.dump(queue, f, indent=2)

    def enqueue(self, card_ids: List[str]) -> Dict[str, Any]:
        q = self.load_queue()
        for cid in card_ids:
            if cid not in q["pending"] and cid not in q["completed"] and cid not in q["in_progress"]:
                q["pending"].append(cid)
        self.save_queue(q)
        logger.info(f"Enqueued {len(card_ids)} cards. Total pending: {len(q['pending'])}")
        return q

    def process_next_batch(
        self,
        catalog_path: str,
        artifacts_root: str = "./refinement_artifacts",
        dry_run: bool = False
    ) -> Dict[str, Any]:
        q = self.load_queue()

        # If cards are awaiting surgical LLM, check if they were completed or resume
        if q["awaiting_surgical_llm"]:
            logger.info(f"Sub-batch {q['awaiting_surgical_llm']} is awaiting surgical LLM translation or gate validation.")
            # Verify gate for awaiting cards
            cleared = []
            for cid in list(q["awaiting_surgical_llm"]):
                card_dir = Path(artifacts_root) / cid
                locked_ctx_p = card_dir / "context_locked.json"
                if locked_ctx_p.exists():
                    gate = RefinementGateValidator(dry_run=dry_run)
                    res = gate.validate(str(locked_ctx_p), str(card_dir))
                    if res.passed:
                        cleared.append(cid)
                        q["completed"].append(cid)
                        q["awaiting_surgical_llm"].remove(cid)
                        logger.info(f"Card {cid} passed Refinement Gate! Marked completed.")
            if cleared:
                self.save_queue(q)
            if q["awaiting_surgical_llm"]:
                return {
                    "status": "PAUSED_AWAITING_SURGICAL_LLM",
                    "cards_awaiting": q["awaiting_surgical_llm"],
                    "message": f"Paused: LLM must translate SELECT body for cards {q['awaiting_surgical_llm']}. Then resume queue."
                }

        # Take next micro-batch of up to max_active
        batch_ids = q["pending"][:self.max_active]
        if not batch_ids:
            logger.info("Refinement queue is empty! All items completed.")
            return {"status": "QUEUE_EMPTY", "completed": q["completed"]}

        q["pending"] = q["pending"][len(batch_ids):]
        q["in_progress"] = batch_ids
        self.save_queue(q)

        logger.info(f"Processing micro-batch of {len(batch_ids)} card(s): {batch_ids}")
        resolver = ContextLockResolver()
        scaffolder = DataformScaffolder()

        needs_llm_cards = []

        for cid in batch_ids:
            card_dir = Path(artifacts_root) / cid
            card_dir.mkdir(parents=True, exist_ok=True)
            tbl_name = f"table_{cid.lower().replace('-', '_')}"

            # Stage 1: Resolve context
            locked = resolver.resolve_and_lock(
                ticket_id=cid,
                target_table_name=tbl_name,
                canonical_catalog_path=catalog_path,
                legacy_sql_or_code=f"-- Legacy code for {cid}\nSELECT id, current_timestamp() FROM raw_{tbl_name}",
                output_dir=str(card_dir)
            )

            # Stage 2: Scaffold
            scaffold_res = scaffolder.scaffold(
                locked_context=locked,
                output_dir=str(card_dir)
            )

            if scaffold_res.get("needs_surgical_llm"):
                needs_llm_cards.append({
                    "card_id": cid,
                    "prompt": scaffold_res.get("surgical_prompt_for_llm")
                })

        q["in_progress"] = []
        if needs_llm_cards:
            q["awaiting_surgical_llm"] = [c["card_id"] for c in needs_llm_cards]
            self.save_queue(q)
            logger.info(f"Sub-batch requires surgical LLM for {len(needs_llm_cards)} cards. Pausing execution.")
            return {
                "status": "NEEDS_SURGICAL_LLM",
                "cards_requiring_llm": [c["card_id"] for c in needs_llm_cards],
                "surgical_prompts": needs_llm_cards,
                "instructions": "Translate only the SELECT query body for these <=4 cards. Then run with --resume-queue."
            }
        else:
            q["completed"].extend(batch_ids)
            self.save_queue(q)
            return {
                "status": "BATCH_PROCESSED_DETERMINISTICALLY",
                "completed_cards": batch_ids,
                "remaining_pending": len(q["pending"])
            }


def main() -> None:
    parser = argparse.ArgumentParser(description="Chained Batch Queue Refinement Orchestrator (.py-First)")
    parser.add_argument("--enqueue", nargs="+", help="Card IDs to enqueue (e.g. --enqueue DATA-101 DATA-102)")
    parser.add_argument("--resume-queue", action="store_true", help="Resume processing current queue")
    parser.add_argument("--max-active", type=int, default=4, help="Maximum concurrent cards per sub-batch (default: 4)")
    parser.add_argument("--catalog", type=str, default="skills/migration-spec-refiner/examples/canonical_catalog.json", help="Path to SSOT catalog")
    parser.add_argument("--queue-file", type=str, default=QUEUE_FILE_DEFAULT, help="Queue persistence JSON file")
    parser.add_argument("--artifacts-root", type=str, default="./refinement_artifacts", help="Root directory for generated artifacts")
    parser.add_argument("--dry-run", action="store_true", help="Run without live cloud checks")

    args = parser.parse_args()

    orchestrator = RefineCardOrchestrator(queue_file=args.queue_file, max_active=args.max_active)

    if args.enqueue:
        orchestrator.enqueue(args.enqueue)

    if args.enqueue or args.resume_queue:
        result = orchestrator.process_next_batch(
            catalog_path=args.catalog,
            artifacts_root=args.artifacts_root,
            dry_run=args.dry_run
        )
        print("\n" + json.dumps(result, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
