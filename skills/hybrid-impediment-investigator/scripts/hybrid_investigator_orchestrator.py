#!/usr/bin/env python3
"""
Hybrid Impediment Investigator Orchestrator.

Combines Stage 1 (Vertex AI Gemini 2.5 Flash Structured Extractor) and Stage 2
(Deterministic Python Probe Runner) into an autonomous investigation pipeline.
Can run locally via CLI or headless in Airflow / Cloud Run Jobs.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

curr_dir = Path(__file__).resolve().parent
if str(curr_dir) not in sys.path:
    sys.path.insert(0, str(curr_dir))

from vertex_gemini_imp_extractor import VertexGeminiImpedimentExtractor
from python_imp_probe_runner import PythonImpedimentProbeRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("hybrid-investigator-orchestrator")


class HybridImpedimentInvestigator:
    """End-to-end investigator orchestrating Vertex AI LLM Extraction + Python Probes."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        region: str = "us-central1",
        dry_run: bool = False,
    ):
        self.extractor = VertexGeminiImpedimentExtractor(
            project_id=project_id,
            region=region,
            dry_run=dry_run,
        )
        self.probe_runner = PythonImpedimentProbeRunner(dry_run=dry_run)
        self.dry_run = dry_run

    def investigate_card(
        self, card_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Runs Stage 1 extraction followed by Stage 2 probes."""
        card_id = card_data.get("id", "UNKNOWN_CARD")
        logger.info(f"--- Starting Hybrid Investigation for Card: {card_id} ---")

        # Stage 1: LLM Structured Extraction
        diagnosis = self.extractor.extract(card_data)

        # Stage 2: Deterministic Python Probes
        probe_summary = self.probe_runner.execute_all_probes(diagnosis)

        # Format 3-block Markdown
        report_md = self.probe_runner.format_markdown_report(card_id, diagnosis, probe_summary)

        return {
            "card_id": card_id,
            "diagnosis": diagnosis,
            "probe_summary": probe_summary,
            "report_md": report_md,
            "verdict": probe_summary.get("verdict"),
            "can_unblock": "READY" in probe_summary.get("verdict", ""),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Hybrid Impediment Investigator Orchestrator")
    parser.add_argument("--history-json", type=str, required=True, help="Input card discussion JSON")
    parser.add_argument("--project-id", type=str, help="GCP project for Vertex AI")
    parser.add_argument("--region", type=str, default="us-central1", help="Vertex AI region")
    parser.add_argument("--output-json", type=str, help="Output consolidated JSON path")
    parser.add_argument("--output-md", type=str, help="Output Markdown report path")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without network calls")

    args = parser.parse_args()

    input_path = Path(args.history_json)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        card_data = json.load(f)

    investigator = HybridImpedimentInvestigator(
        project_id=args.project_id,
        region=args.region,
        dry_run=args.dry_run,
    )

    result = investigator.investigate_card(card_data)

    if args.output_json:
        out_j = Path(args.output_json)
        out_j.parent.mkdir(parents=True, exist_ok=True)
        out_j.write_text(json.dumps(result, indent=2), encoding="utf-8")
        logger.info(f"Consolidated investigation JSON saved to {out_j}")

    if args.output_md:
        out_m = Path(args.output_md)
        out_m.parent.mkdir(parents=True, exist_ok=True)
        out_m.write_text(result["report_md"], encoding="utf-8")
        logger.info(f"Consolidated Markdown report saved to {out_m}")
    else:
        print(result["report_md"])


if __name__ == "__main__":
    main()
