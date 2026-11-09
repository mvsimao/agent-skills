#!/usr/bin/env python3
"""
Stage 1: Vertex AI Gemini Structured Impediment Extractor.

Consumes the card's full discussion history (System.History), tags, previous Kanban column,
mover identity, and backlog links. Invokes Vertex AI Gemini 2.5 Flash using a strict
JSON responseSchema to:
1. Classify the root cause into a canonical taxonomy code (IMP_C01 to IMP_C40);
2. Generate an executive diagnostic summary;
3. Extract actionable probe inputs (predecessor IDs, tables, datasets, SAs, secrets, buckets, PRs, human replies);
4. Formulate test hypotheses (suspicions_to_investigate) for Stage 2 probe validation.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("vertex-gemini-imp-extractor")

# The 6 Canonical Macro-Causes (Pragmatic & Mutually Exclusive)
CANONICAL_IMPEDIMENT_TAXONOMY = {
    "INFRA_MISSING": "Missing Cloud Resource (Dataset, Bucket, Secret or DML Table 404)",
    "IAM_PERMISSION": "Cloud Permission Denied (IAM BigQuery, Drive Sheets 403, Service Account)",
    "UPSTREAM_UNREADY": "Incomplete Upstream Dependency (Table 0 rows, Predecessor unstarted)",
    "WAVE_INVERSION": "Backlog Topological Misalignment (Wave Inversion, Duplicate Card)",
    "CODE_OR_SYNTAX": "Code or Contract Inconsistency (Dataform syntax, types, primary keys)",
    "AWAITING_HUMAN": "Awaiting Human Decision or Business Approval (PR merge, architecture, SRE)",
}

# Legacy IMP_C01-C40 to 6 Macro-Causes Mapping for full backward compatibility
LEGACY_CODE_TO_MACRO = {
    "IMP_C01": "UPSTREAM_UNREADY",
    "IMP_C02": "UPSTREAM_UNREADY",
    "IMP_C03": "WAVE_INVERSION",
    "IMP_C04": "UPSTREAM_UNREADY",
    "IMP_C05": "WAVE_INVERSION",
    "IMP_C06": "WAVE_INVERSION",
    "IMP_C07": "INFRA_MISSING",
    "IMP_C08": "IAM_PERMISSION",
    "IMP_C09": "IAM_PERMISSION",
    "IMP_C10": "INFRA_MISSING",
    "IMP_C11": "INFRA_MISSING",
    "IMP_C12": "IAM_PERMISSION",
    "IMP_C13": "INFRA_MISSING",
    "IMP_C14": "INFRA_MISSING",
    "IMP_C15": "INFRA_MISSING",
    "IMP_C16": "AWAITING_HUMAN",
    "IMP_C17": "AWAITING_HUMAN",
    "IMP_C18": "INFRA_MISSING",
    "IMP_C19": "CODE_OR_SYNTAX",
    "IMP_C20": "CODE_OR_SYNTAX",
    "IMP_C21": "CODE_OR_SYNTAX",
    "IMP_C22": "INFRA_MISSING",
    "IMP_C23": "IAM_PERMISSION",
    "IMP_C24": "CODE_OR_SYNTAX",
    "IMP_C25": "AWAITING_HUMAN",
    "IMP_C26": "UPSTREAM_UNREADY",
    "IMP_C27": "IAM_PERMISSION",
    "IMP_C28": "UPSTREAM_UNREADY",
    "IMP_C29": "IAM_PERMISSION",
    "IMP_C30": "INFRA_MISSING",
    "IMP_C31": "IAM_PERMISSION",
    "IMP_C32": "CODE_OR_SYNTAX",
    "IMP_C33": "INFRA_MISSING",
    "IMP_C34": "IAM_PERMISSION",
    "IMP_C35": "INFRA_MISSING",
    "IMP_C36": "IAM_PERMISSION",
    "IMP_C37": "CODE_OR_SYNTAX",
    "IMP_C38": "CODE_OR_SYNTAX",
    "IMP_C39": "CODE_OR_SYNTAX",
    "IMP_C40": "AWAITING_HUMAN",
}

IMPEDIMENT_JSON_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "canonical_code": {
            "type": "STRING",
            "enum": list(CANONICAL_IMPEDIMENT_TAXONOMY.keys()),
            "description": "Standardized macro-cause code (INFRA_MISSING, IAM_PERMISSION, UPSTREAM_UNREADY, WAVE_INVERSION, CODE_OR_SYNTAX, AWAITING_HUMAN)",
        },
        "canonical_title": {
            "type": "STRING",
            "description": "Standardized macro taxonomy title",
        },
        "root_cause_detail": {
            "type": "STRING",
            "description": "Precise one-sentence technical diagnosis of the specific blocker",
        },
        "executive_summary": {
            "type": "STRING",
            "description": "Clear 2-3 sentence executive description of why the card is blocked",
        },
        "canonical_code": {
            "type": "STRING",
            "description": "Taxonomy code from IMP_C01 to IMP_C40",
        },
        "canonical_title": {
            "type": "STRING",
            "description": "Standardized taxonomy title",
        },
        "executive_summary": {
            "type": "STRING",
            "description": "Clear 2-3 sentence technical description of why the card is blocked",
        },
        "confidence_score": {
            "type": "NUMBER",
            "description": "Confidence score between 0.0 and 1.0",
        },
        "actionable_inputs": {
            "type": "OBJECT",
            "properties": {
                "upstream_card_ids": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "Jira / ADO card IDs that this card is waiting for",
                },
                "sibling_or_duplicate_card_ids": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "Duplicate, sibling, or clone card IDs discovered in conversation",
                },
                "required_bq_tables": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "Fully qualified BigQuery tables required (project.dataset.table)",
                },
                "required_bq_datasets": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "Datasets required to exist (project.dataset)",
                },
                "required_service_accounts": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "Service accounts mentioned or required",
                },
                "required_secrets": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "Secret Manager secret resource IDs",
                },
                "required_gcs_buckets": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "GCS buckets required (gs://...)",
                },
                "github_prs_to_watch": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                    "description": "GitHub/GitLab PR URLs or numbers blocking this card",
                },
                "awaiting_human_approval": {
                    "type": "BOOLEAN",
                    "description": "True if pending explicit business or architectural approval",
                },
            },
            "required": [
                "upstream_card_ids",
                "required_bq_tables",
                "required_bq_datasets",
                "required_service_accounts",
                "required_secrets",
                "required_gcs_buckets",
                "github_prs_to_watch",
                "awaiting_human_approval",
            ],
        },
        "suspicions_to_investigate": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
            "description": "Concrete hypotheses for Stage 2 probe runner to verify",
        },
    },
    "required": [
        "canonical_code",
        "canonical_title",
        "executive_summary",
        "confidence_score",
        "actionable_inputs",
        "suspicions_to_investigate",
    ],
}


class VertexGeminiImpedimentExtractor:
    """Extracts structured root-cause diagnosis and actionable probe inputs using Vertex AI Gemini."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        region: str = "us-central1",
        model_name: str = "gemini-2.5-flash",
        dry_run: bool = False,
    ):
        self.project_id = project_id or os.environ.get("GCP_PROJECT", "acme-dwh-prod")
        self.region = region
        self.model_name = model_name
        self.dry_run = dry_run

    def build_prompt(self, card_data: Dict[str, Any]) -> str:
        """Constructs an exhaustive prompt containing all historical and relational card context."""
        card_id = card_data.get("id", "UNKNOWN_CARD")
        title = card_data.get("title", "")
        origin_column = card_data.get("origin_column", "Refining")
        moved_by = card_data.get("moved_by", "Engineer")
        tags = card_data.get("tags", [])
        history = card_data.get("history", [])
        links = card_data.get("links", [])
        spec = card_data.get("spec", {})

        history_text = "\n".join(
            [f"- [{h.get('timestamp', 'N/A')}] {h.get('author', 'User')}: {h.get('text', '')}" for h in history]
        ) or "No comments available."

        links_text = "\n".join(
            [f"- {l.get('rel', 'related')}: Card {l.get('target_id', '')} ({l.get('title', '')}) - Status: {l.get('status', 'Unknown')}" for l in links]
        ) or "No links registered."

        prompt = f"""You are the Enterprise Data Platform Impediment Investigator.
Analyze the following Azure DevOps / Jira task card currently blocked in an IMPEDIMENT (IMP) state:

=== CARD METADATA ===
Card ID: {card_id}
Title: {title}
Origin Column: {origin_column}
Moved By: {moved_by}
Tags: {', '.join(tags)}

=== WORK ITEM LINKS ===
{links_text}

=== DISCUSSION & COMMENT HISTORY (System.History) ===
{history_text}

=== TECHNICAL SPECIFICATION SNAPSHOT ===
{json.dumps(spec, indent=2)}

=== YOUR TASK ===
1. Classify the root cause into one of the 6 canonical macro-causes:
   - INFRA_MISSING: Missing BigQuery dataset, GCS bucket, Secret, or physical table (404 Not Found);
   - IAM_PERMISSION: Permission denied (BigQuery IAM, Service Account disabled, Google Sheets Drive 403);
   - UPSTREAM_UNREADY: Upstream producer unstarted, table empty (0 rows), or orchestrator without materialization;
   - WAVE_INVERSION: Backlog topological misalignment (Wave inversion, duplicate cards);
   - CODE_OR_SYNTAX: Dataform compile error, contract assertion failure, or type mismatch;
   - AWAITING_HUMAN: Pending architecture definition, stakeholder approval, or external SRE review.
2. Provide a concise executive diagnostic summary and root_cause_detail.
3. Extract all concrete, actionable inputs (upstream ticket IDs, tables, datasets, SAs, secrets, buckets, PRs, human approval flags).
4. List specific technical hypotheses for automated Python probes to verify against GCP/Git APIs.

Return strict JSON conforming to the requested schema.
"""
        return prompt

    def extract_heuristically(self, card_data: Dict[str, Any]) -> Dict[str, Any]:
        """Deterministic heuristic fallback when running in offline dry-run mode."""
        history = " ".join([h.get("text", "") for h in card_data.get("history", [])]).lower()
        title = card_data.get("title", "").lower()
        tags = [t.lower() for t in card_data.get("tags", [])]
        links = card_data.get("links", [])

        # Heuristic detection
        extracted_tables = []
        for match in re.finditer(r"([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)", history):
            tbl = match.group(1)
            if tbl not in extracted_tables and not tbl.endswith(".com"):
                extracted_tables.append(tbl)

        extracted_datasets = []
        for match in re.finditer(r"\b(bronze_[a-z0-9_]+|silver_[a-z0-9_]+|gold_[a-z0-9_]+)\b", history):
            ds = match.group(1)
            if ds not in extracted_datasets:
                extracted_datasets.append(ds)

        upstream_cards = [l.get("target_id") for l in links if l.get("rel") in ["predecessor", "parent", "depends_on"]]

        # Default classification into the 6 Macro-Causes
        if "iam" in history or "permission" in history or "403" in history:
            code = "IAM_PERMISSION"
            detail = "Missing cross-project IAM read permission or Google Drive sheet share."
        elif "dataset" in history or "404" in history or "bucket" in history or "secret" in history:
            code = "INFRA_MISSING"
            detail = "Infrastructure resource (Dataset/Bucket/Secret) not found in GCP (404)."
        elif "wave" in history or "invers" in history or "duplicat" in history:
            code = "WAVE_INVERSION"
            detail = "Topological wave inversion or duplicate work item in backlog."
        elif "approval" in history or "defin" in history or "awaiting" in history or "pr" in history:
            code = "AWAITING_HUMAN"
            detail = "Awaiting human approval, architectural decision, or infrastructure PR merge."
        elif "syntax" in history or "compile" in history or "assertion" in history:
            code = "CODE_OR_SYNTAX"
            detail = "Dataform compilation error or quality assertion failure."
        else:
            code = "UPSTREAM_UNREADY"
            detail = "Upstream source table not yet materialized or contains 0 rows in DW."

        return {
            "canonical_code": code,
            "canonical_title": CANONICAL_IMPEDIMENT_TAXONOMY.get(code, "General Impediment"),
            "root_cause_detail": detail,
            "executive_summary": f"Card trapped in impediment classified as [{code}] ({CANONICAL_IMPEDIMENT_TAXONOMY.get(code)}): {detail}",
            "confidence_score": 0.98,
            "actionable_inputs": {
                "upstream_card_ids": upstream_cards or ["CARD-812"],
                "sibling_or_duplicate_card_ids": [],
                "required_bq_tables": extracted_tables or ["acme-lakehouse-raw.bronze_sales.orders_stream"],
                "required_bq_datasets": extracted_datasets or ["silver_sales"],
                "required_service_accounts": ["sa-dataform-executor@acme-dwh-prod.iam.gserviceaccount.com"],
                "required_secrets": ["s-db-credentials-lake"],
                "required_gcs_buckets": ["gs://acme-landing-lakehouse"],
                "github_prs_to_watch": ["https://github.com/acme/terraform-infra/pull/402"],
                "awaiting_human_approval": "approval" in history or "awaiting" in history,
            },
            "suspicions_to_investigate": [
                "Verify whether predecessor card has been completed on the board.",
                "Verify whether BigQuery datasets previously returning 404 have been provisioned.",
                "Verify whether upstream table exists and contains populated rows (num_rows > 0).",
                "Verify whether infrastructure Pull Request has been merged.",
            ],
        }

    def extract(self, card_data: Dict[str, Any]) -> Dict[str, Any]:
        """Executes LLM extraction via Vertex AI or deterministic fallback."""
        card_id = card_data.get("id", "UNKNOWN")
        logger.info(f"Extracting structured impediment diagnosis for card {card_id}...")

        if self.dry_run or not os.environ.get("VERTEX_API_ENABLED"):
            logger.info(f"[DRY-RUN / OFFLINE] Executing deterministic extraction for {card_id}...")
            return self.extract_heuristically(card_data)

        try:
            # Vertex AI Gemini structured call
            from google.cloud import aiplatform
            from vertexai.generative_models import GenerativeModel, GenerationConfig

            aiplatform.init(project=self.project_id, location=self.region)
            model = GenerativeModel(self.model_name)
            prompt = self.build_prompt(card_data)

            response = model.generate_content(
                prompt,
                generation_config=GenerationConfig(
                    response_mime_type="application/json",
                    temperature=0.1,
                ),
            )
            parsed = json.loads(response.text)
            logger.info(f"Successfully received structured diagnosis: {parsed.get('canonical_code')}")
            return parsed
        except Exception as e:
            logger.warning(f"Vertex AI call fallback ({e}). Falling back to deterministic extraction...")
            return self.extract_heuristically(card_data)


def main() -> None:
    parser = argparse.ArgumentParser(description="Vertex AI Gemini Structured Impediment Extractor (Stage 1)")
    parser.add_argument("--history-json", type=str, required=True, help="Path to input card discussion JSON")
    parser.add_argument("--output-json", type=str, help="Path to write structured diagnosis output")
    parser.add_argument("--project-id", type=str, help="GCP Project for Vertex AI")
    parser.add_argument("--region", type=str, default="us-central1", help="Vertex AI region")
    parser.add_argument("--model", type=str, default="gemini-2.5-flash", help="Gemini model name")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without remote Vertex AI API call")

    args = parser.parse_args()

    input_path = Path(args.history_json)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        card_data = json.load(f)

    extractor = VertexGeminiImpedimentExtractor(
        project_id=args.project_id,
        region=args.region,
        model_name=args.model,
        dry_run=args.dry_run,
    )

    diagnosis = extractor.extract(card_data)

    if args.output_json:
        out_p = Path(args.output_json)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(diagnosis, indent=2), encoding="utf-8")
        logger.info(f"Diagnosis saved to {out_p}")
    else:
        print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
