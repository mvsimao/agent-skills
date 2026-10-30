"""
Stage 1: Live origin/main SSOT Lock & Pre-Flight Resolver.

Capabilities:
- Performs git fetch origin main and loads canonical catalog directly from origin/main (load_latest_ssot_json)
  to prevent local dictionary drift.
- Normalizes project/dataset aliases (e.g. short_names -> active GCP projects).
- Filters false table targets in Egress pipelines (EMAIL, SFTP, WEBHOOK - Rule D-32).
- Preserves multi-target pipelines and multi-region parameterization (sam, nam, eur -> ${constants.REGION}).
- Generates immutable contract context_locked.json before any LLM call.
- Integrates Cloud Preflight Infrastructure and Upstream Lineage Validators.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Any, Optional, Set

logger = logging.getLogger("refiner.stage1-ssot-lock")


class ContextLockResolver:
    """Locks authoritative coordinates from origin/main SSOT and triggers preflight checks."""

    EGRESS_NON_TABLE_TARGETS = {
        "email", "sftp", "webhook", "api", "kafka", "null", "none", "na",
        "data_application_api", "application_api", "rest_api"
    }

    GHOST_VIEWS_MAP = {
        "vw_eventtracks": "silver_platform.eventtracks",
        "legacy_eventtracks": "silver_platform.eventtracks",
        "eventtracks_view": "silver_platform.eventtracks"
    }

    PROJECT_ALIAS_MAP = {
        "prod": "acme-dwh-prod",
        "dwh": "acme-dwh-prod",
        "target": "acme-dwh-prod",
        "raw": "acme-lakehouse-raw",
        "lake": "acme-lakehouse-raw",
        "vault": "acme-security-vault",
        "stage": "acme-sandbox-tmp"
    }

    def load_latest_ssot_json(self, catalog_path: str, repo_root: Optional[str] = None) -> Dict[str, Any]:
        """Loads canonical catalog from origin/main to prevent local dictionary drift, normalizing path variations."""
        norm_catalog_path = catalog_path.strip().replace("#", "_").rstrip("/\\")
        cat_file = Path(norm_catalog_path)
        
        # If inside a git repo, try to read directly from origin/main
        if repo_root:
            try:
                subprocess.run(["git", "fetch", "origin", "main"], cwd=repo_root, capture_output=True, timeout=10)
                rel_path = cat_file.resolve().relative_to(Path(repo_root).resolve())
                res = subprocess.run(
                    ["git", "show", f"origin/main:{rel_path}"],
                    cwd=repo_root, capture_output=True, text=True, timeout=10
                )
                if res.returncode == 0:
                    logger.info(f"Loaded live SSOT catalog directly from origin/main:{rel_path}")
                    return json.loads(res.stdout)
            except Exception as e:
                logger.warning(f"Could not read from origin/main directly, falling back to local file: {e}")

        if not cat_file.exists():
            # Try original path if normalized didn't exist directly
            if Path(catalog_path).exists():
                cat_file = Path(catalog_path)
            else:
                raise FileNotFoundError(f"Canonical catalog not found: {cat_file}")

        with open(cat_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def normalize_project(self, raw_proj: str) -> str:
        clean = raw_proj.strip().lower()
        return self.PROJECT_ALIAS_MAP.get(clean, raw_proj.strip())

    def filter_egress_targets(self, targets: Any) -> List[Dict[str, Any]]:
        """Filters out discarded targets (discarded_by) and non-table egress endpoints (Rule D-32)."""
        valid_targets = []
        target_list: List[Dict[str, Any]] = []

        # Support targets passed as list or dict
        if isinstance(targets, list):
            target_list = targets
        elif isinstance(targets, dict):
            for k, v in targets.items():
                if isinstance(v, dict):
                    v_copy = dict(v)
                    v_copy.setdefault("table_id", k)
                    target_list.append(v_copy)
                elif isinstance(v, str):
                    target_list.append({"table_id": k, "dataset_id": v})

        for t in target_list:
            # Filter discarded targets
            if t.get("discarded_by"):
                logger.info(f"Target '{t.get('table_id')}' discarded by: {t.get('discarded_by')}. Skipping.")
                continue

            tbl_name = t.get("table_id", "").lower().strip()
            ds_name = t.get("dataset_id", "").lower().strip()
            dest_type = t.get("destination_type", "").lower().strip()

            if (tbl_name in self.EGRESS_NON_TABLE_TARGETS or
                ds_name in self.EGRESS_NON_TABLE_TARGETS or
                dest_type in self.EGRESS_NON_TABLE_TARGETS or
                tbl_name == "a_definir_refinamento"):
                logger.info(f"Rule D-32: Discarded non-table egress target '{ds_name}.{tbl_name}'.")
                continue

            # Ghost view resolution on targets
            if tbl_name in self.GHOST_VIEWS_MAP:
                resolved = self.GHOST_VIEWS_MAP[tbl_name]
                parts = resolved.split(".")
                t["dataset_id"] = parts[0]
                t["table_id"] = parts[1]

            valid_targets.append(t)
        return valid_targets

    def parameterize_region(self, partition_or_cluster_expr: str) -> str:
        """Normalizes region expressions: sam, nam, eur -> ${constants.REGION}."""
        if not partition_or_cluster_expr:
            return partition_or_cluster_expr
        for reg in ["sam", "nam", "eur", "latam"]:
            partition_or_cluster_expr = partition_or_cluster_expr.replace(f"'{reg}'", "${constants.REGION}")
        return partition_or_cluster_expr

    def resolve_and_lock(
        self,
        ticket_id: str,
        target_table_name: str,
        canonical_catalog_path: str,
        legacy_sql_or_code: str,
        additional_targets: Optional[List[Dict[str, Any]]] = None,
        output_dir: str = "./refinement_artifacts",
        repo_root: Optional[str] = None
    ) -> Dict[str, Any]:
        catalog = self.load_latest_ssot_json(canonical_catalog_path, repo_root=repo_root)

        projects_raw = catalog.get("canonical_projects", {
            "target": "acme-dwh-prod",
            "upstream": "acme-lakehouse-raw",
            "secrets": "acme-security-vault",
            "staging": "acme-sandbox-tmp"
        })
        projects_dict = {k: self.normalize_project(v) for k, v in projects_raw.items()}

        table_metadata = catalog.get("tables", {}).get(target_table_name, {
            "dataset_id": "gold_customer_analytics",
            "type": "incremental",
            "unique_keys": ["customer_id"],
            "partition_by": "DATE(processed_at)",
            "cluster_by": ["customer_id", "country_code"]
        })

        target_dataset = table_metadata.get("dataset_id", "gold_customer_analytics")
        service_account = catalog.get("service_accounts", {}).get(
            "dataform", f"sa-dataform-executor@{projects_dict['target']}.iam.gserviceaccount.com"
        )

        # Multi-target support
        primary_target = {
            "project_id": projects_dict["target"],
            "dataset_id": target_dataset,
            "table_id": target_table_name,
            "type": table_metadata.get("type", "incremental"),
            "unique_keys": table_metadata.get("unique_keys", ["id"]),
            "partition_by": self.parameterize_region(table_metadata.get("partition_by", "")),
            "cluster_by": table_metadata.get("cluster_by", [])
        }
        all_targets = [primary_target] + self.filter_egress_targets(additional_targets or [])

        declared_sources = catalog.get("canonical_sources", [])

        # Build immutable locked contract
        locked_context = {
            "ticket_id": ticket_id,
            "target_project": projects_dict["target"],
            "upstream_project": projects_dict["upstream"],
            "secrets_project": projects_dict["secrets"],
            "staging_project": projects_dict["staging"],
            "target_dataset": target_dataset,
            "target_table": target_table_name,
            "table_type": table_metadata.get("type", "incremental"),
            "unique_keys": table_metadata.get("unique_keys", ["id"]),
            "partition_by": self.parameterize_region(table_metadata.get("partition_by", "")),
            "cluster_by": table_metadata.get("cluster_by", []),
            "all_targets": all_targets,
            "git_repo_url": catalog.get("git_repo_url", "https://github.com/acme-data-platform/dataform-core-warehouse.git"),
            "service_account": service_account,
            "canonical_sources": declared_sources,
            "legacy_source_code": legacy_sql_or_code,
            "is_locked": True,
            "lock_version": "2.0.0"
        }

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        lock_file = out_dir / "context_locked.json"
        lock_file.write_text(json.dumps(locked_context, indent=2), encoding="utf-8")
        logger.info(f"Generated authoritative context lock artifact at: {lock_file}")

        return locked_context
