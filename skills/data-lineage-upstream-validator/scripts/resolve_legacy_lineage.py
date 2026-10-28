"""
6-Layer Cascaded Legacy Lineage Resolver.

When an upstream table is physically missing in BigQuery, this resolver automatically discovers
which legacy pipeline / Spark job / ingestion flow is responsible for producing it by cascading through:
  Layer 1: Recursive View Expansion (Hive / Spark SQL / Trino views)
  Layer 2: Legacy Job Orchestration Graph (Airflow DAGs, Oozie, Control-M, Autosys)
  Layer 3: Parameterized Ingestion Templates (generic JDBC/CDC framework configs)
  Layer 4: Target Catalog Exact / Stem Match with Recurring Writer Score
  Layer 5: Static Code Metadata (YAML/JSON pipeline manifests)
  Layer 6: AST & Regex Write Command Scanner (.saveAsTable, INSERT INTO, MERGE INTO, CREATE TABLE)
"""

from __future__ import annotations

import logging
import re
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple

logger = logging.getLogger("lineage-validator.resolver")


@dataclass
class ProducerDiscovery:
    target_table: str
    discovered_producer_job: str
    resolution_layer: str  # 1_VIEW_EXPANSION, 2_JOB_GRAPH, 3_INGESTION_TEMPLATE, 4_CATALOG_MATCH, 5_STATIC_METADATA, 6_AST_REGEX_SCAN
    confidence_score: float  # 0.0 to 1.0
    underlying_source_tables: List[str] = field(default_factory=list)
    evidence_file: Optional[str] = None
    line_number: Optional[int] = None
    details: str = ""


class LegacyLineageResolver:
    """Cascading 6-layer lineage resolver for missing upstream tables."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def resolve(
        self,
        missing_table: str,
        legacy_context: Dict[str, Any]
    ) -> Optional[ProducerDiscovery]:
        table_name = missing_table.split(".")[-1].lower()

        # -------------------------------------------------------------
        # LAYER 1: Recursive View Expansion
        # -------------------------------------------------------------
        view_match = self._layer1_recursive_views(table_name, legacy_context.get("views", {}))
        if view_match:
            return view_match

        # -------------------------------------------------------------
        # LAYER 2: Legacy Job Orchestration Dependency Graph
        # -------------------------------------------------------------
        job_graph_match = self._layer2_job_graph(table_name, legacy_context.get("job_graph", {}))
        if job_graph_match:
            return job_graph_match

        # -------------------------------------------------------------
        # LAYER 3: Parameterized Ingestion Templates
        # -------------------------------------------------------------
        template_match = self._layer3_ingestion_templates(table_name, legacy_context.get("ingestion_templates", []))
        if template_match:
            return template_match

        # -------------------------------------------------------------
        # LAYER 4: Target Catalog Match with Recurring Writer Score
        # -------------------------------------------------------------
        catalog_match = self._layer4_catalog_match(table_name, legacy_context.get("target_catalog", []))
        if catalog_match:
            return catalog_match

        # -------------------------------------------------------------
        # LAYER 5: Static Code Metadata
        # -------------------------------------------------------------
        static_match = self._layer5_static_metadata(table_name, legacy_context.get("static_manifests", []))
        if static_match:
            return static_match

        # -------------------------------------------------------------
        # LAYER 6: AST & Regex Write Command Scanner
        # -------------------------------------------------------------
        ast_match = self._layer6_ast_regex_scan(table_name, legacy_context.get("code_repo_path"))
        if ast_match:
            return ast_match

        return None

    def _layer1_recursive_views(self, table_name: str, views_dict: Dict[str, str]) -> Optional[ProducerDiscovery]:
        if table_name in views_dict:
            sql = views_dict[table_name]
            from_matches = re.findall(r"FROM\s+([a-zA-Z0-9_\-\.]+)", sql, re.IGNORECASE)
            join_matches = re.findall(r"JOIN\s+([a-zA-Z0-9_\-\.]+)", sql, re.IGNORECASE)
            underlying = list(set(from_matches + join_matches))
            return ProducerDiscovery(
                target_table=table_name,
                discovered_producer_job=f"VIEW_EXPANSION:{table_name}",
                resolution_layer="1_VIEW_EXPANSION",
                confidence_score=0.98,
                underlying_source_tables=underlying,
                details=f"Table '{table_name}' is a legacy View resolving to underlying tables: {', '.join(underlying)}."
            )
        return None

    def _layer2_job_graph(self, table_name: str, job_graph: Dict[str, Any]) -> Optional[ProducerDiscovery]:
        # job_graph schema: { "dag_id / job_id": { "outputs": ["table_a", "table_b"], "inputs": [...] } }
        for job_id, meta in job_graph.items():
            outputs = [o.lower().split(".")[-1] for o in meta.get("outputs", [])]
            if table_name in outputs:
                return ProducerDiscovery(
                    target_table=table_name,
                    discovered_producer_job=job_id,
                    resolution_layer="2_JOB_GRAPH",
                    confidence_score=0.95,
                    underlying_source_tables=meta.get("inputs", []),
                    details=f"Discovered in orchestration DAG/Job '{job_id}' with declared output '{table_name}'."
                )
        return None

    def _layer3_ingestion_templates(self, table_name: str, templates: List[Dict[str, Any]]) -> Optional[ProducerDiscovery]:
        # Ingestion frameworks often map source RDBMS tables to target landing/bronze via templates
        for tpl in templates:
            table_pattern = tpl.get("table_pattern", "")
            framework_job = tpl.get("framework_job_name", "generic-rdbms-ingestor")
            if table_pattern and (table_pattern == table_name or re.search(table_pattern, table_name, re.IGNORECASE)):
                return ProducerDiscovery(
                    target_table=table_name,
                    discovered_producer_job=framework_job,
                    resolution_layer="3_INGESTION_TEMPLATE",
                    confidence_score=0.90,
                    underlying_source_tables=[f"jdbc_source.{tpl.get('source_rdbms_table', table_name)}"],
                    details=f"Discovered via parameterized ingestion template '{tpl.get('template_id', 'standard_cdc')}' mapped to '{framework_job}'."
                )
        return None

    def _layer4_catalog_match(self, table_name: str, catalog: List[Dict[str, Any]]) -> Optional[ProducerDiscovery]:
        # Stem and exact matches with recurring writer score
        best_candidate = None
        highest_score = 0.0

        for entry in catalog:
            cat_table = entry.get("table_name", "").lower()
            writer_job = entry.get("writer_job", "")
            frequency = entry.get("execution_frequency", "daily")
            write_count = entry.get("recurring_write_count", 1)

            # Score computation
            score = 0.0
            if cat_table == table_name:
                score = 0.85 + min(write_count * 0.02, 0.10)
            elif cat_table.replace("_v1", "").replace("_v2", "") == table_name:
                score = 0.75

            if score > highest_score and score > 0.70:
                highest_score = score
                best_candidate = ProducerDiscovery(
                    target_table=table_name,
                    discovered_producer_job=writer_job,
                    resolution_layer="4_CATALOG_MATCH",
                    confidence_score=round(score, 2),
                    underlying_source_tables=entry.get("inputs", []),
                    details=f"Target catalog match in writer '{writer_job}' (recurring write count: {write_count}, freq: {frequency})."
                )

        return best_candidate

    def _layer5_static_metadata(self, table_name: str, manifests: List[Dict[str, Any]]) -> Optional[ProducerDiscovery]:
        for m in manifests:
            outputs = [o.lower() for o in m.get("target_tables", [])]
            if table_name in outputs:
                return ProducerDiscovery(
                    target_table=table_name,
                    discovered_producer_job=m.get("pipeline_id", "unnamed_pipeline"),
                    resolution_layer="5_STATIC_METADATA",
                    confidence_score=0.80,
                    evidence_file=m.get("manifest_path"),
                    details=f"Identified in static pipeline manifest '{m.get('pipeline_id')}'."
                )
        return None

    def _layer6_ast_regex_scan(self, table_name: str, repo_path: Optional[str]) -> Optional[ProducerDiscovery]:
        if not repo_path:
            return None

        p = Path(repo_path)
        if not p.exists():
            return None

        # Regex write patterns in Spark/SQL files
        patterns = [
            (re.compile(r"saveAsTable\([\"'](?:[a-zA-Z0-9_\-]+\.)?" + re.escape(table_name) + r"[\"']\)", re.IGNORECASE), 0.75),
            (re.compile(r"insertInto\([\"'](?:[a-zA-Z0-9_\-]+\.)?" + re.escape(table_name) + r"[\"']\)", re.IGNORECASE), 0.75),
            (re.compile(r"INSERT\s+INTO\s+(?:TABLE\s+)?(?:[a-zA-Z0-9_\-]+\.)?" + re.escape(table_name) + r"\b", re.IGNORECASE), 0.70),
            (re.compile(r"MERGE\s+INTO\s+(?:[a-zA-Z0-9_\-]+\.)?" + re.escape(table_name) + r"\b", re.IGNORECASE), 0.70),
            (re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:[a-zA-Z0-9_\-]+\.)?" + re.escape(table_name) + r"\b", re.IGNORECASE), 0.65),
        ]

        # Scan python/sql files in legacy codebase
        for code_file in p.glob("**/*"):
            if code_file.suffix in [".py", ".sql", ".scala", ".hql"]:
                try:
                    lines = code_file.read_text(encoding="utf-8", errors="ignore").splitlines()
                    for idx, line in enumerate(lines, start=1):
                        for pat, conf in patterns:
                            if pat.search(line):
                                return ProducerDiscovery(
                                    target_table=table_name,
                                    discovered_producer_job=code_file.stem,
                                    resolution_layer="6_AST_REGEX_SCAN",
                                    confidence_score=conf,
                                    evidence_file=str(code_file),
                                    line_number=idx,
                                    details=f"Found code write operation '{pat.pattern}' in file '{code_file.name}' at line {idx}."
                                )
                except Exception as e:
                    logger.debug(f"Scan error in {code_file}: {e}")

        return None
