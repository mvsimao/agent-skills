"""
Source vs Target Table Extractor.

Separates upstream read dependencies from target write tables without false positives.
Supports:
- Pure external ingestion pipelines (JDBC/APIs/Kafka) with 0 upstream DW tables.
- Standard SQL / PySpark transformation pipelines.
- DML and temporary view filtering.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Set, Dict, Optional, Any


@dataclass
class LineageExtractionResult:
    pipeline_type: str  # EXTERNAL_INGESTION, DW_TRANSFORMATION, HYBRID
    upstream_sources: List[Dict[str, str]] = field(default_factory=list)
    target_tables: List[Dict[str, str]] = field(default_factory=list)
    external_sources: List[Dict[str, Any]] = field(default_factory=list)
    intermediate_ctes: List[str] = field(default_factory=list)


class SourceTargetExtractor:
    """Robust extractor separating read sources from write targets."""

    READ_PATTERNS = [
        re.compile(r"FROM\s+[`\"']?([a-zA-Z0-9_\-\.]+)[`\"']?", re.IGNORECASE),
        re.compile(r"JOIN\s+[`\"']?([a-zA-Z0-9_\-\.]+)[`\"']?", re.IGNORECASE),
        re.compile(r"spark\.read\.table\([\"']([a-zA-Z0-9_\-\.]+)[\"']\)", re.IGNORECASE),
        re.compile(r"spark\.table\([\"']([a-zA-Z0-9_\-\.]+)[\"']\)", re.IGNORECASE),
    ]

    WRITE_PATTERNS = [
        re.compile(r"INSERT\s+INTO\s+(?:TABLE\s+)?[`\"']?([a-zA-Z0-9_\-\.]+)[`\"']?", re.IGNORECASE),
        re.compile(r"MERGE\s+INTO\s+[`\"']?([a-zA-Z0-9_\-\.]+)[`\"']?", re.IGNORECASE),
        re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+[`\"']?([a-zA-Z0-9_\-\.]+)[`\"']?", re.IGNORECASE),
        re.compile(r"\.write(?:\.mode\([^)]+\))?\.saveAsTable\([\"']([a-zA-Z0-9_\-\.]+)[\"']\)", re.IGNORECASE),
        re.compile(r"\.write(?:\.mode\([^)]+\))?\.insertInto\([\"']([a-zA-Z0-9_\-\.]+)[\"']\)", re.IGNORECASE),
    ]

    JDBC_PATTERN = re.compile(r"jdbc:[a-zA-Z0-9]+://[^\s\"']+", re.IGNORECASE)
    API_PATTERN = re.compile(r"https?://[^\s\"']+/api/[^\s\"']+", re.IGNORECASE)

    def extract(
        self,
        code_text: str,
        explicit_targets: Optional[List[str]] = None,
        default_project: str = "acme-dwh-prod"
    ) -> LineageExtractionResult:
        ctes = self._extract_cte_names(code_text)
        targets = set(explicit_targets or [])
        sources = set()
        external_sources = []

        # 1. Detect External Ingestion endpoints
        for m in self.JDBC_PATTERN.finditer(code_text):
            external_sources.append({"type": "JDBC", "uri": m.group(0)})
        for m in self.API_PATTERN.finditer(code_text):
            external_sources.append({"type": "REST_API", "uri": m.group(0)})

        # 2. Extract Writes / Targets
        for pat in self.WRITE_PATTERNS:
            for m in pat.finditer(code_text):
                tbl = m.group(1).strip().strip("`").strip('"')
                if tbl and tbl.lower() not in ctes:
                    targets.add(tbl)

        # 3. Extract Reads / Upstream Sources
        for pat in self.READ_PATTERNS:
            for m in pat.finditer(code_text):
                tbl = m.group(1).strip().strip("`").strip('"')
                # Filter out CTEs, subquery aliases, functions, and target tables
                if tbl and tbl.lower() not in ctes and tbl not in targets:
                    if not self._is_keyword_or_function(tbl):
                        sources.add(tbl)

        # 4. Classify pipeline type
        if external_sources and not sources:
            pipeline_type = "EXTERNAL_INGESTION"
        elif sources and targets:
            pipeline_type = "DW_TRANSFORMATION"
        else:
            pipeline_type = "HYBRID"

        formatted_sources = [self._normalize_table_ref(s, default_project) for s in sorted(sources)]
        formatted_targets = [self._normalize_table_ref(t, default_project) for t in sorted(targets)]

        return LineageExtractionResult(
            pipeline_type=pipeline_type,
            upstream_sources=formatted_sources,
            target_tables=formatted_targets,
            external_sources=external_sources,
            intermediate_ctes=sorted(list(ctes))
        )

    def _extract_cte_names(self, sql_or_code: str) -> Set[str]:
        ctes = set()
        cte_matches = re.findall(r"(?:WITH|,)\s*([a-zA-Z0-9_]+)\s+AS\s*\(", sql_or_code, re.IGNORECASE)
        for c in cte_matches:
            ctes.add(c.lower())
        temp_views = re.findall(r"\.createOrReplaceTempView\([\"']([a-zA-Z0-9_]+)[\"']\)", sql_or_code)
        for tv in temp_views:
            ctes.add(tv.lower())
        return ctes

    def _is_keyword_or_function(self, token: str) -> bool:
        keywords = {
            "select", "where", "unnest", "generate_date_array", "values",
            "lateral", "window", "qualify", "group", "order", "having"
        }
        return token.lower() in keywords

    def _normalize_table_ref(self, ref: str, default_project: str) -> Dict[str, str]:
        parts = ref.replace("`", "").split(".")
        if len(parts) == 3:
            return {"project_id": parts[0], "dataset_id": parts[1], "table_id": parts[2]}
        elif len(parts) == 2:
            return {"project_id": default_project, "dataset_id": parts[0], "table_id": parts[1]}
        else:
            return {"project_id": default_project, "dataset_id": "default", "table_id": parts[0]}
