"""
Layer 4: Task Graph and Migration Backlog Auditor.

Detects 4 distinct classes of governance anomalies:
1. INVERTED_LINK: Predecessor task is erroneously declared as a successor in the agile board.
2. UNLINKED_PREDECESSOR: Discovered code dependency exists in the backlog, but is not linked to the current card.
3. PREDECESSOR_WITHOUT_PBI: Table is produced by a legacy routine that has no corresponding ticket in the backlog yet.
4. OMITTED_SOURCES: Sources physically read in legacy code that were omitted in the ticket's documentation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set

logger = logging.getLogger("lineage-validator.backlog")


@dataclass
class BacklogGovernanceAnomaly:
    anomaly_type: str  # INVERTED_LINK, UNLINKED_PREDECESSOR, PREDECESSOR_WITHOUT_PBI, OMITTED_SOURCES
    ticket_id: str
    predecessor_id: Optional[str] = None
    affected_resource: str = ""
    description: str = ""
    suggested_action: str = ""


@dataclass
class PredecessorRecommendation:
    required_table: str
    producer_job: str
    predecessor_ticket_id: Optional[str]
    ticket_status: str  # TO_DO, IN_PROGRESS, MERGED, UNMAPPED
    link_anomalies: List[BacklogGovernanceAnomaly] = field(default_factory=list)
    action_item: str = ""


class BacklogTaskAuditor:
    """Audits task dependencies, detecting all 4 classes of governance anomalies."""

    GENERIC_ORCHESTRATOR_PATTERNS = ["daily_cron", "master_dag", "orchestration_wrapper", "global_scheduler"]

    def audit_predecessor_tasks(
        self,
        current_ticket_id: str,
        missing_tables_producers: List[Dict[str, str]],
        backlog_items: List[Dict[str, Any]],
        physically_extracted_sources: Optional[List[str]] = None,
        documented_sources: Optional[List[str]] = None
    ) -> List[PredecessorRecommendation]:
        recommendations: List[PredecessorRecommendation] = []
        backlog_map = {item.get("ticket_id"): item for item in backlog_items if item.get("ticket_id")}
        job_to_ticket = {}

        for item in backlog_items:
            t_id = item.get("ticket_id")
            for job in item.get("legacy_jobs", []):
                job_to_ticket[job.lower()] = t_id
            for tbl in item.get("target_tables", []):
                job_to_ticket[tbl.lower()] = t_id

        current_item = backlog_map.get(current_ticket_id, {})
        declared_predecessors = current_item.get("predecessor_tickets", [])

        # Check Anomaly 4: OMITTED_SOURCES
        if physically_extracted_sources is not None and documented_sources is not None:
            phys_set = {s.lower().split(".")[-1] for s in physically_extracted_sources}
            doc_set = {s.lower().split(".")[-1] for s in documented_sources}
            omitted = phys_set - doc_set
            for o_tbl in omitted:
                recommendations.append(
                    PredecessorRecommendation(
                        required_table=o_tbl,
                        producer_job="code_extractor",
                        predecessor_ticket_id=None,
                        ticket_status="DOCUMENTATION_OMISSION",
                        link_anomalies=[
                            BacklogGovernanceAnomaly(
                                anomaly_type="OMITTED_SOURCES",
                                ticket_id=current_ticket_id,
                                affected_resource=o_tbl,
                                description=f"Source table '{o_tbl}' is physically read in legacy code but omitted from card's specification.",
                                suggested_action=f"Auto-heal card specification to include '{o_tbl}' in Section 3.2 (Required Sources)."
                            )
                        ],
                        action_item=f"Add '{o_tbl}' to card documented source dependencies."
                    )
                )

        # Audit missing producers against backlog
        for missing in missing_tables_producers:
            tbl = missing.get("table_name", "")
            producer_job = missing.get("producer_job", "")
            anomalies: List[BacklogGovernanceAnomaly] = []

            matched_ticket_id = job_to_ticket.get(producer_job.lower()) or job_to_ticket.get(tbl.lower())

            if matched_ticket_id:
                ticket_info = backlog_map.get(matched_ticket_id, {})
                ticket_status = ticket_info.get("status", "TO_DO")

                # Anomaly 1: INVERTED_LINK
                if current_ticket_id in ticket_info.get("predecessor_tickets", []):
                    anomalies.append(
                        BacklogGovernanceAnomaly(
                            anomaly_type="INVERTED_LINK",
                            ticket_id=current_ticket_id,
                            predecessor_id=matched_ticket_id,
                            affected_resource=tbl,
                            description=f"Inverted Dependency: Predecessor ticket '{matched_ticket_id}' has current card '{current_ticket_id}' registered as predecessor!",
                            suggested_action=f"Invert relationship: '{matched_ticket_id}' must block '{current_ticket_id}', not vice versa."
                        )
                    )

                # Anomaly 2: UNLINKED_PREDECESSOR
                if matched_ticket_id not in declared_predecessors:
                    anomalies.append(
                        BacklogGovernanceAnomaly(
                            anomaly_type="UNLINKED_PREDECESSOR",
                            ticket_id=current_ticket_id,
                            predecessor_id=matched_ticket_id,
                            affected_resource=tbl,
                            description=f"Unlinked Predecessor: Pipeline consumes '{tbl}' produced by '{matched_ticket_id}', but ticket is unlinked on board.",
                            suggested_action=f"Link '{matched_ticket_id}' as blocking predecessor of '{current_ticket_id}'."
                        )
                    )

                action = f"Prioritize predecessor ticket '{matched_ticket_id}' (Status: {ticket_status}) to unblock table '{tbl}'."
            else:
                # Anomaly 3: PREDECESSOR_WITHOUT_PBI
                anomalies.append(
                    BacklogGovernanceAnomaly(
                        anomaly_type="PREDECESSOR_WITHOUT_PBI",
                        ticket_id=current_ticket_id,
                        affected_resource=tbl,
                        description=f"Missing PBI: Legacy producer routine '{producer_job}' does not have any corresponding migration card in the backlog.",
                        suggested_action=f"Create a new migration PBI for producer job '{producer_job}' to deliver table '{tbl}'."
                    )
                )
                action = f"Create new migration card for legacy producer '{producer_job}'."
                ticket_status = "UNMAPPED"

            recommendations.append(
                PredecessorRecommendation(
                    required_table=tbl,
                    producer_job=producer_job,
                    predecessor_ticket_id=matched_ticket_id,
                    ticket_status=ticket_status,
                    link_anomalies=anomalies,
                    action_item=action
                )
            )

        return recommendations
