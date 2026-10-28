"""
Layer 4: Task Graph and Migration Backlog Auditor.

Cross-references discovered legacy producer jobs with project management tickets (Jira / Azure DevOps),
identifying:
- Exactly which Predecessor Ticket must be prioritized before this pipeline can begin.
- Link anomalies:
    * Inverted links (child depends on parent incorrectly inverted)
    * Orphan predecessor tickets (referenced ticket was deleted or closed without delivery)
    * False links to generic orchestrators (e.g. linked to daily_cron_master rather than the producer pipeline)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

logger = logging.getLogger("lineage-validator.backlog")


@dataclass
class BacklogLinkAnomaly:
    anomaly_type: str  # INVERTED_DEPENDENCY, ORPHAN_PREDECESSOR, GENERIC_ORCHESTRATOR_LINK
    ticket_id: str
    predecessor_id: Optional[str] = None
    description: str = ""
    suggested_action: str = ""


@dataclass
class PredecessorRecommendation:
    required_table: str
    producer_job: str
    predecessor_ticket_id: Optional[str]
    ticket_status: str  # TO_DO, IN_PROGRESS, MERGED, UNMAPPED
    link_anomalies: List[BacklogLinkAnomaly] = field(default_factory=list)
    action_item: str = ""


class BacklogTaskAuditor:
    """Audits task dependencies, linking producer jobs to migration backlog items."""

    GENERIC_ORCHESTRATOR_PATTERNS = ["daily_cron", "master_dag", "orchestration_wrapper", "global_scheduler"]

    def audit_predecessor_tasks(
        self,
        current_ticket_id: str,
        missing_tables_producers: List[Dict[str, str]],
        backlog_items: List[Dict[str, Any]]
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

        for missing in missing_tables_producers:
            tbl = missing.get("table_name", "")
            producer_job = missing.get("producer_job", "")
            anomalies: List[BacklogLinkAnomaly] = []

            # 1. Match producer job to backlog ticket
            matched_ticket_id = job_to_ticket.get(producer_job.lower()) or job_to_ticket.get(tbl.lower())
            
            # Check for generic orchestrator false link
            for gen in self.GENERIC_ORCHESTRATOR_PATTERNS:
                if gen in producer_job.lower():
                    anomalies.append(
                        BacklogLinkAnomaly(
                            anomaly_type="GENERIC_ORCHESTRATOR_LINK",
                            ticket_id=current_ticket_id,
                            description=f"Producer job '{producer_job}' is a generic wrapper/scheduler. Needs deep drilldown to find atomic pipeline ticket.",
                            suggested_action="Decouple wrapper from backlog and link directly to underlying component pipeline ticket."
                        )
                    )

            ticket_status = "UNMAPPED"
            if matched_ticket_id:
                ticket_info = backlog_map.get(matched_ticket_id, {})
                ticket_status = ticket_info.get("status", "TO_DO")

                # Check if current ticket declared this predecessor
                if matched_ticket_id not in declared_predecessors:
                    anomalies.append(
                        BacklogLinkAnomaly(
                            anomaly_type="ORPHAN_PREDECESSOR",
                            ticket_id=current_ticket_id,
                            predecessor_id=matched_ticket_id,
                            description=f"Current card '{current_ticket_id}' requires '{tbl}' from '{matched_ticket_id}', but ticket is NOT declared as predecessor in agile board.",
                            suggested_action=f"Add '{matched_ticket_id}' as blocking predecessor of '{current_ticket_id}' in Jira/DevOps."
                        )
                    )

                # Check for inverted dependency
                if current_ticket_id in ticket_info.get("predecessor_tickets", []):
                    anomalies.append(
                        BacklogLinkAnomaly(
                            anomaly_type="INVERTED_DEPENDENCY",
                            ticket_id=current_ticket_id,
                            predecessor_id=matched_ticket_id,
                            description=f"Circular / Inverted Link: Predecessor '{matched_ticket_id}' declares current ticket '{current_ticket_id}' as its predecessor!",
                            suggested_action=f"Reverse link direction: '{matched_ticket_id}' must precede '{current_ticket_id}'."
                        )
                    )

                action = f"Prioritize predecessor ticket '{matched_ticket_id}' (Current status: {ticket_status}) to unblock table '{tbl}'."
            else:
                action = f"No backlog ticket found for legacy producer '{producer_job}'. Create a new migration card for '{producer_job}'."

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
