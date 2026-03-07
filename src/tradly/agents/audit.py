from __future__ import annotations

from dataclasses import dataclass

from tradly.schemas.run_manifest import AuditStatus
from tradly.services.freshness_gate import FreshnessResult


@dataclass(frozen=True)
class DataAuditOutput:
    decision: str
    broker_freshness_seconds: int
    detail: str


def run_data_audit(freshness: FreshnessResult) -> DataAuditOutput:
    if freshness.is_fresh:
        return DataAuditOutput(
            decision="pass",
            broker_freshness_seconds=freshness.freshness_seconds,
            detail="Broker state is within freshness SLA.",
        )

    return DataAuditOutput(
        decision="fail",
        broker_freshness_seconds=freshness.freshness_seconds,
        detail="Broker state is stale; recommendations are blocked.",
    )


def aggregate_action_safe(audit_status: AuditStatus) -> bool:
    return audit_status.aggregate == "pass"
