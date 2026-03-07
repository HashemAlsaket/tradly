from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

AuditDecision = Literal["pass", "fail"]
RunType = Literal["pre_market", "post_close", "event_driven"]


@dataclass(frozen=True)
class AuditStatus:
    data_audit: AuditDecision
    calculation_audit: AuditDecision
    decision_audit: AuditDecision

    @property
    def aggregate(self) -> AuditDecision:
        if "fail" in (self.data_audit, self.calculation_audit, self.decision_audit):
            return "fail"
        return "pass"


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    run_type: RunType
    started_at: datetime
    completed_at: datetime
    broker_state_freshness_seconds: int
    input_snapshots: list[str]
    audit_status: AuditStatus
    action_safe: bool
    degraded_components: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if self.broker_state_freshness_seconds < 0:
            raise ValueError("broker_state_freshness_seconds must be >= 0")
        if self.started_at.tzinfo is None or self.completed_at.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
