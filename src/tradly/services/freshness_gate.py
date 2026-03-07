from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


BROKER_STATE_SLA_SECONDS = 120


@dataclass(frozen=True)
class FreshnessResult:
    is_fresh: bool
    freshness_seconds: int
    sla_seconds: int
    reason: str


def evaluate_broker_state_freshness(
    as_of_timestamp: datetime,
    now: datetime | None = None,
    sla_seconds: int = BROKER_STATE_SLA_SECONDS,
) -> FreshnessResult:
    """Blocking gate for recommendation runs when broker-state data is stale."""

    if as_of_timestamp.tzinfo is None:
        raise ValueError("as_of_timestamp must be timezone-aware")

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    freshness_seconds = int((current - as_of_timestamp).total_seconds())
    is_fresh = freshness_seconds <= sla_seconds

    reason = (
        "broker_state_fresh"
        if is_fresh
        else "broker_state_stale_blocking_recommendations"
    )

    return FreshnessResult(
        is_fresh=is_fresh,
        freshness_seconds=freshness_seconds,
        sla_seconds=sla_seconds,
        reason=reason,
    )
