from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionAttempt:
    """Represents an attempted execution request from any subsystem."""

    symbol: str
    side: str
    quantity: float
    reason: str


@dataclass(frozen=True)
class ExecutionDecision:
    allowed: bool
    reason: str


def block_all_execution(_: ExecutionAttempt) -> ExecutionDecision:
    """Hard safety rule: this system never places orders programmatically."""

    return ExecutionDecision(
        allowed=False,
        reason="execution_blocked_manual_only_user_executes_in_robinhood",
    )
