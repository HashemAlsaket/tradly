from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Action = Literal["Strong Buy", "Buy", "Watch", "Trim", "Exit"]


@dataclass(frozen=True)
class InvestabilityDecision:
    final_action: Action
    blocked: bool
    reason_code: str | None


BUY_ACTIONS = {"Strong Buy", "Buy"}
SOFT_BLOCK_STATUSES = {"review_required"}
HARD_BLOCK_STATUSES = {"probably_not_halal", "not_halal"}
VALID_STATUSES = {"investable", "review_required", "probably_not_halal", "not_halal"}


def apply_investability_gate(proposed_action: Action, halal_flag: str) -> InvestabilityDecision:
    status = halal_flag.strip().lower()

    if status not in VALID_STATUSES:
        return InvestabilityDecision(
            final_action="Watch",
            blocked=True,
            reason_code="investability_unknown_status",
        )

    if status in HARD_BLOCK_STATUSES and proposed_action in BUY_ACTIONS:
        return InvestabilityDecision(
            final_action="Watch",
            blocked=True,
            reason_code="investability_blocked",
        )

    if status in SOFT_BLOCK_STATUSES and proposed_action in BUY_ACTIONS:
        return InvestabilityDecision(
            final_action="Watch",
            blocked=True,
            reason_code="investability_review_required",
        )

    return InvestabilityDecision(
        final_action=proposed_action,
        blocked=False,
        reason_code=None,
    )
