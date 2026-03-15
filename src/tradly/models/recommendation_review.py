from __future__ import annotations

from datetime import datetime
from typing import Any


PROMOTE_CONFIDENCE_MIN = 60


def _review_disposition(row: dict[str, Any]) -> tuple[str, str]:
    action = str(row.get("recommended_action", "")).strip()
    regime_alignment = str(row.get("regime_alignment", "")).strip().lower()
    confidence = int(row.get("confidence_score", 0) or 0)
    source_state = str(row.get("source_state", "")).strip().lower()
    execution_ready = bool(row.get("execution_ready", True))

    if action in {"Blocked", "Unknown"} or source_state == "blocked":
        return "blocked", "blocked_or_unknown"
    if action in {"Defer Buy", "Defer Trim", "Defer"} or not execution_ready:
        return "defer", "execution_deferred"
    if action in {"Watch Buy", "Watch Trim", "Hold/Watch", "Hold"}:
        return "watch", "recommendation_not_actionable"
    if action in {"Buy", "Sell/Trim"}:
        if regime_alignment == "contrarian":
            return "review_required", "contrarian_setup"
        if confidence < PROMOTE_CONFIDENCE_MIN:
            return "review_required", "confidence_below_promote_threshold"
        return "promote", "regime_aligned_actionable"
    return "watch", "unclassified_action"


def _review_bucket(row: dict[str, Any], disposition: str) -> str:
    action = str(row.get("recommended_action", "")).strip()
    if disposition == "promote":
        return "top_longs" if action == "Buy" else "top_shorts" if action == "Sell/Trim" else "top_ideas"
    if disposition == "review_required":
        return "contrarian_review" if str(row.get("regime_alignment", "")).strip().lower() == "contrarian" else "manual_review"
    if disposition == "defer":
        return "deferred"
    if disposition == "blocked":
        return "blocked"
    return "watchlist"


def build_review_rows(*, recommendation_rows: list[dict[str, Any]], now_utc: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in recommendation_rows:
        if not isinstance(row, dict):
            continue
        scope_id = str(row.get("scope_id", "")).strip()
        if not scope_id:
            continue
        disposition, reason_code = _review_disposition(row)
        review_bucket = _review_bucket(row, disposition)
        rows.append(
            {
                "model_id": "recommendation_review_v1",
                "scope_id": scope_id,
                "recommended_action": row.get("recommended_action"),
                "recommended_horizon": row.get("recommended_horizon"),
                "recommendation_class": row.get("recommendation_class"),
                "regime_alignment": row.get("regime_alignment"),
                "signal_direction": row.get("signal_direction"),
                "confidence_score": row.get("confidence_score"),
                "execution_ready": row.get("execution_ready"),
                "source_state": row.get("source_state"),
                "review_disposition": disposition,
                "review_bucket": review_bucket,
                "review_reason_code": reason_code,
                "primary_reason_code": row.get("primary_reason_code"),
                "why_code": row.get("why_code", []),
                "as_of_utc": now_utc.isoformat(),
            }
        )
    disposition_priority = {
        "promote": 4,
        "review_required": 3,
        "watch": 2,
        "defer": 1,
        "blocked": 0,
    }
    return sorted(
        rows,
        key=lambda row: (
            disposition_priority.get(str(row.get("review_disposition", "")), -1),
            int(row.get("confidence_score", 0) or 0),
        ),
        reverse=True,
    )
