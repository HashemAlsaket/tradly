from __future__ import annotations

from datetime import datetime
from typing import Any


HORIZON_ORDER = ("1to3d", "1to2w", "2to6w")
SHORTER_HORIZON_PRIORITY = {"1to3d": 2, "1to2w": 1, "2to6w": 0}


def action_for_horizon(horizon_row: dict[str, Any]) -> str:
    state = str(horizon_row.get("state", "missing")).strip().lower()
    direction = str(horizon_row.get("signal_direction", "neutral")).strip().lower()
    confidence = int(horizon_row.get("confidence_score", 0) or 0)
    execution_ready = bool(horizon_row.get("execution_ready", True))
    if state == "actionable":
        if not execution_ready:
            if direction == "bullish":
                return "Defer Buy"
            if direction == "bearish":
                return "Defer Trim"
            return "Defer"
        if direction == "bullish":
            return "Buy"
        if direction == "bearish":
            return "Sell/Trim"
        return "Hold"
    if state == "research_only":
        if direction == "bullish" and confidence >= 60:
            return "Watch Buy"
        if direction == "bearish" and confidence >= 55:
            return "Watch Trim"
        return "Hold/Watch"
    if state == "blocked":
        return "Blocked"
    return "Unknown"


def action_priority(action: str) -> int:
    return {
        "Buy": 6,
        "Sell/Trim": 6,
        "Defer Buy": 5,
        "Defer Trim": 5,
        "Defer": 5,
        "Watch Buy": 4,
        "Watch Trim": 4,
        "Hold": 3,
        "Hold/Watch": 2,
        "Blocked": 1,
        "Unknown": 0,
    }.get(action, 0)


def _coverage_rank(coverage_state: str) -> int:
    return {"insufficient_evidence": 0, "thin_evidence": 1, "sufficient_evidence": 2}.get(coverage_state, 0)


def _regime_alignment(direction: str, why_codes: list[str]) -> str:
    normalized_direction = str(direction).strip().lower()
    codes = {str(code).strip() for code in why_codes}
    market_supportive = "market_context_supportive" in codes
    market_headwind = "market_context_headwind" in codes
    if normalized_direction == "bullish":
        if market_supportive:
            return "aligned"
        if market_headwind:
            return "contrarian"
    if normalized_direction == "bearish":
        if market_headwind:
            return "aligned"
        if market_supportive:
            return "contrarian"
    return "mixed"


def _recommendation_class(action: str, *, direction: str, regime_alignment: str) -> str:
    normalized_direction = str(direction).strip().lower()
    if action == "Buy":
        return "aligned_long" if regime_alignment == "aligned" else "contrarian_long" if regime_alignment == "contrarian" else "long"
    if action == "Sell/Trim":
        return "aligned_short" if regime_alignment == "aligned" else "contrarian_short" if regime_alignment == "contrarian" else "short"
    if action in {"Defer Buy", "Defer Trim", "Defer"}:
        if action == "Defer Buy":
            return "deferred_long"
        if action == "Defer Trim":
            return "deferred_short"
        return "deferred"
    if action == "Watch Buy" and normalized_direction == "bullish":
        return "watch_long"
    if action == "Watch Trim" and normalized_direction == "bearish":
        return "watch_short"
    if action == "Blocked":
        return "blocked"
    return "watch"


def _best_horizon(summary: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    ranked: list[tuple[tuple[int, int, int, float, int], str, dict[str, Any], str]] = []
    for horizon in HORIZON_ORDER:
        horizon_row = summary.get(horizon, {}) if isinstance(summary, dict) else {}
        if not isinstance(horizon_row, dict):
            horizon_row = {}
        action = action_for_horizon(horizon_row)
        ranking = (
            action_priority(action),
            int(horizon_row.get("confidence_score", 0) or 0),
            _coverage_rank(str(horizon_row.get("coverage_state", "insufficient_evidence"))),
            abs(float(horizon_row.get("score_normalized", 0.0) or 0.0)),
            SHORTER_HORIZON_PRIORITY.get(horizon, -1),
        )
        ranked.append((ranking, horizon, horizon_row, action))
    ranked.sort(reverse=True)
    _, horizon, horizon_row, action = ranked[0]
    return horizon, horizon_row, action


def build_recommendation_rows(*, ensemble_rows: list[dict], now_utc: datetime) -> list[dict]:
    recommendations: list[dict] = []
    for row in ensemble_rows:
        if not isinstance(row, dict):
            continue
        scope_id = str(row.get("scope_id", "")).strip()
        summary = row.get("horizon_summary", {})
        if not scope_id or not isinstance(summary, dict):
            continue
        recommended_horizon, horizon_row, action = _best_horizon(summary)
        why_codes = [str(code) for code in horizon_row.get("why_code", []) if str(code).strip()]
        direction = str(horizon_row.get("signal_direction", "neutral")).strip().lower()
        regime_alignment = _regime_alignment(direction, why_codes)
        recommendations.append(
            {
                "model_id": "recommendation_v1",
                "scope_id": scope_id,
                "recommended_action": action,
                "recommended_horizon": recommended_horizon,
                "recommendation_class": _recommendation_class(action, direction=direction, regime_alignment=regime_alignment),
                "regime_alignment": regime_alignment,
                "signal_direction": direction,
                "confidence_score": int(horizon_row.get("confidence_score", row.get("confidence_score", 0)) or 0),
                "confidence_label": str(horizon_row.get("confidence_label", row.get("confidence_label", "low"))),
                "coverage_state": str(horizon_row.get("coverage_state", row.get("coverage_state", "insufficient_evidence"))),
                "primary_reason_code": why_codes[0] if why_codes else "recommendation_signal_unclear",
                "why_code": why_codes,
                "execution_ready": bool(horizon_row.get("execution_ready", row.get("execution_ready", True))),
                "score_normalized": float(horizon_row.get("score_normalized", row.get("score_normalized", 0.0)) or 0.0),
                "source_state": str(horizon_row.get("state", "unknown")),
                "as_of_utc": now_utc.isoformat(),
            }
        )
    return sorted(
        recommendations,
        key=lambda row: (
            action_priority(str(row.get("recommended_action", ""))),
            int(row.get("confidence_score", 0) or 0),
            SHORTER_HORIZON_PRIORITY.get(str(row.get("recommended_horizon", "")), -1),
        ),
        reverse=True,
    )
