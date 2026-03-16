from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


HORIZON_TO_TRADING_DAYS = {
    "1to3d": 3,
    "1to2w": 10,
    "2to6w": 30,
}


def _parse_utc_timestamp(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _expected_direction(action: str) -> str:
    normalized = str(action).strip().lower()
    if normalized in {"buy", "watch buy", "defer buy"}:
        return "bullish"
    if normalized in {"sell/trim", "watch trim", "defer trim"}:
        return "bearish"
    return "neutral"


def _score_outcome(expected_direction: str, realized_return_pct: float) -> tuple[float | None, str]:
    if expected_direction == "bullish":
        directional = realized_return_pct
    elif expected_direction == "bearish":
        directional = -realized_return_pct
    else:
        return None, "not_scored"

    if directional > 0:
        return directional, "correct"
    if directional < 0:
        return directional, "incorrect"
    return directional, "flat"


def build_scorecard_rows(
    *,
    recommendation_rows: list[dict[str, Any]],
    bars_by_symbol: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for recommendation in recommendation_rows:
        scope_id = str(recommendation.get("scope_id", "")).strip()
        horizon = str(recommendation.get("recommended_horizon", "")).strip()
        trading_days = HORIZON_TO_TRADING_DAYS.get(horizon)
        expected_direction = _expected_direction(str(recommendation.get("recommended_action", "")))
        base_row = {
            "scope_id": scope_id or None,
            "recommended_action": recommendation.get("recommended_action"),
            "recommended_horizon": horizon or None,
            "recommendation_class": recommendation.get("recommendation_class"),
            "regime_alignment": recommendation.get("regime_alignment"),
            "review_disposition": recommendation.get("review_disposition"),
            "review_bucket": recommendation.get("review_bucket"),
            "confidence_score": recommendation.get("confidence_score"),
            "recommendation_as_of_utc": None,
            "trading_days_target": trading_days,
            "expected_direction": expected_direction,
        }
        if not scope_id:
            rows.append(
                {
                    **base_row,
                    "evaluation_status": "not_scored",
                    "pending_reason": None,
                    "not_scored_reason": "missing_scope_id",
                    "entry_ts_utc": None,
                    "entry_close": None,
                    "exit_ts_utc": None,
                    "exit_close": None,
                    "realized_return_pct": None,
                    "directional_return_pct": None,
                    "outcome_label": "not_scored",
                }
            )
            continue
        if not trading_days:
            rows.append(
                {
                    **base_row,
                    "evaluation_status": "not_scored",
                    "pending_reason": None,
                    "not_scored_reason": "unsupported_horizon",
                    "entry_ts_utc": None,
                    "entry_close": None,
                    "exit_ts_utc": None,
                    "exit_close": None,
                    "realized_return_pct": None,
                    "directional_return_pct": None,
                    "outcome_label": "not_scored",
                }
            )
            continue

        as_of_raw = str(recommendation.get("as_of_utc", "")).strip()
        if not as_of_raw:
            rows.append(
                {
                    **base_row,
                    "evaluation_status": "not_scored",
                    "pending_reason": None,
                    "not_scored_reason": "missing_as_of_utc",
                    "entry_ts_utc": None,
                    "entry_close": None,
                    "exit_ts_utc": None,
                    "exit_close": None,
                    "realized_return_pct": None,
                    "directional_return_pct": None,
                    "outcome_label": "not_scored",
                }
            )
            continue
        as_of_utc = _parse_utc_timestamp(as_of_raw)
        symbol_bars = sorted(
            bars_by_symbol.get(scope_id, []),
            key=lambda row: row["ts_utc"],
        )
        base_row["recommendation_as_of_utc"] = as_of_utc.isoformat()
        if not symbol_bars:
            rows.append(
                {
                    **base_row,
                    "evaluation_status": "not_scored",
                    "pending_reason": None,
                    "not_scored_reason": "missing_symbol_bars",
                    "entry_ts_utc": None,
                    "entry_close": None,
                    "exit_ts_utc": None,
                    "exit_close": None,
                    "realized_return_pct": None,
                    "directional_return_pct": None,
                    "outcome_label": "not_scored",
                }
            )
            continue
        entry_index = next((idx for idx, bar in enumerate(symbol_bars) if bar["ts_utc"] > as_of_utc), None)

        if entry_index is None:
            rows.append(
                {
                    **base_row,
                    "evaluation_status": "pending",
                    "pending_reason": "waiting_for_entry_bar",
                    "not_scored_reason": None,
                    "entry_ts_utc": None,
                    "entry_close": None,
                    "exit_ts_utc": None,
                    "exit_close": None,
                    "realized_return_pct": None,
                    "directional_return_pct": None,
                    "outcome_label": "pending",
                }
            )
            continue

        exit_index = entry_index + trading_days - 1
        entry_bar = symbol_bars[entry_index]
        if exit_index >= len(symbol_bars):
            rows.append(
                {
                    **base_row,
                    "evaluation_status": "pending",
                    "pending_reason": "waiting_for_exit_bar",
                    "not_scored_reason": None,
                    "entry_ts_utc": entry_bar["ts_utc"].isoformat(),
                    "entry_close": entry_bar["close"],
                    "exit_ts_utc": None,
                    "exit_close": None,
                    "realized_return_pct": None,
                    "directional_return_pct": None,
                    "outcome_label": "pending",
                }
            )
            continue

        exit_bar = symbol_bars[exit_index]
        entry_close = float(entry_bar["close"])
        exit_close = float(exit_bar["close"])
        realized_return_pct = ((exit_close - entry_close) / entry_close) * 100.0 if entry_close else 0.0
        directional_return_pct, outcome_label = _score_outcome(expected_direction, realized_return_pct)

        rows.append(
            {
                **base_row,
                "evaluation_status": "scored" if directional_return_pct is not None else "not_scored",
                "pending_reason": None,
                "not_scored_reason": None if directional_return_pct is not None else "neutral_direction",
                "entry_ts_utc": entry_bar["ts_utc"].isoformat(),
                "entry_close": entry_close,
                "exit_ts_utc": exit_bar["ts_utc"].isoformat(),
                "exit_close": exit_close,
                "realized_return_pct": round(realized_return_pct, 4),
                "directional_return_pct": round(directional_return_pct, 4) if directional_return_pct is not None else None,
                "outcome_label": outcome_label,
            }
        )

    return rows


def summarize_scorecard(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "total_recommendations": len(rows),
        "pending_count": sum(1 for row in rows if row.get("evaluation_status") == "pending"),
        "scored_count": sum(1 for row in rows if row.get("evaluation_status") == "scored"),
        "not_scored_count": sum(1 for row in rows if row.get("evaluation_status") == "not_scored"),
        "correct_count": sum(1 for row in rows if row.get("outcome_label") == "correct"),
        "incorrect_count": sum(1 for row in rows if row.get("outcome_label") == "incorrect"),
        "flat_count": sum(1 for row in rows if row.get("outcome_label") == "flat"),
    }
    directional_returns = [
        float(row["directional_return_pct"])
        for row in rows
        if row.get("directional_return_pct") is not None
    ]
    if directional_returns:
        summary["average_directional_return_pct"] = round(sum(directional_returns) / len(directional_returns), 4)
        summary["hit_rate"] = round(summary["correct_count"] / len(directional_returns), 4)
    else:
        summary["average_directional_return_pct"] = None
        summary["hit_rate"] = None
    return summary


def _group_key(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    return str(value).strip() if value is not None and str(value).strip() else "unknown"


def summarize_groups(rows: list[dict[str, Any]], *, field: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_group_key(row, field), []).append(row)
    return {key: summarize_scorecard(group_rows) for key, group_rows in sorted(grouped.items())}
