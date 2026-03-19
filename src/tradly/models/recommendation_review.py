from __future__ import annotations

from datetime import datetime
from typing import Any


PROMOTE_CONFIDENCE_MIN = 60
MIXED_STRONG_PROMOTE_CONFIDENCE_MIN = 72


def _healthcare_subtype(metadata: dict[str, Any]) -> str:
    roles = {str(role).strip().lower() for role in metadata.get("roles", []) if str(role).strip()}
    industry = str(metadata.get("industry", "")).strip().lower()
    if "managed_care_retail_health" in roles or "healthcare plan" in industry or "pharmacy" in industry:
        return "managed_care_retail_health"
    if "quality_tools_devices" in roles or "device" in industry or "diagnostics" in industry or "instrument" in industry:
        return "quality_tools_devices"
    if "pharma_defensive" in roles or "drug manufacturers" in industry or "biotech" in industry:
        return "pharma_defensive"
    return "general_healthcare"


def _review_disposition(row: dict[str, Any], *, intraday_actionable: bool) -> tuple[str, str]:
    action = str(row.get("recommended_action", "")).strip()
    recommended_horizon = str(row.get("recommended_horizon", "")).strip()
    regime_alignment = str(row.get("regime_alignment", "")).strip().lower()
    evidence_balance_class = str(row.get("evidence_balance_class", "")).strip().lower()
    confidence = int(row.get("confidence_score", 0) or 0)
    source_state = str(row.get("source_state", "")).strip().lower()
    execution_ready = bool(row.get("execution_ready", True))

    if action in {"Blocked", "Unknown"} or source_state == "blocked":
        return "blocked", "blocked_or_unknown"
    if action in {"Defer Buy", "Defer Trim", "Defer"} or not execution_ready:
        return "defer", "execution_deferred"
    if recommended_horizon == "1to3d" and action in {"Buy", "Sell/Trim"} and not intraday_actionable:
        return "defer", "intraday_freshness_not_ready"
    if action in {"Watch Buy", "Watch Trim", "Hold/Watch", "Hold"}:
        return "watch", "recommendation_not_actionable"
    if action in {"Buy", "Sell/Trim"}:
        if regime_alignment == "contrarian":
            return "review_required", "contrarian_setup"
        if regime_alignment == "mixed":
            if evidence_balance_class == "mixed_strong" and confidence >= MIXED_STRONG_PROMOTE_CONFIDENCE_MIN:
                return "promote", "mixed_strong_actionable"
            return "review_required", "mixed_setup"
        if confidence < PROMOTE_CONFIDENCE_MIN:
            return "review_required", "confidence_below_promote_threshold"
        return "promote", "regime_aligned_actionable"
    return "watch", "unclassified_action"


def _review_bucket(row: dict[str, Any], disposition: str) -> str:
    action = str(row.get("recommended_action", "")).strip()
    if disposition == "promote":
        return "top_longs" if action == "Buy" else "top_shorts" if action == "Sell/Trim" else "top_ideas"
    if disposition == "review_required":
        if str(row.get("regime_alignment", "")).strip().lower() == "contrarian":
            return "contrarian_rebound" if action == "Buy" else "contrarian_review"
        return "manual_review"
    if disposition == "defer":
        return "deferred"
    if disposition == "blocked":
        return "blocked"
    return "watchlist"


def build_review_rows(
    *,
    recommendation_rows: list[dict[str, Any]],
    now_utc: datetime,
    intraday_actionable: bool = True,
    symbol_metadata: dict[str, dict[str, Any]] | None = None,
    symbol_news_rows_by_symbol: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    symbol_metadata = symbol_metadata or {}
    symbol_news_rows_by_symbol = symbol_news_rows_by_symbol or {}
    for row in recommendation_rows:
        if not isinstance(row, dict):
            continue
        scope_id = str(row.get("scope_id", "")).strip()
        if not scope_id:
            continue
        disposition, reason_code = _review_disposition(row, intraday_actionable=intraday_actionable)
        metadata = symbol_metadata.get(scope_id, {})
        symbol_news_row = symbol_news_rows_by_symbol.get(scope_id, {})
        sector = str(metadata.get("sector", "")).strip()
        direct_news = bool(metadata.get("direct_news", False))
        onboarding_stage = str(metadata.get("onboarding_stage", "")).strip()
        symbol_news_coverage = str(symbol_news_row.get("coverage_state", "")).strip().lower()
        healthcare_subtype = ""

        if sector == "Healthcare":
            healthcare_subtype = _healthcare_subtype(metadata)
            if direct_news and symbol_news_coverage in {"thin_evidence", "insufficient_evidence"}:
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code in {"regime_aligned_actionable", "mixed_strong_actionable"}:
                    reason_code = "healthcare_thin_evidence"
            elif disposition == "promote":
                if healthcare_subtype == "pharma_defensive":
                    reason_code = "healthcare_pharma_actionable"
                elif healthcare_subtype == "quality_tools_devices":
                    reason_code = "healthcare_tools_devices_actionable"
                elif healthcare_subtype == "managed_care_retail_health":
                    reason_code = "healthcare_managed_care_actionable"
                else:
                    reason_code = "healthcare_actionable"
            elif disposition == "review_required" and onboarding_stage == "modeled":
                reason_code = "healthcare_probationary_modeled"

        review_bucket = _review_bucket(row, disposition)
        rows.append(
            {
                "model_id": "recommendation_review_v1",
                "scope_id": scope_id,
                "recommended_action": row.get("recommended_action"),
                "recommended_horizon": row.get("recommended_horizon"),
                "recommendation_class": row.get("recommendation_class"),
                "evidence_balance_class": row.get("evidence_balance_class"),
                "regime_alignment": row.get("regime_alignment"),
                "signal_direction": row.get("signal_direction"),
                "confidence_score": row.get("confidence_score"),
                "execution_ready": row.get("execution_ready"),
                "source_state": row.get("source_state"),
                "review_disposition": disposition,
                "review_bucket": review_bucket,
                "review_reason_code": reason_code,
                "sector": sector,
                "sector_subtype": healthcare_subtype,
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
