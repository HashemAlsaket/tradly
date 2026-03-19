from __future__ import annotations

from datetime import datetime
from typing import Any


PROMOTE_CONFIDENCE_MIN = 60
MIXED_STRONG_PROMOTE_CONFIDENCE_MIN = 72
MIXED_WEAK_PROMOTE_CONFIDENCE_MIN = 66


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


def _industrials_subtype(metadata: dict[str, Any]) -> str:
    roles = {str(role).strip().lower() for role in metadata.get("roles", []) if str(role).strip()}
    industry = str(metadata.get("industry", "")).strip().lower()
    if "heavy_equipment_capex" in roles or "construction machinery" in industry or "farm" in industry:
        return "heavy_equipment_capex"
    if "aerospace_defense" in roles or "aerospace" in industry or "defense" in industry:
        return "aerospace_defense"
    if "diversified_industrials" in roles or "industrial machinery" in industry:
        return "diversified_industrials"
    if "rails_logistics" in roles or "rail" in industry or "logistics" in industry or "freight" in industry:
        return "rails_logistics"
    return "general_industrials"


def _consumer_defensive_subtype(metadata: dict[str, Any]) -> str:
    roles = {str(role).strip().lower() for role in metadata.get("roles", []) if str(role).strip()}
    industry = str(metadata.get("industry", "")).strip().lower()
    if "membership_retail" in roles or "discount stores" in industry:
        return "membership_retail"
    if "household_personal_care" in roles or "household" in industry or "personal products" in industry:
        return "household_personal_care"
    if "staples_beverages" in roles or "beverages" in industry:
        return "staples_beverages"
    if "defensive_brand_staples" in roles or "packaged foods" in industry or "confectioners" in industry:
        return "defensive_brand_staples"
    return "general_consumer_defensive"


def _communication_services_subtype(metadata: dict[str, Any]) -> str:
    roles = {str(role).strip().lower() for role in metadata.get("roles", []) if str(role).strip()}
    industry = str(metadata.get("industry", "")).strip().lower()
    if "internet_platforms" in roles or "internet content" in industry:
        return "internet_platforms"
    if "streaming_media" in roles or "streaming" in industry:
        return "streaming_media"
    if "media_entertainment" in roles or "entertainment" in industry:
        return "media_entertainment"
    if "cable_broadband" in roles or "telecom services" in industry or "broadband" in industry:
        return "cable_broadband"
    return "general_communication_services"


def _technology_subtype(metadata: dict[str, Any]) -> str:
    roles = {str(role).strip().lower() for role in metadata.get("roles", []) if str(role).strip()}
    industry = str(metadata.get("industry", "")).strip().lower()
    if "cloud_platforms" in roles or "software - infrastructure" in industry:
        return "cloud_platforms"
    if "enterprise_software" in roles or "software - application" in industry:
        return "enterprise_software"
    if "networking_infrastructure" in roles or "communication equipment" in industry or "network" in industry:
        return "networking_infrastructure"
    if "consumer_hardware" in roles or "consumer electronics" in industry or "computer hardware" in industry:
        return "consumer_hardware"
    if "ai_application_software" in roles:
        return "ai_application_software"
    return "general_technology"


def _energy_subtype(metadata: dict[str, Any]) -> str:
    roles = {str(role).strip().lower() for role in metadata.get("roles", []) if str(role).strip()}
    industry = str(metadata.get("industry", "")).strip().lower()
    if "integrated_majors" in roles or "integrated" in industry:
        return "integrated_majors"
    if "energy_services" in roles or "equipment & services" in industry or "services" in industry:
        return "energy_services"
    if "upstream_ep" in roles or "e&p" in industry or "exploration" in industry:
        return "upstream_ep"
    return "general_energy"


def _market_stress_level(market_row: dict[str, Any] | None) -> str:
    if not isinstance(market_row, dict):
        return "low"
    signal = str(market_row.get("signal_direction", "")).strip().lower()
    confidence = int(market_row.get("confidence_score", 0) or 0)
    evidence = market_row.get("evidence", {}) if isinstance(market_row.get("evidence"), dict) else {}
    macro_hostility = evidence.get("macro_hostility", {}) if isinstance(evidence.get("macro_hostility"), dict) else {}
    macro_state = str(macro_hostility.get("macro_state", "")).strip().lower()
    why_codes = {str(code).strip().lower() for code in market_row.get("why_code", []) if str(code).strip()}

    if signal == "bearish" and confidence >= 70 and (
        macro_state in {"risk_off", "macro_unstable"}
        or {"vix_elevated", "macro_rates_pressure", "macro_energy_stress"} & why_codes
    ):
        return "high"
    if signal == "bearish" and confidence >= 60:
        return "medium"
    if signal == "neutral" and confidence >= 65 and macro_state in {"risk_off", "macro_unstable"}:
        return "medium"
    return "low"


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
            if (
                evidence_balance_class == "mixed_weak"
                and recommended_horizon in {"1to2w", "2to6w"}
                and confidence >= MIXED_WEAK_PROMOTE_CONFIDENCE_MIN
            ):
                return "promote", "mixed_cautious_actionable"
            return "review_required", "mixed_setup"
        if confidence < PROMOTE_CONFIDENCE_MIN:
            return "review_required", "confidence_below_promote_threshold"
        return "promote", "regime_aligned_actionable"
    return "watch", "unclassified_action"


def _review_bucket(row: dict[str, Any], disposition: str, reason_code: str) -> str:
    action = str(row.get("recommended_action", "")).strip()
    if disposition == "promote":
        return "top_longs" if action == "Buy" else "top_shorts" if action == "Sell/Trim" else "top_ideas"
    if disposition == "review_required":
        if str(row.get("regime_alignment", "")).strip().lower() == "contrarian":
            return "contrarian_rebound" if action == "Buy" else "contrarian_review"
        if action == "Buy" and int(row.get("confidence_score", 0) or 0) >= 70:
            return "review_high_priority"
        return "manual_review"
    if disposition == "defer":
        return "deferred"
    if disposition == "blocked":
        return "blocked"
    if reason_code in {"event_buy_capped_to_watch", "event_reaction_damage", "event_reaction_caution"}:
        return "watch_event_damaged"
    if reason_code == "market_stress_watch":
        return "watch_tape_blocked"
    return "watch_needs_confirmation"


def _display_confidence_score(
    *,
    raw_confidence: int,
    disposition: str,
    review_bucket: str,
    reason_code: str,
    recommended_action: str,
) -> int:
    raw = max(0, min(int(raw_confidence or 0), 100))
    normalized = max(0.0, min((raw - 50) / 35.0, 1.0))

    def _band(low: int, high: int) -> int:
        if high <= low:
            return low
        return int(round(low + (high - low) * normalized))

    if disposition == "promote":
        if review_bucket == "top_longs":
            return _band(64, 72)
        if review_bucket == "top_shorts":
            return _band(66, 74)
        return _band(62, 70)
    if disposition == "review_required":
        if review_bucket == "review_high_priority":
            return _band(56, 64)
        return _band(50, 58)
    if disposition == "defer":
        return _band(48, 56) if recommended_action in {"Buy", "Sell/Trim"} else _band(44, 52)
    if disposition == "blocked":
        return _band(18, 30)
    if review_bucket == "watch_event_damaged" or reason_code in {
        "event_buy_capped_to_watch",
        "event_reaction_damage",
        "event_reaction_caution",
    }:
        return _band(50, 58)
    if review_bucket == "watch_tape_blocked":
        return _band(56, 64)
    return _band(52, 60)


def build_review_rows(
    *,
    recommendation_rows: list[dict[str, Any]],
    now_utc: datetime,
    intraday_actionable: bool = True,
    symbol_metadata: dict[str, dict[str, Any]] | None = None,
    symbol_news_rows_by_symbol: dict[str, dict[str, Any]] | None = None,
    event_risk_rows_by_symbol: dict[str, dict[str, Any]] | None = None,
    market_row: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    symbol_metadata = symbol_metadata or {}
    symbol_news_rows_by_symbol = symbol_news_rows_by_symbol or {}
    event_risk_rows_by_symbol = event_risk_rows_by_symbol or {}
    market_stress = _market_stress_level(market_row)
    for row in recommendation_rows:
        if not isinstance(row, dict):
            continue
        scope_id = str(row.get("scope_id", "")).strip()
        if not scope_id:
            continue
        disposition, reason_code = _review_disposition(row, intraday_actionable=intraday_actionable)
        metadata = symbol_metadata.get(scope_id, {})
        symbol_news_row = symbol_news_rows_by_symbol.get(scope_id, {})
        event_risk_row = event_risk_rows_by_symbol.get(scope_id, {})
        sector = str(metadata.get("sector", "")).strip()
        direct_news = bool(metadata.get("direct_news", False))
        onboarding_stage = str(metadata.get("onboarding_stage", "")).strip()
        symbol_news_coverage = str(symbol_news_row.get("coverage_state", "")).strip().lower()
        sector_subtype = ""

        if sector == "Healthcare":
            sector_subtype = _healthcare_subtype(metadata)
            if direct_news and symbol_news_coverage in {"thin_evidence", "insufficient_evidence"}:
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code in {"regime_aligned_actionable", "mixed_strong_actionable"}:
                    reason_code = "healthcare_thin_evidence"
            elif disposition == "promote":
                if sector_subtype == "pharma_defensive":
                    reason_code = "healthcare_pharma_actionable"
                elif sector_subtype == "quality_tools_devices":
                    reason_code = "healthcare_tools_devices_actionable"
                elif sector_subtype == "managed_care_retail_health":
                    reason_code = "healthcare_managed_care_actionable"
                else:
                    reason_code = "healthcare_actionable"
            elif disposition == "review_required" and onboarding_stage == "modeled":
                reason_code = "healthcare_probationary_modeled"
        elif sector == "Industrials":
            sector_subtype = _industrials_subtype(metadata)
            if direct_news and symbol_news_coverage in {"thin_evidence", "insufficient_evidence"}:
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code in {"regime_aligned_actionable", "mixed_strong_actionable"}:
                    reason_code = "industrials_thin_evidence"
            elif disposition == "promote":
                if sector_subtype == "heavy_equipment_capex":
                    reason_code = "industrials_heavy_equipment_actionable"
                elif sector_subtype == "aerospace_defense":
                    reason_code = "industrials_aerospace_defense_actionable"
                elif sector_subtype == "diversified_industrials":
                    reason_code = "industrials_diversified_actionable"
                elif sector_subtype == "rails_logistics":
                    reason_code = "industrials_logistics_actionable"
                else:
                    reason_code = "industrials_actionable"
            elif disposition == "review_required" and onboarding_stage in {"modeled", "modeled_with_direct_news"}:
                reason_code = "industrials_probationary_modeled"
        elif sector == "Consumer Defensive":
            sector_subtype = _consumer_defensive_subtype(metadata)
            if direct_news and symbol_news_coverage in {"thin_evidence", "insufficient_evidence"}:
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code in {"regime_aligned_actionable", "mixed_strong_actionable"}:
                    reason_code = "consumer_defensive_thin_evidence"
            elif disposition == "promote":
                if sector_subtype == "membership_retail":
                    reason_code = "consumer_defensive_membership_actionable"
                elif sector_subtype == "household_personal_care":
                    reason_code = "consumer_defensive_household_actionable"
                elif sector_subtype == "staples_beverages":
                    reason_code = "consumer_defensive_beverages_actionable"
                elif sector_subtype == "defensive_brand_staples":
                    reason_code = "consumer_defensive_staples_actionable"
                else:
                    reason_code = "consumer_defensive_actionable"
            elif disposition == "review_required" and onboarding_stage in {"modeled", "modeled_with_direct_news"}:
                reason_code = "consumer_defensive_probationary_modeled"
        elif sector == "Communication Services":
            sector_subtype = _communication_services_subtype(metadata)
            if direct_news and symbol_news_coverage in {"thin_evidence", "insufficient_evidence"}:
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code in {"regime_aligned_actionable", "mixed_strong_actionable"}:
                    reason_code = "communication_services_thin_evidence"
            elif disposition == "promote":
                if sector_subtype == "internet_platforms":
                    reason_code = "communication_services_platform_actionable"
                elif sector_subtype == "streaming_media":
                    reason_code = "communication_services_streaming_actionable"
                elif sector_subtype == "media_entertainment":
                    reason_code = "communication_services_media_actionable"
                elif sector_subtype == "cable_broadband":
                    reason_code = "communication_services_cable_actionable"
                else:
                    reason_code = "communication_services_actionable"
            elif disposition == "review_required" and onboarding_stage in {"modeled", "modeled_with_direct_news"}:
                reason_code = "communication_services_probationary_modeled"
        elif sector == "Technology" and "semis" not in {
            str(role).strip().lower() for role in metadata.get("roles", []) if str(role).strip()
        }:
            sector_subtype = _technology_subtype(metadata)
            if direct_news and symbol_news_coverage in {"thin_evidence", "insufficient_evidence"}:
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code in {"regime_aligned_actionable", "mixed_strong_actionable"}:
                    reason_code = "technology_thin_evidence"
            elif disposition == "promote":
                if sector_subtype == "cloud_platforms":
                    reason_code = "technology_cloud_platform_actionable"
                elif sector_subtype == "enterprise_software":
                    reason_code = "technology_enterprise_software_actionable"
                elif sector_subtype == "networking_infrastructure":
                    reason_code = "technology_networking_actionable"
                elif sector_subtype == "consumer_hardware":
                    reason_code = "technology_consumer_hardware_actionable"
                elif sector_subtype == "ai_application_software":
                    reason_code = "technology_ai_application_actionable"
                else:
                    reason_code = "technology_actionable"
            elif disposition == "review_required" and onboarding_stage in {"modeled", "modeled_with_direct_news"}:
                reason_code = "technology_probationary_modeled"
        elif sector == "Energy":
            sector_subtype = _energy_subtype(metadata)
            if direct_news and symbol_news_coverage in {"thin_evidence", "insufficient_evidence"}:
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code in {"regime_aligned_actionable", "mixed_strong_actionable"}:
                    reason_code = "energy_thin_evidence"
            elif disposition == "promote":
                if sector_subtype == "integrated_majors":
                    reason_code = "energy_integrated_actionable"
                elif sector_subtype == "energy_services":
                    reason_code = "energy_services_actionable"
                elif sector_subtype == "upstream_ep":
                    reason_code = "energy_upstream_actionable"
                else:
                    reason_code = "energy_actionable"
            elif disposition == "review_required" and onboarding_stage in {"modeled", "modeled_with_direct_news"}:
                reason_code = "energy_probationary_modeled"

        event_action_bias = str(event_risk_row.get("action_bias", "")).strip().lower()
        event_reaction_state = str(event_risk_row.get("reaction_state", "")).strip().lower()
        event_reaction_severity = str(event_risk_row.get("reaction_severity", "")).strip().lower()
        event_hard_cap = bool(event_risk_row.get("hard_cap_buy_to_watch", False))
        if bool(event_risk_row.get("event_active", False)):
            if event_hard_cap and str(row.get("recommended_action", "")).strip() == "Buy":
                disposition = "watch"
                reason_code = "event_buy_capped_to_watch"
            elif event_action_bias == "downgrade" and event_reaction_severity == "high":
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code not in {"event_buy_capped_to_watch"}:
                    reason_code = "event_reaction_damage"
            elif event_action_bias == "downgrade" and event_reaction_severity == "medium":
                if disposition == "promote":
                    disposition = "review_required"
                if reason_code not in {"event_buy_capped_to_watch", "event_reaction_damage"}:
                    reason_code = "event_reaction_caution"

        recommended_action = str(row.get("recommended_action", "")).strip()
        recommendation_class = str(row.get("recommendation_class", "")).strip().lower()
        if recommended_action == "Buy" and disposition == "promote":
            if market_stress == "high":
                confidence = int(row.get("confidence_score", 0) or 0)
                if (
                    recommendation_class in {"mixed_strong_long", "mixed_weak_long", "aligned_long"}
                    and confidence >= 70
                ):
                    reason_code = "risk_off_survivor"
                else:
                    disposition = "watch"
                    reason_code = "market_stress_watch"
            elif market_stress == "medium" and int(row.get("confidence_score", 0) or 0) < 68:
                disposition = "watch"
                reason_code = "market_stress_watch"
        elif recommended_action == "Buy" and disposition == "review_required" and market_stress in {"medium", "high"}:
            if reason_code not in {
                "event_buy_capped_to_watch",
                "event_reaction_damage",
                "event_reaction_caution",
                "healthcare_thin_evidence",
                "industrials_thin_evidence",
                "industrials_probationary_modeled",
                "consumer_defensive_thin_evidence",
                "consumer_defensive_probationary_modeled",
                "communication_services_thin_evidence",
                "communication_services_probationary_modeled",
                "technology_thin_evidence",
                "technology_probationary_modeled",
                "energy_thin_evidence",
                "energy_probationary_modeled",
            }:
                confidence = int(row.get("confidence_score", 0) or 0)
                if market_stress == "high" and confidence >= 70 and str(row.get("recommended_horizon", "")).strip() in {"1to2w", "2to6w"}:
                    disposition = "promote"
                    reason_code = "risk_off_survivor"
                else:
                    disposition = "watch"
                    reason_code = "market_stress_watch"

        review_bucket = _review_bucket(row, disposition, reason_code)
        raw_confidence = int(row.get("confidence_score", 0) or 0)
        display_confidence = _display_confidence_score(
            raw_confidence=raw_confidence,
            disposition=disposition,
            review_bucket=review_bucket,
            reason_code=reason_code,
            recommended_action=str(row.get("recommended_action", "")).strip(),
        )
        rows.append(
            {
                "model_id": "recommendation_review_v1",
                "scope_id": scope_id,
                "symbol": scope_id,
                "recommended_action": row.get("recommended_action"),
                "recommended_horizon": row.get("recommended_horizon"),
                "recommendation_class": row.get("recommendation_class"),
                "evidence_balance_class": row.get("evidence_balance_class"),
                "regime_alignment": row.get("regime_alignment"),
                "signal_direction": row.get("signal_direction"),
                "confidence_score": raw_confidence,
                "display_confidence_score": display_confidence,
                "execution_ready": row.get("execution_ready"),
                "source_state": row.get("source_state"),
                "review_disposition": disposition,
                "review_bucket": review_bucket,
                "review_reason_code": reason_code,
                "event_active": bool(event_risk_row.get("event_active", False)),
                "event_reaction_state": event_reaction_state,
                "event_reaction_severity": event_reaction_severity,
                "market_stress_level": market_stress,
                "sector": sector,
                "sector_subtype": sector_subtype,
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
