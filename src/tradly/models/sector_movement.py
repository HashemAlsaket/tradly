from __future__ import annotations

import json
from datetime import datetime
from statistics import pstdev
from zoneinfo import ZoneInfo

from tradly.models.calibration import (
    ConfidenceInputs,
    apply_freshness_cap,
    assess_latency,
    compute_confidence,
    confidence_label,
    normalize_score,
)
from tradly.models.market_regime import Bar, clamp
from tradly.services.db_time import from_db_utc
from tradly.services.market_calendar import (
    build_trading_calendar_row,
    horizon_execution_ready,
    market_closed_reason_code,
    market_session_state,
    previous_trading_day,
)


MARKET_TZ = ZoneInfo("America/New_York")
MIN_DAILY_BARS = 61
VALID_DATA_STATUS = {"REALTIME", "DELAYED"}
RAW_SCORE_SCALE = 140.0
LANE_TO_HORIZON = {
    "near_term": "1to3d",
    "swing_term": "1to2w",
    "position_term": "2to6w",
}
BROAD_MARKET_PROXIES = ("SPY", "QQQ", "IWM", "DIA", "VTI")
CANONICAL_SECTOR_PROXIES = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Energy": "XLE",
    "Healthcare": "XLV",
    "Industrials": "XLI",
    "Basic Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Consumer Defensive": "XLP",
    "Consumer Cyclical": "XLY",
    "Communication Services": "XLC",
}


def _market_date_from_db_ts(ts: datetime) -> datetime.date:
    return from_db_utc(ts).astimezone(MARKET_TZ).date()


def _returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for idx in range(1, len(closes)):
        prev = closes[idx - 1]
        curr = closes[idx]
        out.append(0.0 if prev <= 0 else curr / prev - 1.0)
    return out


def _r20(closes: list[float]) -> float:
    return closes[-1] / closes[-21] - 1.0


def _r60(closes: list[float]) -> float:
    return closes[-1] / closes[-61] - 1.0


def _load_sector_members(scope_manifest_path) -> dict[str, list[str]]:
    payload = json.loads(scope_manifest_path.read_text(encoding="utf-8"))
    return payload["groupings"]["by_sector"]


def _confidence_cap_reasons(
    *,
    coverage_state: str,
    signal_strength: float,
    informative_feature_count: int,
    independent_informative_feature_count: int,
    latency_confidence_cap: int | None,
) -> list[str]:
    reasons: list[str] = []
    if coverage_state == "thin_evidence":
        reasons.append("coverage_thin_evidence_cap_49")
    elif coverage_state == "insufficient_evidence":
        reasons.append("coverage_insufficient_evidence_cap_25")

    if signal_strength < 0.05:
        reasons.append("signal_strength_cap_54")
    elif signal_strength < 0.10:
        reasons.append("signal_strength_cap_60")
    elif signal_strength < 0.20:
        reasons.append("signal_strength_cap_60")

    if informative_feature_count <= 1:
        reasons.append("informative_feature_count_cap_65")
    elif independent_informative_feature_count < 3:
        reasons.append("independent_informative_feature_count_cap_85")

    if latency_confidence_cap is not None:
        reasons.append(f"latency_confidence_cap_{latency_confidence_cap}")

    return reasons


def _merge_forced_coverage_state(base_state: str, forced_state: str | None) -> str:
    if forced_state is None:
        return base_state
    forced_priority = {"insufficient_evidence": 0, "thin_evidence": 1, "sufficient_evidence": 2}
    if forced_priority[forced_state] < forced_priority[base_state]:
        return forced_state
    return base_state


def build_sector_movement_rows(
    *,
    bars_by_symbol: dict[str, list[Bar]],
    now_utc: datetime,
    sector_members: dict[str, list[str]],
) -> list[dict]:
    rows: list[dict] = []

    broad_proxy_metrics: dict[str, dict[str, float | datetime.date]] = {}
    missing_broad_proxies: list[str] = []
    for symbol in BROAD_MARKET_PROXIES:
        bars = bars_by_symbol.get(symbol, [])
        if len(bars) < MIN_DAILY_BARS:
            missing_broad_proxies.append(symbol)
            continue
        latest = bars[-1]
        if (latest.data_status or "").upper() not in VALID_DATA_STATUS:
            missing_broad_proxies.append(symbol)
            continue
        closes = [bar.close for bar in bars]
        broad_proxy_metrics[symbol] = {
            "r20": _r20(closes),
            "r60": _r60(closes),
            "latest_market_date": _market_date_from_db_ts(latest.ts_utc),
        }

    expected_min_market_date = previous_trading_day(now_utc.astimezone(MARKET_TZ).date())
    calendar_row = build_trading_calendar_row(now_utc.astimezone(MARKET_TZ).date())
    current_market_session = market_session_state(now_utc)
    broad_latest_ok = bool(broad_proxy_metrics) and all(
        metric["latest_market_date"] >= expected_min_market_date for metric in broad_proxy_metrics.values()
    )
    broad_r20_avg = (
        sum(float(metric["r20"]) for metric in broad_proxy_metrics.values()) / len(broad_proxy_metrics)
        if broad_proxy_metrics
        else 0.0
    )
    broad_r60_avg = (
        sum(float(metric["r60"]) for metric in broad_proxy_metrics.values()) / len(broad_proxy_metrics)
        if broad_proxy_metrics
        else 0.0
    )

    for sector, proxy_symbol in CANONICAL_SECTOR_PROXIES.items():
        member_symbols = sector_members.get(sector, [])
        bars = bars_by_symbol.get(proxy_symbol, [])
        why_code: list[str] = []
        evidence: dict[str, object] = {
            "sector_proxy": proxy_symbol,
            "member_symbols": member_symbols,
            "broad_market_proxies": list(BROAD_MARKET_PROXIES),
            "missing_broad_market_proxies": missing_broad_proxies,
        }

        if len(bars) < MIN_DAILY_BARS:
            rows.append(
                {
                    "model_id": "sector_movement_v1",
                    "model_scope": "sector",
                    "scope_id": sector,
                    "horizon_primary": "1to3d",
                    "horizon_secondary": ["1to2w", "2to6w"],
                    "signal_direction": "neutral",
                    "signal_strength": 0.0,
                    "confidence_score": 20,
                    "confidence_label": "low",
                    "coverage_state": "insufficient_evidence",
                    "score_raw": 0.0,
                    "score_normalized": 0.0,
                    "why_code": ["sector_proxy_missing"],
                    "evidence": {
                        **evidence,
                        "sector_proxy_present": False,
                        "required_min_daily_bars": MIN_DAILY_BARS,
                    },
                    "as_of_utc": now_utc.isoformat(),
                    "data_freshness_ok": False,
                }
            )
            continue

        latest = bars[-1]
        latest_status = (latest.data_status or "").upper()
        closes = [bar.close for bar in bars]
        sector_latest_market_date = _market_date_from_db_ts(latest.ts_utc)
        sector_latest_ok = sector_latest_market_date >= expected_min_market_date
        sector_proxy_valid = latest_status in VALID_DATA_STATUS
        r20 = _r20(closes)
        r60 = _r60(closes)
        relative_r20 = r20 - broad_r20_avg
        relative_r60 = r60 - broad_r60_avg
        returns_20 = _returns(closes[-21:])
        vol20 = pstdev(returns_20) if len(returns_20) >= 2 else 0.0

        raw_score = relative_r20 * 900.0 + relative_r60 * 450.0 - max(0.0, vol20 - 0.02) * 500.0
        score_normalized = normalize_score(score_raw=raw_score, raw_scale=RAW_SCORE_SCALE)
        signal_strength = round(abs(score_normalized) / 100.0, 4)

        if score_normalized >= 15:
            signal_direction = "bullish"
            why_code.append("sector_relative_strength_positive")
        elif score_normalized <= -15:
            signal_direction = "bearish"
            why_code.append("sector_relative_strength_negative")
        else:
            signal_direction = "neutral"
            why_code.append("sector_relative_strength_mixed")

        if r20 > broad_r20_avg:
            why_code.append("sector_outperforming_20d")
        elif r20 < broad_r20_avg:
            why_code.append("sector_underperforming_20d")

        if not sector_proxy_valid:
            why_code.append("sector_proxy_invalid_status")
        if not sector_latest_ok:
            why_code.append("sector_proxy_stale")
        if not broad_latest_ok:
            why_code.append("broad_market_proxy_stale")

        evidence_density_score = round((1 + len(broad_proxy_metrics)) / (1 + len(BROAD_MARKET_PROXIES)) * 100)
        feature_signs = []
        for value in (relative_r20, relative_r60):
            if value > 0:
                feature_signs.append(1)
            elif value < 0:
                feature_signs.append(-1)
        pos = sum(1 for sign in feature_signs if sign > 0)
        neg = sum(1 for sign in feature_signs if sign < 0)
        feature_agreement_score = round((max(pos, neg) / (pos + neg)) * 100) if (pos + neg) else 0
        stability_score = round(clamp(100.0 - vol20 * 1500.0, 20.0, 100.0))

        base_coverage_state = (
            "insufficient_evidence"
            if (not sector_proxy_valid or len(broad_proxy_metrics) < len(BROAD_MARKET_PROXIES))
            else "thin_evidence"
            if (not sector_latest_ok or not broad_latest_ok)
            else "sufficient_evidence"
        )

        def _build_lane(lane_id: str) -> dict[str, object]:
            horizon = LANE_TO_HORIZON[lane_id]
            lane_execution_ready = horizon_execution_ready(horizon=horizon, now_utc=now_utc)
            assessment = assess_latency(
                data_status=latest_status,
                recency_ok=sector_latest_ok and broad_latest_ok,
                horizon=horizon,  # type: ignore[arg-type]
            )
            coverage_state = _merge_forced_coverage_state(base_coverage_state, assessment.forced_coverage_state)
            lane_why_code = list(why_code)
            if lane_id == "position_term" and sector_proxy_valid and broad_proxy_metrics:
                if base_coverage_state == "insufficient_evidence":
                    coverage_state = "thin_evidence"
            freshness_score = 100 if sector_latest_ok and broad_latest_ok else 60
            freshness_score = apply_freshness_cap(freshness_score=freshness_score, assessment=assessment)
            if lane_id == "position_term" and sector_proxy_valid and broad_proxy_metrics:
                if not sector_latest_ok:
                    freshness_score = min(max(freshness_score, 90), 90)
                    lane_why_code.append("sector_proxy_slow_ok")
                if not broad_latest_ok:
                    freshness_score = min(max(freshness_score, 85), 85)
                    lane_why_code.append("broad_market_proxy_slow_ok")
            if coverage_state == "sufficient_evidence":
                coverage_score = 100
            elif coverage_state == "thin_evidence":
                coverage_score = 49
            else:
                coverage_score = 25

            confidence_score = compute_confidence(
                ConfidenceInputs(
                    evidence_density_score=evidence_density_score,
                    feature_agreement_score=feature_agreement_score,
                    freshness_score=freshness_score,
                    stability_score=stability_score,
                    coverage_score=coverage_score,
                    coverage_state=coverage_state,
                    signal_strength=signal_strength,
                    informative_feature_count=2,
                    independent_informative_feature_count=2,
                ),
                assessment=assessment,
            )

            for code in assessment.why_code:
                if code not in lane_why_code:
                    lane_why_code.append(code)
            calendar_reason = market_closed_reason_code(now_utc=now_utc)
            if not lane_execution_ready and calendar_reason is not None:
                lane_why_code.append(calendar_reason)
            if coverage_state != "sufficient_evidence" and "market_data_stale" not in lane_why_code:
                lane_why_code.append("market_data_stale")

            return {
                "lane_id": lane_id,
                "canonical_horizon": horizon,
                "confidence_score": confidence_score,
                "confidence_label": confidence_label(confidence_score),
                "coverage_state": coverage_state,
                "freshness_score": freshness_score,
                "coverage_score": coverage_score,
                "latency_assessment": assessment.to_dict(),
                "why_code": lane_why_code,
                "market_session_state": current_market_session,
                "lane_execution_ready": lane_execution_ready,
                "lane_data_freshness_ok": (
                    sector_proxy_valid
                    and (
                        (lane_id != "position_term" and sector_latest_ok and broad_latest_ok)
                        or (lane_id == "position_term" and bool(broad_proxy_metrics))
                    )
                    and coverage_state == "sufficient_evidence"
                ),
            }

        lane_diagnostics = {
            "near_term": _build_lane("near_term"),
            "swing_term": _build_lane("swing_term"),
            "position_term": _build_lane("position_term"),
        }
        near_term_lane = lane_diagnostics["near_term"]
        swing_term_lane = lane_diagnostics["swing_term"]
        position_term_lane = lane_diagnostics["position_term"]
        if (
            near_term_lane["coverage_state"] != "sufficient_evidence"
            and swing_term_lane["coverage_state"] == "sufficient_evidence"
        ):
            primary_lane_id = "swing_term"
        elif (
            near_term_lane["coverage_state"] != "sufficient_evidence"
            and swing_term_lane["coverage_state"] != "sufficient_evidence"
            and position_term_lane["coverage_state"] == "sufficient_evidence"
        ):
            primary_lane_id = "position_term"
        elif abs(relative_r20) >= 0.02:
            primary_lane_id = "near_term"
        elif abs(relative_r60) >= 0.03:
            primary_lane_id = "position_term"
        else:
            primary_lane_id = "swing_term"
        secondary_lane_ids = [lane_id for lane_id in ("near_term", "swing_term", "position_term") if lane_id != primary_lane_id]
        primary_lane = lane_diagnostics[primary_lane_id]

        confidence_score = int(primary_lane["confidence_score"])
        coverage_state = str(primary_lane["coverage_state"])
        freshness_score = int(primary_lane["freshness_score"])
        coverage_score = int(primary_lane["coverage_score"])
        latency_details = primary_lane["latency_assessment"]
        sector_latency_minutes = int(latency_details["market_data_latency_minutes"])
        latency_class = str(latency_details["latency_class"])
        why_code = list(primary_lane["why_code"])

        horizon_primary = str(primary_lane["canonical_horizon"])
        horizon_secondary = [str(lane_diagnostics[lane_id]["canonical_horizon"]) for lane_id in secondary_lane_ids]

        evidence.update(
            {
                "sector_proxy_present": True,
                "latest_bar_utc": from_db_utc(latest.ts_utc).isoformat(),
                "sector_proxy_latest_market_date": sector_latest_market_date.isoformat(),
                "expected_min_market_date": expected_min_market_date.isoformat(),
                "market_calendar_state": calendar_row.market_calendar_state,
                "day_name": calendar_row.day_name,
                "last_cash_session_date": calendar_row.last_cash_session_date.isoformat(),
                "market_session_state": current_market_session,
                "sector_proxy_latest_ok": sector_latest_ok,
                "sector_proxy_status": latest_status,
                "data_status": latest_status,
                "market_data_latency_minutes": sector_latency_minutes,
                "recency_ok": sector_latest_ok and broad_latest_ok,
                "latency_class": latency_class,
                "sector_proxy_r20": round(r20, 6),
                "sector_proxy_r60": round(r60, 6),
                "broad_market_r20_avg": round(broad_r20_avg, 6),
                "broad_market_r60_avg": round(broad_r60_avg, 6),
                "relative_r20": round(relative_r20, 6),
                "relative_r60": round(relative_r60, 6),
                "vol20": round(vol20, 6),
                "evidence_density_score": evidence_density_score,
                "feature_agreement_score": feature_agreement_score,
                "freshness_score": freshness_score,
                "stability_score": stability_score,
                "coverage_score": coverage_score,
                "lane_diagnostics": lane_diagnostics,
            }
        )

        rows.append(
            {
                "model_id": "sector_movement_v1",
                "model_scope": "sector",
                "scope_id": sector,
                "horizon_primary": horizon_primary,
                "horizon_secondary": horizon_secondary,
                "lane_primary": primary_lane_id,
                "lane_secondary": secondary_lane_ids,
                "signal_direction": signal_direction,
                "signal_strength": signal_strength,
                "confidence_score": confidence_score,
                "confidence_label": confidence_label(confidence_score),
                "coverage_state": coverage_state,
                "score_raw": round(raw_score, 4),
                "score_normalized": round(score_normalized, 4),
                "why_code": why_code,
                "lane_diagnostics": lane_diagnostics,
                "diagnostics": {
                    "normalization": {
                        "raw_score": round(raw_score, 4),
                        "raw_scale": RAW_SCORE_SCALE,
                        "normalized_score": round(score_normalized, 4),
                    },
                    "latency_assessment": latency_details,
                    "confidence_inputs": {
                        "evidence_density_score": evidence_density_score,
                        "feature_agreement_score": feature_agreement_score,
                        "freshness_score": freshness_score,
                        "stability_score": stability_score,
                        "coverage_score": coverage_score,
                        "coverage_state": coverage_state,
                        "signal_strength": signal_strength,
                        "informative_feature_count": 2,
                        "independent_informative_feature_count": 2,
                    },
                    "cap_reasons": _confidence_cap_reasons(
                        coverage_state=coverage_state,
                        signal_strength=signal_strength,
                        informative_feature_count=2,
                        independent_informative_feature_count=2,
                        latency_confidence_cap=latency_details.get("confidence_cap"),
                    ),
                    "audit_flags": {
                        "score_extreme_without_exception": abs(score_normalized) >= 95,
                        "confidence_90_plus": confidence_score >= 90,
                        "weak_score_high_confidence": (
                            (abs(score_normalized) < 5 and confidence_score >= 55)
                            or (abs(score_normalized) < 10 and confidence_score >= 70)
                        ),
                    },
                },
                "evidence": evidence,
                "as_of_utc": now_utc.isoformat(),
                "data_freshness_ok": bool(primary_lane["lane_data_freshness_ok"]),
                "execution_ready": bool(primary_lane["lane_execution_ready"]),
            }
        )

    return rows
