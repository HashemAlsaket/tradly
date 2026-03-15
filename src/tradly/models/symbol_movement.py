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
RAW_SCORE_SCALE = 150.0
LIQUIDITY_STRONG_ADV20 = 50_000_000.0
LIQUIDITY_MIN_ADV20 = 10_000_000.0
HORIZON_TO_MARKET_LANE = {
    "1to3d": "near_term",
    "1to2w": "swing_term",
    "2to6w": "position_term",
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


def _drawdown_20(closes: list[float]) -> float:
    latest_close = closes[-1]
    peak_20 = max(closes[-20:])
    return 0.0 if peak_20 <= 0 else latest_close / peak_20 - 1.0


def _adv20(closes: list[float], volumes: list[float | None]) -> float:
    dollar_volume = []
    for close, volume in zip(closes[-20:], volumes[-20:]):
        if volume is None or volume <= 0:
            continue
        dollar_volume.append(close * volume)
    if not dollar_volume:
        return 0.0
    return sum(dollar_volume) / len(dollar_volume)


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


def build_symbol_movement_rows(
    *,
    bars_by_symbol: dict[str, list[Bar]],
    symbol_metadata: dict[str, dict[str, str]],
    market_regime_row: dict,
    sector_rows_by_scope: dict[str, dict],
    model_symbols: list[str],
    now_utc: datetime,
    market_overlay_fresh: bool,
    sector_overlay_fresh: bool,
) -> list[dict]:
    rows: list[dict] = []

    market_evidence = market_regime_row.get("evidence", {}) if isinstance(market_regime_row.get("evidence"), dict) else {}
    market_row_available = bool(market_regime_row)
    market_r20 = float(market_evidence.get("spy_r20", 0.0) or 0.0)
    market_lane_diagnostics = (
        market_regime_row.get("lane_diagnostics", {})
        if isinstance(market_regime_row.get("lane_diagnostics"), dict)
        else {}
    )
    expected_min_market_date = previous_trading_day(now_utc.astimezone(MARKET_TZ).date())
    calendar_row = build_trading_calendar_row(now_utc.astimezone(MARKET_TZ).date())
    current_market_session = market_session_state(now_utc)

    for symbol in model_symbols:
        metadata = symbol_metadata.get(symbol, {})
        asset_type = str(metadata.get("asset_type", "stock")).strip().lower() or "stock"
        sector = str(metadata.get("sector", "")).strip()
        bars = bars_by_symbol.get(symbol, [])
        why_code: list[str] = []
        evidence: dict[str, object] = {
            "symbol": symbol,
            "asset_type": asset_type,
            "sector": sector,
            "market_overlay_present": market_row_available,
            "market_overlay_fresh": market_overlay_fresh,
            "sector_overlay_fresh": sector_overlay_fresh,
        }

        if len(bars) < MIN_DAILY_BARS:
            rows.append(
                {
                    "model_id": "symbol_movement_v1",
                    "model_scope": "symbol",
                    "scope_id": symbol,
                    "horizon_primary": "1to3d",
                    "horizon_secondary": ["1to2w", "2to6w"],
                    "signal_direction": "neutral",
                    "signal_strength": 0.0,
                    "confidence_score": 20,
                    "confidence_label": "low",
                    "coverage_state": "insufficient_evidence",
                    "score_raw": 0.0,
                    "score_normalized": 0.0,
                    "why_code": ["symbol_bars_missing"],
                    "evidence": {
                        **evidence,
                        "symbol_bar_count": len(bars),
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
        volumes = [bar.volume for bar in bars]
        latest_market_date = _market_date_from_db_ts(latest.ts_utc)
        latest_market_date_ok = latest_market_date >= expected_min_market_date
        symbol_valid = latest_status in VALID_DATA_STATUS

        symbol_r20 = _r20(closes)
        symbol_r60 = _r60(closes)
        drawdown_20 = _drawdown_20(closes)
        returns_20 = _returns(closes[-21:])
        vol20 = pstdev(returns_20) if len(returns_20) >= 2 else 0.0
        adv20 = _adv20(closes, volumes)
        relative_vs_market = symbol_r20 - market_r20

        sector_row = sector_rows_by_scope.get(sector) if sector and sector in sector_rows_by_scope else None
        sector_overlay_present = asset_type == "stock" and sector_row is not None
        sector_evidence = sector_row.get("evidence", {}) if isinstance(sector_row, dict) else {}
        sector_proxy_r20 = (
            float(sector_evidence.get("sector_proxy_r20", 0.0) or 0.0)
            if isinstance(sector_evidence, dict)
            else 0.0
        )
        relative_vs_sector = symbol_r20 - sector_proxy_r20 if sector_overlay_present else 0.0

        trend_component = symbol_r20 * 700.0 + symbol_r60 * 350.0
        market_relative_component = relative_vs_market * 450.0
        sector_relative_component = relative_vs_sector * 350.0 if sector_overlay_present else 0.0
        if symbol_r20 > 0 and drawdown_20 >= -0.03:
            pullback_component = 10.0
        elif symbol_r20 > 0 and drawdown_20 >= -0.08:
            pullback_component = 4.0
        elif drawdown_20 <= -0.12:
            pullback_component = -10.0
        else:
            pullback_component = -2.0 if symbol_r20 < 0 else 0.0

        if adv20 >= LIQUIDITY_STRONG_ADV20:
            liquidity_component = 6.0
        elif adv20 >= LIQUIDITY_MIN_ADV20:
            liquidity_component = 2.0
        elif adv20 > 0:
            liquidity_component = -8.0
        else:
            liquidity_component = -15.0

        vol_penalty = max(0.0, vol20 - 0.03) * 900.0
        raw_score = (
            trend_component
            + market_relative_component
            + sector_relative_component
            + pullback_component
            + liquidity_component
            - vol_penalty
        )
        score_normalized = normalize_score(score_raw=raw_score, raw_scale=RAW_SCORE_SCALE)
        signal_strength = round(abs(score_normalized) / 100.0, 4)

        if score_normalized >= 15:
            signal_direction = "bullish"
            why_code.append("symbol_price_structure_positive")
        elif score_normalized <= -15:
            signal_direction = "bearish"
            why_code.append("symbol_price_structure_negative")
        else:
            signal_direction = "neutral"
            why_code.append("symbol_price_structure_mixed")

        if relative_vs_market > 0.02:
            why_code.append("outperforming_market_20d")
        elif relative_vs_market < -0.02:
            why_code.append("underperforming_market_20d")

        if sector_overlay_present and relative_vs_sector > 0.02:
            why_code.append("outperforming_sector_20d")
        elif sector_overlay_present and relative_vs_sector < -0.02:
            why_code.append("underperforming_sector_20d")

        if adv20 < LIQUIDITY_MIN_ADV20:
            why_code.append("adv20_below_minimum")
        if vol20 > 0.03:
            why_code.append("volatility_elevated")

        if (
            abs(symbol_r20) >= 0.025
            or abs(relative_vs_market) >= 0.025
            or (sector_overlay_present and abs(relative_vs_sector) >= 0.03)
        ):
            provisional_horizon_primary = "1to3d"
        elif (
            abs(symbol_r60) >= 0.06
            or (abs(symbol_r60) >= 0.04 and abs(symbol_r20) < 0.015)
            or (
                abs(symbol_r60) >= 0.03
                and abs(relative_vs_market) < 0.02
                and abs(drawdown_20) <= 0.05
                and abs(symbol_r60) > abs(symbol_r20)
            )
        ):
            provisional_horizon_primary = "2to6w"
        else:
            provisional_horizon_primary = "1to2w"
        provisional_horizon_secondary = [
            horizon for horizon in ("1to3d", "1to2w", "2to6w") if horizon != provisional_horizon_primary
        ]
        latency_assessment = assess_latency(
            data_status=latest_status,
            recency_ok=latest_market_date_ok,
            horizon=provisional_horizon_primary,
        )
        execution_ready = horizon_execution_ready(horizon=provisional_horizon_primary, now_utc=now_utc)
        for code in latency_assessment.why_code:
            if code not in why_code:
                why_code.append(code)
        calendar_reason = market_closed_reason_code(now_utc=now_utc)
        if not execution_ready and calendar_reason is not None:
            why_code.append(calendar_reason)

        market_lane_id = HORIZON_TO_MARKET_LANE[provisional_horizon_primary]
        market_lane = market_lane_diagnostics.get(market_lane_id, {}) if isinstance(market_lane_diagnostics, dict) else {}
        market_lane_present = bool(market_lane)
        market_lane_confidence = int(
            market_lane.get("confidence_score", market_regime_row.get("confidence_score", 25)) or 25
        )
        market_lane_coverage_state = str(
            market_lane.get("coverage_state", market_regime_row.get("coverage_state", "thin_evidence"))
        ).strip() or "thin_evidence"
        market_lane_data_freshness_ok = bool(
            market_lane.get("lane_data_freshness_ok", market_regime_row.get("data_freshness_ok", False))
        )
        sector_lane_diagnostics = (
            sector_row.get("lane_diagnostics", {})
            if isinstance(sector_row, dict) and isinstance(sector_row.get("lane_diagnostics"), dict)
            else {}
        )
        sector_lane_id = HORIZON_TO_MARKET_LANE[provisional_horizon_primary]
        sector_lane = sector_lane_diagnostics.get(sector_lane_id, {}) if isinstance(sector_lane_diagnostics, dict) else {}
        sector_lane_present = bool(sector_lane)
        sector_lane_confidence = int(sector_lane.get("confidence_score", 25) or 25)
        sector_lane_coverage_state = str(sector_lane.get("coverage_state", "thin_evidence")).strip() or "thin_evidence"
        sector_lane_data_freshness_ok = bool(sector_lane.get("lane_data_freshness_ok", False))

        if not symbol_valid or not market_row_available:
            coverage_state = "insufficient_evidence"
        elif not market_overlay_fresh:
            coverage_state = "thin_evidence"
            why_code.append("market_overlay_stale")
        elif asset_type == "stock" and sector_overlay_present and not sector_overlay_fresh:
            coverage_state = "thin_evidence"
            why_code.append("sector_overlay_stale")
        elif asset_type == "stock" and not sector_overlay_present:
            coverage_state = "thin_evidence"
            why_code.append("sector_overlay_missing")
        elif not latest_market_date_ok:
            coverage_state = "thin_evidence"
        else:
            coverage_state = "sufficient_evidence"

        if market_lane_coverage_state == "insufficient_evidence" and coverage_state != "insufficient_evidence":
            coverage_state = "insufficient_evidence"
            why_code.append("market_overlay_lane_insufficient")
        elif market_lane_coverage_state == "thin_evidence":
            why_code.append("market_overlay_lane_thin")
        if asset_type == "stock" and sector_overlay_present:
            if sector_lane_coverage_state == "insufficient_evidence" and coverage_state == "sufficient_evidence":
                coverage_state = "thin_evidence"
                why_code.append("sector_overlay_lane_insufficient")
            elif sector_lane_coverage_state == "thin_evidence":
                why_code.append("sector_overlay_lane_thin")

        coverage_state = _merge_forced_coverage_state(coverage_state, latency_assessment.forced_coverage_state)

        feature_signs = []
        for value in (symbol_r20, symbol_r60, relative_vs_market):
            if value > 0:
                feature_signs.append(1)
            elif value < 0:
                feature_signs.append(-1)
        if sector_overlay_present:
            if relative_vs_sector > 0:
                feature_signs.append(1)
            elif relative_vs_sector < 0:
                feature_signs.append(-1)

        pos = sum(1 for sign in feature_signs if sign > 0)
        neg = sum(1 for sign in feature_signs if sign < 0)
        feature_agreement_score = round((max(pos, neg) / (pos + neg)) * 100) if (pos + neg) else 0
        evidence_density_score = 100 if sector_overlay_present or asset_type != "stock" else 75
        freshness_score = 100 if latest_market_date_ok else 55
        freshness_score = apply_freshness_cap(freshness_score=freshness_score, assessment=latency_assessment)
        stability_score = round(clamp(100.0 - vol20 * 1200.0, 20.0, 100.0))
        if coverage_state == "sufficient_evidence":
            coverage_score = 100
        elif coverage_state == "thin_evidence":
            coverage_score = 49
        else:
            coverage_score = 25

        informative_feature_count = 4 if sector_overlay_present else 3
        independent_informative_feature_count = informative_feature_count
        confidence_score = compute_confidence(
            ConfidenceInputs(
                evidence_density_score=evidence_density_score,
                feature_agreement_score=feature_agreement_score,
                freshness_score=freshness_score,
                stability_score=stability_score,
                coverage_score=coverage_score,
                coverage_state=coverage_state,
                signal_strength=signal_strength,
                informative_feature_count=informative_feature_count,
                independent_informative_feature_count=independent_informative_feature_count,
            ),
            assessment=latency_assessment,
        )
        overlay_confidence_cap: int | None = None
        overlay_confidence_blend: int | None = None
        sector_overlay_confidence_cap: int | None = None
        sector_overlay_confidence_blend: int | None = None
        if market_lane_present:
            if market_lane_coverage_state == "insufficient_evidence":
                overlay_confidence_cap = market_lane_confidence
                confidence_score = min(confidence_score, overlay_confidence_cap)
            elif market_lane_coverage_state == "thin_evidence":
                overlay_confidence_blend = round(0.65 * confidence_score + 0.35 * market_lane_confidence)
                confidence_score = min(confidence_score, overlay_confidence_blend)
            else:
                overlay_confidence_cap = min(100, market_lane_confidence + 10)
                confidence_score = min(confidence_score, overlay_confidence_cap)
        if asset_type == "stock" and sector_overlay_present and sector_lane_present:
            if sector_lane_coverage_state == "insufficient_evidence":
                sector_overlay_confidence_cap = sector_lane_confidence
                confidence_score = min(confidence_score, sector_overlay_confidence_cap)
            elif sector_lane_coverage_state == "thin_evidence":
                sector_overlay_confidence_blend = round(0.75 * confidence_score + 0.25 * sector_lane_confidence)
                confidence_score = min(confidence_score, sector_overlay_confidence_blend)
            else:
                sector_overlay_confidence_cap = min(100, sector_lane_confidence + 10)
                confidence_score = min(confidence_score, sector_overlay_confidence_cap)

        horizon_primary = provisional_horizon_primary
        horizon_secondary = provisional_horizon_secondary

        evidence.update(
            {
                "latest_bar_utc": from_db_utc(latest.ts_utc).isoformat(),
                "latest_market_date": latest_market_date.isoformat(),
                "expected_min_market_date": expected_min_market_date.isoformat(),
                "market_calendar_state": calendar_row.market_calendar_state,
                "day_name": calendar_row.day_name,
                "last_cash_session_date": calendar_row.last_cash_session_date.isoformat(),
                "market_session_state": current_market_session,
                "latest_market_date_ok": latest_market_date_ok,
                "data_status": latest_status,
                "market_data_latency_minutes": latency_assessment.market_data_latency_minutes,
                "recency_ok": latest_market_date_ok,
                "latency_class": latency_assessment.latency_class,
                "symbol_r20": round(symbol_r20, 6),
                "symbol_r60": round(symbol_r60, 6),
                "drawdown_20": round(drawdown_20, 6),
                "vol20": round(vol20, 6),
                "adv20_dollar_volume": round(adv20, 2),
                "relative_vs_market_20d": round(relative_vs_market, 6),
                "market_regime_direction": market_regime_row.get("signal_direction", "unknown"),
                "market_regime_score": market_regime_row.get("score_normalized", 0.0),
                "market_regime_lane_id": market_lane_id,
                "market_regime_lane_present": market_lane_present,
                "market_regime_lane_confidence": market_lane_confidence,
                "market_regime_lane_coverage_state": market_lane_coverage_state,
                "market_regime_lane_data_freshness_ok": market_lane_data_freshness_ok,
                "sector_overlay_present": sector_overlay_present,
                "sector_overlay_lane_id": sector_lane_id if sector_overlay_present else None,
                "sector_overlay_lane_present": sector_lane_present,
                "sector_overlay_lane_confidence": sector_lane_confidence if sector_overlay_present else None,
                "sector_overlay_lane_coverage_state": sector_lane_coverage_state if sector_overlay_present else None,
                "sector_overlay_lane_data_freshness_ok": sector_lane_data_freshness_ok if sector_overlay_present else None,
                "market_overlay_fresh": market_overlay_fresh,
                "sector_overlay_fresh": sector_overlay_fresh,
                "sector_relative_20d": round(relative_vs_sector, 6) if sector_overlay_present else None,
                "sector_score": sector_row.get("score_normalized") if isinstance(sector_row, dict) else None,
                "evidence_density_score": evidence_density_score,
                "feature_agreement_score": feature_agreement_score,
                "freshness_score": freshness_score,
                "stability_score": stability_score,
                "coverage_score": coverage_score,
                "execution_ready": execution_ready,
            }
        )

        rows.append(
            {
                "model_id": "symbol_movement_v1",
                "model_scope": "symbol",
                "scope_id": symbol,
                "horizon_primary": horizon_primary,
                "horizon_secondary": horizon_secondary,
                "signal_direction": signal_direction,
                "signal_strength": signal_strength,
                "confidence_score": confidence_score,
                "confidence_label": confidence_label(confidence_score),
                "coverage_state": coverage_state,
                "score_raw": round(raw_score, 4),
                "score_normalized": round(score_normalized, 4),
                "why_code": why_code,
                "diagnostics": {
                    "normalization": {
                        "raw_score": round(raw_score, 4),
                        "raw_scale": RAW_SCORE_SCALE,
                        "normalized_score": round(score_normalized, 4),
                    },
                    "latency_assessment": latency_assessment.to_dict(),
                    "overlay_alignment": {
                        "market_overlay_present": market_row_available,
                        "market_overlay_fresh": market_overlay_fresh,
                        "market_overlay_lane_id": market_lane_id,
                        "market_overlay_lane_present": market_lane_present,
                        "market_overlay_lane_confidence": market_lane_confidence,
                        "market_overlay_lane_coverage_state": market_lane_coverage_state,
                        "market_session_state": current_market_session,
                        "execution_ready": execution_ready,
                        "sector_overlay_present": sector_overlay_present,
                        "sector_overlay_lane_id": sector_lane_id if sector_overlay_present else None,
                        "sector_overlay_lane_present": sector_lane_present,
                        "sector_overlay_lane_confidence": sector_lane_confidence if sector_overlay_present else None,
                        "sector_overlay_lane_coverage_state": sector_lane_coverage_state if sector_overlay_present else None,
                        "sector_overlay_fresh": sector_overlay_fresh,
                    },
                    "confidence_inputs": {
                        "evidence_density_score": evidence_density_score,
                        "feature_agreement_score": feature_agreement_score,
                        "freshness_score": freshness_score,
                        "stability_score": stability_score,
                        "coverage_score": coverage_score,
                        "coverage_state": coverage_state,
                        "signal_strength": signal_strength,
                        "informative_feature_count": informative_feature_count,
                        "independent_informative_feature_count": independent_informative_feature_count,
                    },
                    "cap_reasons": _confidence_cap_reasons(
                        coverage_state=coverage_state,
                        signal_strength=signal_strength,
                        informative_feature_count=informative_feature_count,
                        independent_informative_feature_count=independent_informative_feature_count,
                        latency_confidence_cap=latency_assessment.confidence_cap,
                    )
                    + (
                        [f"market_overlay_confidence_blend_{overlay_confidence_blend}"]
                        if overlay_confidence_blend is not None
                        else []
                    )
                    + (
                        [f"market_overlay_confidence_cap_{overlay_confidence_cap}"]
                        if overlay_confidence_cap is not None
                        else []
                    )
                    + (
                        [
                            f"market_overlay_confidence_reference_{market_lane_confidence}"
                        ]
                        if market_lane_present
                        else []
                    )
                    + (
                        [f"sector_overlay_confidence_blend_{sector_overlay_confidence_blend}"]
                        if sector_overlay_confidence_blend is not None
                        else []
                    )
                    + (
                        [f"sector_overlay_confidence_cap_{sector_overlay_confidence_cap}"]
                        if sector_overlay_confidence_cap is not None
                        else []
                    )
                    + (
                        [
                            f"sector_overlay_confidence_reference_{sector_lane_confidence}"
                        ]
                        if asset_type == "stock" and sector_overlay_present and sector_lane_present
                        else []
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
                "data_freshness_ok": latest_market_date_ok and symbol_valid,
                "execution_ready": execution_ready,
            }
        )

    return rows
