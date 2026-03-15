from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import sqrt
from statistics import pstdev

from tradly.models.calibration import (
    ConfidenceInputs,
    apply_freshness_cap,
    assess_latency,
    compute_confidence,
    confidence_label,
    normalize_score,
)
from tradly.services.db_time import from_db_utc
from tradly.services.market_calendar import (
    MARKET_TZ,
    build_trading_calendar_row,
    horizon_execution_ready,
    market_closed_reason_code,
    market_session_state,
)


MIN_BARS_NEAR = 20
MIN_BARS_SWING = 35
MIN_BARS_POSITION = 63
RAW_SCORE_SCALE = 60.0
LANE_CONFIG = {
    "near_term": {"horizon": "1to3d", "days_forward": 3, "min_bars": MIN_BARS_NEAR},
    "swing_term": {"horizon": "1to2w", "days_forward": 10, "min_bars": MIN_BARS_SWING},
    "position_term": {"horizon": "2to6w", "days_forward": 30, "min_bars": MIN_BARS_POSITION},
}


@dataclass(frozen=True)
class DailyBar:
    ts_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    data_status: str | None


def _returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for idx in range(1, len(closes)):
        prev = closes[idx - 1]
        curr = closes[idx]
        out.append(0.0 if prev <= 0 else curr / prev - 1.0)
    return out


def _true_ranges(bars: list[DailyBar]) -> list[float]:
    out: list[float] = []
    prev_close = bars[0].close
    for bar in bars[1:]:
        tr = max(
            bar.high - bar.low,
            abs(bar.high - prev_close),
            abs(bar.low - prev_close),
        )
        out.append(max(0.0, tr))
        prev_close = bar.close
    return out


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _lane_output(
    *,
    lane_id: str,
    bars: list[DailyBar],
    latest_status: str,
    latest_market_date_ok: bool,
    now_utc: datetime,
) -> dict[str, object]:
    config = LANE_CONFIG[lane_id]
    horizon = str(config["horizon"])
    days_forward = int(config["days_forward"])
    min_bars = int(config["min_bars"])
    canonical_horizon = horizon
    lane_execution_ready = horizon_execution_ready(horizon=horizon, now_utc=now_utc)
    current_market_session = market_session_state(now_utc)
    calendar_reason = market_closed_reason_code(now_utc=now_utc)

    if len(bars) < min_bars:
        why_code = ["range_history_missing"]
        if not lane_execution_ready and calendar_reason is not None:
            why_code.append(calendar_reason)
        return {
            "lane_id": lane_id,
            "canonical_horizon": canonical_horizon,
            "confidence_score": 20,
            "confidence_label": "low",
            "coverage_state": "insufficient_evidence",
            "freshness_score": 25,
            "coverage_score": 25,
            "why_code": why_code,
            "lane_data_freshness_ok": False,
            "lane_execution_ready": lane_execution_ready,
            "market_session_state": current_market_session,
            "bar_count": len(bars),
            "expected_move_pct": None,
            "expected_move_abs": None,
            "upper_bound": None,
            "lower_bound": None,
            "score_raw": 0.0,
            "score_normalized": 0.0,
            "signal_direction": "neutral",
            "signal_strength": 0.0,
        }

    closes = [bar.close for bar in bars]
    tr_values = _true_ranges(bars)
    close_now = closes[-1]
    atr14 = _avg(tr_values[-14:]) if len(tr_values) >= 14 else _avg(tr_values)
    atr63 = _avg(tr_values[-63:]) if len(tr_values) >= 63 else _avg(tr_values)
    atr14_pct = 0.0 if close_now <= 0 else atr14 / close_now
    atr63_pct = 0.0 if close_now <= 0 else atr63 / close_now

    returns20 = _returns(closes[-21:])
    returns63 = _returns(closes[-64:])
    cc_vol20 = pstdev(returns20) if len(returns20) >= 2 else 0.0
    cc_vol63 = pstdev(returns63) if len(returns63) >= 2 else 0.0

    move_pct_atr = atr14_pct * sqrt(days_forward)
    move_pct_cc = (cc_vol20 if lane_id != "position_term" else max(cc_vol20, cc_vol63)) * sqrt(days_forward)
    expected_move_pct = max(move_pct_atr, move_pct_cc)
    baseline_pct = max(atr63_pct * sqrt(days_forward), cc_vol63 * sqrt(days_forward), 0.005)

    raw_score = ((expected_move_pct / baseline_pct) - 1.0) * 80.0
    score_normalized = normalize_score(score_raw=raw_score, raw_scale=RAW_SCORE_SCALE)
    signal_strength = round(abs(score_normalized) / 100.0, 4)

    latency_assessment = assess_latency(
        data_status=latest_status,
        recency_ok=latest_market_date_ok,
        horizon=horizon,
    )

    if len(bars) >= min_bars and latest_status in {"REALTIME", "DELAYED"}:
        coverage_state = "sufficient_evidence"
        coverage_score = 100
    else:
        coverage_state = "thin_evidence"
        coverage_score = 49
    if latency_assessment.forced_coverage_state == "thin_evidence":
        coverage_state = "thin_evidence"
        coverage_score = 49
    elif latency_assessment.forced_coverage_state == "insufficient_evidence":
        coverage_state = "insufficient_evidence"
        coverage_score = 25

    freshness_score = 100 if latest_market_date_ok else 55
    freshness_score = apply_freshness_cap(freshness_score=freshness_score, assessment=latency_assessment)

    disagreement = 0.0
    if max(move_pct_atr, move_pct_cc) > 0:
        disagreement = abs(move_pct_atr - move_pct_cc) / max(move_pct_atr, move_pct_cc)
    feature_agreement_score = round(max(0.0, 100.0 - disagreement * 100.0))

    tr_vol = pstdev(tr_values[-20:]) if len(tr_values[-20:]) >= 2 else 0.0
    tr_level = _avg(tr_values[-20:]) or 1.0
    tr_instability = min(1.0, tr_vol / tr_level) if tr_level > 0 else 1.0
    stability_score = round(max(25.0, 100.0 - tr_instability * 70.0))
    evidence_density_score = min(100, round(len(bars) / min_bars * 100))

    confidence_score = compute_confidence(
        ConfidenceInputs(
            evidence_density_score=evidence_density_score,
            feature_agreement_score=feature_agreement_score,
            freshness_score=freshness_score,
            stability_score=stability_score,
            coverage_score=coverage_score,
            coverage_state=coverage_state,
            signal_strength=signal_strength,
            informative_feature_count=3,
            independent_informative_feature_count=3,
        ),
        assessment=latency_assessment,
    )
    if signal_strength < 0.10:
        subtle_cap = 45 + round(feature_agreement_score * 0.15)
        confidence_score = min(confidence_score, subtle_cap)

    why_code: list[str] = []
    if score_normalized >= 20:
        why_code.append("expected_range_expanding")
    elif score_normalized <= -20:
        why_code.append("expected_range_contracting")
    else:
        why_code.append("expected_range_stable")
    if signal_strength < 0.10:
        why_code.append("range_regime_subtle")
    if abs(move_pct_atr - move_pct_cc) / max(move_pct_atr, move_pct_cc, 1e-9) > 0.35:
        why_code.append("range_methods_disagree")
    for code in latency_assessment.why_code:
        if code not in why_code:
            why_code.append(code)
    if not lane_execution_ready and calendar_reason is not None:
        why_code.append(calendar_reason)

    expected_move_abs = close_now * expected_move_pct
    upper_bound = close_now + expected_move_abs
    lower_bound = max(0.0, close_now - expected_move_abs)

    return {
        "lane_id": lane_id,
        "canonical_horizon": canonical_horizon,
        "confidence_score": confidence_score,
        "confidence_label": confidence_label(confidence_score),
        "coverage_state": coverage_state,
        "freshness_score": freshness_score,
        "coverage_score": coverage_score,
        "why_code": why_code,
        "lane_data_freshness_ok": freshness_score >= 70,
        "lane_execution_ready": lane_execution_ready,
        "market_session_state": current_market_session,
        "bar_count": len(bars),
        "expected_move_pct": round(expected_move_pct * 100.0, 4),
        "expected_move_abs": round(expected_move_abs, 4),
        "upper_bound": round(upper_bound, 4),
        "lower_bound": round(lower_bound, 4),
        "score_raw": round(raw_score, 4),
        "score_normalized": round(score_normalized, 4),
        "signal_direction": "neutral",
        "signal_strength": signal_strength,
        "atr14_pct": round(atr14_pct * 100.0, 4),
        "atr63_pct": round(atr63_pct * 100.0, 4),
        "cc_vol20_pct": round(cc_vol20 * 100.0, 4),
        "cc_vol63_pct": round(cc_vol63 * 100.0, 4),
        "feature_agreement_score": feature_agreement_score,
        "stability_score": stability_score,
        "evidence_density_score": evidence_density_score,
    }


def build_range_expectation_rows(
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    symbol_metadata: dict[str, dict[str, str]],
    model_symbols: list[str],
    now_utc: datetime,
    expected_min_market_date,
) -> list[dict]:
    rows: list[dict] = []
    calendar_row = build_trading_calendar_row(now_utc.astimezone(MARKET_TZ).date())
    current_market_session = market_session_state(now_utc)

    for symbol in model_symbols:
        metadata = symbol_metadata.get(symbol, {})
        asset_type = str(metadata.get("asset_type", "stock")).strip().lower() or "stock"
        sector = str(metadata.get("sector", "")).strip()
        bars = bars_by_symbol.get(symbol, [])
        evidence: dict[str, object] = {
            "symbol": symbol,
            "asset_type": asset_type,
            "sector": sector,
            "symbol_bar_count": len(bars),
            "required_bar_thresholds": {
                "near_term": MIN_BARS_NEAR,
                "swing_term": MIN_BARS_SWING,
                "position_term": MIN_BARS_POSITION,
            },
            "market_calendar_state": calendar_row.market_calendar_state,
            "day_name": calendar_row.day_name,
            "last_cash_session_date": calendar_row.last_cash_session_date.isoformat(),
            "market_session_state": current_market_session,
        }

        if not bars:
            rows.append(
                {
                    "model_id": "range_expectation_v1",
                    "model_scope": "symbol",
                    "scope_id": symbol,
                    "horizon_primary": "1to2w",
                    "horizon_secondary": ["1to3d", "2to6w"],
                    "lane_primary": "swing_term",
                    "lane_secondary": ["near_term", "position_term"],
                    "signal_direction": "neutral",
                    "signal_strength": 0.0,
                    "confidence_score": 20,
                    "confidence_label": "low",
                    "coverage_state": "insufficient_evidence",
                    "score_raw": 0.0,
                    "score_normalized": 0.0,
                    "why_code": ["range_history_missing"],
                    "lane_diagnostics": {},
                    "diagnostics": {"quality_inputs": {"bar_count": 0}},
                    "evidence": evidence,
                    "as_of_utc": now_utc.isoformat(),
                    "data_freshness_ok": False,
                    "execution_ready": False,
                }
            )
            continue

        latest = bars[-1]
        latest_status = (latest.data_status or "").upper()
        latest_market_date = from_db_utc(latest.ts_utc).date()
        latest_market_date_ok = latest_market_date >= expected_min_market_date

        lane_diagnostics = {
            lane_id: _lane_output(
                lane_id=lane_id,
                bars=bars,
                latest_status=latest_status,
                latest_market_date_ok=latest_market_date_ok,
                now_utc=now_utc,
            )
            for lane_id in ("near_term", "swing_term", "position_term")
        }

        primary_lane_id = "swing_term"
        if lane_diagnostics["swing_term"]["coverage_state"] == "insufficient_evidence":
            primary_lane_id = "near_term"
        primary_lane = lane_diagnostics[primary_lane_id]
        secondary_lane_ids = [lane for lane in ("near_term", "position_term") if lane != primary_lane_id]

        evidence["latest_close"] = round(bars[-1].close, 4)
        evidence["latest_data_status"] = latest_status
        evidence["latest_market_date"] = latest_market_date.isoformat()

        rows.append(
            {
                "model_id": "range_expectation_v1",
                "model_scope": "symbol",
                "scope_id": symbol,
                "horizon_primary": primary_lane["canonical_horizon"],
                "horizon_secondary": [lane_diagnostics[lane]["canonical_horizon"] for lane in secondary_lane_ids],
                "lane_primary": primary_lane_id,
                "lane_secondary": secondary_lane_ids,
                "signal_direction": "neutral",
                "signal_strength": primary_lane["signal_strength"],
                "confidence_score": primary_lane["confidence_score"],
                "confidence_label": primary_lane["confidence_label"],
                "coverage_state": primary_lane["coverage_state"],
                "score_raw": primary_lane["score_raw"],
                "score_normalized": primary_lane["score_normalized"],
                "why_code": primary_lane["why_code"],
                "lane_diagnostics": lane_diagnostics,
                "diagnostics": {
                    "quality_inputs": {
                        "bar_count": len(bars),
                        "latest_market_date_ok": latest_market_date_ok,
                    },
                },
                "evidence": evidence,
                "as_of_utc": now_utc.isoformat(),
                "data_freshness_ok": bool(primary_lane["lane_data_freshness_ok"]),
                "execution_ready": bool(primary_lane["lane_execution_ready"]),
            }
        )

    return rows
