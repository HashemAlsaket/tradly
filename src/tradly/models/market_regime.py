from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import pstdev
from zoneinfo import ZoneInfo

from tradly.models.calibration import (
    ConfidenceInputs,
    apply_freshness_cap,
    assess_latency,
    audit_model_artifact,
    compute_confidence,
    confidence_label,
    normalize_score,
)
from tradly.services.db_time import from_db_utc
from tradly.services.market_calendar import (
    build_trading_calendar_row,
    horizon_execution_ready,
    market_closed_reason_code,
    market_session_state,
    previous_trading_day,
)


MARKET_TZ = ZoneInfo("America/New_York")
REGIME_SYMBOLS = ("SPY", "QQQ", "VIXY", "TLT", "IEF", "SHY")
MIN_DAILY_BARS = 61
VALID_DATA_STATUS = {"REALTIME", "DELAYED"}
MAX_MACRO_AGE_DAYS = 2
MAX_MACRO_NEWS_AGE_HOURS = 24
RAW_SCORE_SCALE = 12.0
LANE_TO_HORIZON = {
    "near_term": "1to3d",
    "swing_term": "1to2w",
    "position_term": "2to6w",
}


@dataclass(frozen=True)
class Bar:
    ts_utc: datetime
    close: float
    volume: float | None
    data_status: str | None


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def _market_date_from_db_ts(ts: datetime) -> datetime.date:
    return from_db_utc(ts).astimezone(MARKET_TZ).date()


def _latest_close_and_r20(bars: list[Bar]) -> tuple[float, float]:
    closes = [bar.close for bar in bars]
    return closes[-1], closes[-1] / closes[-21] - 1.0


def _returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for idx in range(1, len(closes)):
        prev = closes[idx - 1]
        curr = closes[idx]
        out.append(0.0 if prev <= 0 else curr / prev - 1.0)
    return out


def _coverage_state(required_symbols_present: int, latest_market_date_ok: bool) -> str:
    if required_symbols_present < len(REGIME_SYMBOLS):
        return "insufficient_evidence"
    if not latest_market_date_ok:
        return "thin_evidence"
    return "sufficient_evidence"


def _merge_forced_coverage_state(base_state: str, forced_state: str | None) -> str:
    if forced_state is None:
        return base_state
    forced_priority = {"insufficient_evidence": 0, "thin_evidence": 1, "sufficient_evidence": 2}
    if forced_priority[forced_state] < forced_priority[base_state]:
        return forced_state
    return base_state


def build_market_regime_row(
    *,
    bars_by_symbol: dict[str, list[Bar]],
    now_utc: datetime,
    latest_macro_ts_utc: datetime | None,
    latest_macro_news_ts_utc: datetime | None,
) -> dict:
    evidence: dict[str, object] = {}
    why_code: list[str] = []
    missing_symbols: list[str] = []
    latest_by_symbol: dict[str, datetime] = {}
    latest_closes: dict[str, float] = {}
    r20_by_symbol: dict[str, float] = {}
    latest_status_by_symbol: dict[str, str] = {}
    feature_signs: list[int] = []

    for symbol in REGIME_SYMBOLS:
        bars = bars_by_symbol.get(symbol, [])
        if len(bars) < MIN_DAILY_BARS:
            missing_symbols.append(symbol)
            continue
        latest = bars[-1]
        status = (latest.data_status or "").upper()
        if status not in VALID_DATA_STATUS:
            missing_symbols.append(symbol)
            continue
        latest_close, r20 = _latest_close_and_r20(bars)
        latest_by_symbol[symbol] = latest.ts_utc
        latest_closes[symbol] = latest_close
        r20_by_symbol[symbol] = r20
        latest_status_by_symbol[symbol] = status
        evidence[f"{symbol.lower()}_latest_close"] = round(latest_close, 4)
        evidence[f"{symbol.lower()}_r20"] = round(r20, 6)

    required_symbols_present = len(latest_by_symbol)
    if required_symbols_present < len(REGIME_SYMBOLS):
        return {
            "model_id": "market_regime_v1",
            "model_scope": "market",
            "scope_id": "US_BROAD_MARKET",
            "horizon_primary": "1to3d",
            "horizon_secondary": ["1to2w", "2to6w"],
            "signal_direction": "neutral",
            "signal_strength": 0.0,
            "confidence_score": 20,
            "confidence_label": "low",
            "coverage_state": "insufficient_evidence",
            "score_raw": 0.0,
            "score_normalized": 0.0,
            "why_code": ["regime_inputs_missing"],
            "evidence": {
                "missing_symbols": missing_symbols,
                "required_symbols": list(REGIME_SYMBOLS),
            },
            "as_of_utc": now_utc.isoformat(),
            "data_freshness_ok": False,
        }

    global_latest_utc = max(latest_by_symbol.values())
    latest_market_date = _market_date_from_db_ts(global_latest_utc)
    expected_min_market_date = previous_trading_day(now_utc.astimezone(MARKET_TZ).date())
    latest_market_date_ok = latest_market_date >= expected_min_market_date
    calendar_row = build_trading_calendar_row(now_utc.astimezone(MARKET_TZ).date())
    current_market_session = market_session_state(now_utc)
    market_data_statuses = sorted(set(latest_status_by_symbol.values()))
    market_data_status = "DELAYED" if "DELAYED" in market_data_statuses else "REALTIME"
    macro_data_missing = latest_macro_ts_utc is None
    macro_news_missing = latest_macro_news_ts_utc is None
    macro_age_days = None
    macro_news_age_hours = None
    macro_data_fresh = False
    macro_news_fresh = False
    if latest_macro_ts_utc is not None:
        macro_ts = from_db_utc(latest_macro_ts_utc)
        macro_age_days = int((now_utc.date() - macro_ts.date()).days)
        macro_data_fresh = macro_age_days <= MAX_MACRO_AGE_DAYS
    if latest_macro_news_ts_utc is not None:
        macro_news_ts = from_db_utc(latest_macro_news_ts_utc)
        macro_news_age_hours = (now_utc - macro_news_ts).total_seconds() / 3600.0
        macro_news_fresh = macro_news_age_hours <= MAX_MACRO_NEWS_AGE_HOURS

    vix_proxy_bars = bars_by_symbol["VIXY"]
    vix_proxy_level = latest_closes["VIXY"]
    vix_proxy_5d_change = vix_proxy_level / vix_proxy_bars[-6].close - 1.0
    spy_r20 = r20_by_symbol["SPY"]
    qqq_r20 = r20_by_symbol["QQQ"]
    tlt_r20 = r20_by_symbol["TLT"]
    ief_r20 = r20_by_symbol["IEF"]
    shy_r20 = r20_by_symbol["SHY"]

    if spy_r20 > 0:
        feature_signs.append(1)
    elif spy_r20 < 0:
        feature_signs.append(-1)
    if qqq_r20 > 0:
        feature_signs.append(1)
    elif qqq_r20 < 0:
        feature_signs.append(-1)
    if vix_proxy_5d_change > 0.05:
        feature_signs.append(-1)
    elif vix_proxy_5d_change < -0.05:
        feature_signs.append(1)
    if tlt_r20 > shy_r20:
        feature_signs.append(-1)
    elif tlt_r20 < shy_r20:
        feature_signs.append(1)

    penalties: list[tuple[str, float]] = []
    supports: list[tuple[str, float]] = []
    if spy_r20 < -0.03:
        penalties.append(("spy_weak_20d", 8.0))
    elif spy_r20 > 0.03:
        supports.append(("spy_strong_20d", 8.0))
    if qqq_r20 < -0.04:
        penalties.append(("qqq_weak_20d", 6.0))
    elif qqq_r20 > 0.04:
        supports.append(("qqq_strong_20d", 6.0))
    if vix_proxy_level > 35:
        penalties.append(("vix_extreme", 10.0))
    elif vix_proxy_level > 25:
        penalties.append(("vix_elevated", 6.0))
    elif vix_proxy_level < 18:
        supports.append(("vix_calm", 4.0))
    if vix_proxy_5d_change > 0.15:
        penalties.append(("vix_rising_fast", 4.0))
    elif vix_proxy_5d_change < -0.10:
        supports.append(("vix_falling", 3.0))
    if tlt_r20 > 0.04 and spy_r20 < 0:
        penalties.append(("risk_off_bond_bid", 4.0))
    elif tlt_r20 < 0 and spy_r20 > 0:
        supports.append(("bond_bid_absent", 2.0))

    raw_score = sum(value for _, value in supports) - sum(value for _, value in penalties)
    score_normalized = normalize_score(score_raw=raw_score, raw_scale=RAW_SCORE_SCALE)
    signal_strength = round(abs(score_normalized) / 100.0, 4)

    if score_normalized >= 15:
        signal_direction = "bullish"
    elif score_normalized <= -15:
        signal_direction = "bearish"
    else:
        signal_direction = "neutral"

    if signal_direction == "bullish":
        why_code.extend(label for label, _ in supports[:3])
    elif signal_direction == "bearish":
        why_code.extend(label for label, _ in penalties[:3])
    else:
        why_code.append("regime_mixed")

    evidence_density_score = round(required_symbols_present / len(REGIME_SYMBOLS) * 100)
    pos = sum(1 for sign in feature_signs if sign > 0)
    neg = sum(1 for sign in feature_signs if sign < 0)
    total_signals = pos + neg
    feature_agreement_score = round((max(pos, neg) / total_signals) * 100) if total_signals else 0
    vix_returns = _returns([bar.close for bar in vix_proxy_bars[-21:]])
    vix_vol = pstdev(vix_returns) if len(vix_returns) >= 2 else 0.0
    stability_score = round(clamp(100.0 - vix_vol * 800.0, 20.0, 100.0))
    base_coverage_state = _coverage_state(required_symbols_present, latest_market_date_ok)

    def _build_lane(lane_id: str) -> dict[str, object]:
        horizon = LANE_TO_HORIZON[lane_id]
        lane_execution_ready = horizon_execution_ready(horizon=horizon, now_utc=now_utc)
        assessment = assess_latency(
            data_status=market_data_status,
            recency_ok=latest_market_date_ok,
            horizon=horizon,  # type: ignore[arg-type]
        )
        coverage_state = _merge_forced_coverage_state(base_coverage_state, assessment.forced_coverage_state)
        lane_why_code: list[str] = list(why_code)

        if macro_data_missing or macro_news_missing:
            coverage_state = "insufficient_evidence"
        elif lane_id == "near_term":
            if not macro_data_fresh or not macro_news_fresh:
                coverage_state = "thin_evidence" if coverage_state == "sufficient_evidence" else coverage_state
        elif lane_id == "swing_term":
            if not macro_news_fresh:
                coverage_state = "thin_evidence" if coverage_state == "sufficient_evidence" else coverage_state
        else:
            # Position horizons should tolerate slower macro/news cadence unless inputs are fully missing.
            coverage_state = coverage_state

        freshness_score = 100 if latest_market_date_ok else 55
        freshness_score = apply_freshness_cap(freshness_score=freshness_score, assessment=assessment)
        if macro_data_missing or macro_news_missing:
            freshness_score = min(freshness_score, 25)
        elif lane_id == "near_term":
            if not macro_data_fresh or not macro_news_fresh:
                freshness_score = min(freshness_score, 60)
        elif lane_id == "swing_term":
            if not macro_data_fresh:
                freshness_score = min(freshness_score, 80)
            if not macro_news_fresh:
                freshness_score = min(freshness_score, 70)
        else:
            if not macro_data_fresh:
                freshness_score = min(freshness_score, 90)
            if not macro_news_fresh:
                freshness_score = min(freshness_score, 85)

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
                informative_feature_count=4,
                independent_informative_feature_count=4,
            ),
            assessment=assessment,
        )

        if coverage_state != "sufficient_evidence":
            lane_why_code.append("market_data_stale")
        calendar_reason = market_closed_reason_code(now_utc=now_utc)
        if not lane_execution_ready and calendar_reason is not None:
            lane_why_code.append(calendar_reason)
        for code in assessment.why_code:
            if code not in lane_why_code:
                lane_why_code.append(code)
        if macro_data_missing:
            lane_why_code.append("macro_input_missing")
        elif not macro_data_fresh:
            lane_why_code.append(
                "macro_input_stale"
                if lane_id == "near_term"
                else "macro_input_warning"
                if lane_id == "swing_term"
                else "macro_input_slow_ok"
            )
        if macro_news_missing:
            lane_why_code.append("macro_news_missing")
        elif not macro_news_fresh:
            lane_why_code.append(
                "macro_news_stale"
                if lane_id == "near_term"
                else "macro_news_warning"
                if lane_id == "swing_term"
                else "macro_news_slow_ok"
            )

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
                latest_market_date_ok
                and not macro_data_missing
                and not macro_news_missing
                and (
                    (lane_id == "near_term" and macro_data_fresh and macro_news_fresh)
                    or (lane_id == "swing_term" and macro_news_fresh)
                    or (lane_id == "position_term")
                )
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
    else:
        primary_lane_id = "near_term" if abs(score_normalized) >= 40 or vix_proxy_level > 25 else "swing_term"
    secondary_lane_ids = [lane_id for lane_id in ("near_term", "swing_term", "position_term") if lane_id != primary_lane_id]
    primary_lane = lane_diagnostics[primary_lane_id]

    confidence_score = int(primary_lane["confidence_score"])
    confidence_label_value = str(primary_lane["confidence_label"])
    coverage_state = str(primary_lane["coverage_state"])
    freshness_score = int(primary_lane["freshness_score"])
    coverage_score = int(primary_lane["coverage_score"])
    latency_details = primary_lane["latency_assessment"]
    market_data_latency_minutes = int(latency_details["market_data_latency_minutes"])
    latency_class = str(latency_details["latency_class"])
    why_code = list(primary_lane["why_code"])

    horizon_primary = str(primary_lane["canonical_horizon"])
    horizon_secondary = [str(lane_diagnostics[lane_id]["canonical_horizon"]) for lane_id in secondary_lane_ids]

    evidence.update(
        {
            "required_symbols": list(REGIME_SYMBOLS),
            "required_symbols_present": required_symbols_present,
            "latest_bar_utc": from_db_utc(global_latest_utc).isoformat(),
            "latest_market_date": latest_market_date.isoformat(),
            "expected_min_market_date": expected_min_market_date.isoformat(),
            "latest_market_date_ok": latest_market_date_ok,
            "market_calendar_state": calendar_row.market_calendar_state,
            "day_name": calendar_row.day_name,
            "last_cash_session_date": calendar_row.last_cash_session_date.isoformat(),
            "market_session_state": current_market_session,
            "data_status": market_data_statuses,
            "market_data_latency_minutes": market_data_latency_minutes,
            "recency_ok": latest_market_date_ok,
            "latency_class": latency_class,
            "vix_proxy_symbol": "VIXY",
            "vix_proxy_level": round(vix_proxy_level, 4),
            "vix_proxy_5d_change": round(vix_proxy_5d_change, 6),
            "supports": [label for label, _ in supports],
            "penalties": [label for label, _ in penalties],
            "latest_macro_ts_utc": from_db_utc(latest_macro_ts_utc).isoformat() if latest_macro_ts_utc else None,
            "latest_macro_news_ts_utc": (
                from_db_utc(latest_macro_news_ts_utc).isoformat() if latest_macro_news_ts_utc else None
            ),
            "macro_age_days": macro_age_days,
            "macro_news_age_hours": round(macro_news_age_hours, 2) if macro_news_age_hours is not None else None,
            "macro_data_fresh": macro_data_fresh,
            "macro_news_fresh": macro_news_fresh,
            "evidence_density_score": evidence_density_score,
            "feature_agreement_score": feature_agreement_score,
            "freshness_score": freshness_score,
            "stability_score": stability_score,
            "coverage_score": coverage_score,
            "lane_diagnostics": lane_diagnostics,
        }
    )

    row = {
        "model_id": "market_regime_v1",
        "model_scope": "market",
        "scope_id": "US_BROAD_MARKET",
        "horizon_primary": horizon_primary,
        "horizon_secondary": horizon_secondary,
        "lane_primary": primary_lane_id,
        "lane_secondary": secondary_lane_ids,
        "signal_direction": signal_direction,
        "signal_strength": signal_strength,
        "confidence_score": confidence_score,
        "confidence_label": confidence_label_value,
        "coverage_state": coverage_state,
        "score_raw": round(raw_score, 4),
        "score_normalized": round(score_normalized, 4),
        "why_code": why_code,
        "lane_diagnostics": lane_diagnostics,
        "evidence": evidence,
        "as_of_utc": now_utc.isoformat(),
        "data_freshness_ok": bool(primary_lane["lane_data_freshness_ok"]),
        "execution_ready": bool(primary_lane["lane_execution_ready"]),
    }
    return row
