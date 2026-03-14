from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from statistics import pstdev
from zoneinfo import ZoneInfo

from tradly.paths import get_repo_root
from tradly.services.db_time import from_db_utc
from tradly.services.investability_gate import apply_investability_gate
from tradly.services.market_calendar import previous_trading_day
from tradly.services.time_context import get_time_context


@dataclass(frozen=True)
class Bar:
    ts_utc: datetime
    close: float
    volume: float | None
    data_status: str | None


MIN_DAILY_BARS = 61
REQUIRED_ADV_BARS = 20
VALID_DATA_STATUS = {"REALTIME", "DELAYED"}
REGIME_SYMBOLS = ("SPY", "QQQ", "VIXY", "TLT", "IEF", "SHY")
FOCUS_SEMIS = ("MU", "SNDK", "NVDA", "NVTS")
CT_ZONE = ZoneInfo("America/Chicago")
MARKET_TZ = ZoneInfo("America/New_York")
WATCHLIST_PATH = Path("data/manual/news_seed_watchlists.json")
DEFAULT_DAILY_REQUEST_BUDGET = 100
MIN_INTERPRETED_NEWS_24H = 4
MAX_INTERPRETED_NEWS_AGE_SECONDS = 3 * 60 * 60
MAX_PULL_USAGE_AGE_SECONDS = 3 * 60 * 60
NEWS_INTERPRETER_PROMPT_VERSION = "news_interpreter_v0"


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def _market_date_from_db_ts(ts: datetime) -> datetime.date:
    return from_db_utc(ts).astimezone(MARKET_TZ).date()


def _age_seconds_from_db_ts(ts: datetime | None, now_utc: datetime) -> int | None:
    if ts is None:
        return None
    return int((now_utc - from_db_utc(ts)).total_seconds())


def _is_market_hours(now_local: datetime) -> bool:
    if now_local.weekday() >= 5:
        return False
    t = now_local.time()
    return time(8, 30) <= t <= time(15, 0)


def _validate_market_recency(time_ctx, bars_by_symbol: dict[str, list[Bar]]) -> list[str]:
    errors: list[str] = []
    latest_by_symbol: dict[str, datetime] = {}
    for symbol in REGIME_SYMBOLS:
        bars = bars_by_symbol.get(symbol, [])
        if not bars:
            errors.append(f"market_recency:missing_symbol:{symbol}")
            continue
        latest_by_symbol[symbol] = bars[-1].ts_utc

    if errors:
        return errors

    global_latest_utc = max(latest_by_symbol.values())
    latest_market_date = _market_date_from_db_ts(global_latest_utc)
    expected_min_market_date = previous_trading_day(time_ctx.now_utc.astimezone(MARKET_TZ).date())
    if latest_market_date < expected_min_market_date:
        errors.append(
            (
                "market_recency:stale:"
                f"latest_market_date={latest_market_date}:expected_min_market_date={expected_min_market_date}"
            )
        )
    return errors


def compute_returns(closes: list[float]) -> list[float]:
    out: list[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        curr = closes[i]
        if prev <= 0:
            out.append(0.0)
        else:
            out.append(curr / prev - 1.0)
    return out


def latest_bar_by_day(rows: list[tuple]) -> dict[str, list[Bar]]:
    grouped: dict[str, dict[datetime, tuple[int, Bar]]] = defaultdict(dict)
    for symbol, ts_utc, close, volume, data_status, correction_seq in rows:
        if close is None:
            continue
        bar = Bar(ts_utc=ts_utc, close=float(close), volume=volume, data_status=data_status)
        current = grouped[symbol].get(ts_utc)
        if current is None or correction_seq > current[0]:
            grouped[symbol][ts_utc] = (correction_seq, bar)

    out: dict[str, list[Bar]] = {}
    for symbol, by_day in grouped.items():
        bars = [item[1] for item in by_day.values()]
        bars.sort(key=lambda b: b.ts_utc)
        out[symbol] = bars
    return out


def _latest_close_and_r20(bars: list[Bar]) -> tuple[float, float]:
    closes = [b.close for b in bars]
    return closes[-1], closes[-1] / closes[-21] - 1.0


def compute_regime_context(bars_by_symbol: dict[str, list[Bar]]) -> tuple[dict, list[str]]:
    errors: list[str] = []
    latest_closes: dict[str, float] = {}
    r20_by_symbol: dict[str, float] = {}

    for symbol in REGIME_SYMBOLS:
        bars = bars_by_symbol.get(symbol, [])
        if len(bars) < MIN_DAILY_BARS:
            errors.append(f"regime:{symbol}:insufficient_daily_bars:{len(bars)}")
            continue
        latest = bars[-1]
        status = (latest.data_status or "").upper()
        if status not in VALID_DATA_STATUS:
            errors.append(f"regime:{symbol}:invalid_data_status:{latest.data_status}")
            continue
        latest_close, r20 = _latest_close_and_r20(bars)
        latest_closes[symbol] = latest_close
        r20_by_symbol[symbol] = r20

    if errors:
        return {}, errors

    vix_proxy_bars = bars_by_symbol["VIXY"]
    vix_proxy_level = latest_closes["VIXY"]
    vix_proxy_5d_change = vix_proxy_level / vix_proxy_bars[-6].close - 1.0

    spy_r20 = r20_by_symbol["SPY"]
    qqq_r20 = r20_by_symbol["QQQ"]
    tlt_r20 = r20_by_symbol["TLT"]
    ief_r20 = r20_by_symbol["IEF"]
    shy_r20 = r20_by_symbol["SHY"]

    penalties: list[tuple[str, float]] = []
    if spy_r20 < -0.03:
        penalties.append(("spy_weak_20d", 8.0))
    if qqq_r20 < -0.04:
        penalties.append(("qqq_weak_20d", 6.0))
    if vix_proxy_level > 35:
        penalties.append(("vix_extreme", 10.0))
    elif vix_proxy_level > 25:
        penalties.append(("vix_elevated", 6.0))
    if vix_proxy_5d_change > 0.15:
        penalties.append(("vix_rising_fast", 4.0))
    if tlt_r20 > 0.04 and spy_r20 < 0:
        penalties.append(("risk_off_bond_bid", 4.0))

    regime_penalty = clamp(sum(value for _label, value in penalties), 0.0, 20.0)

    summary_parts: list[str] = []
    summary_parts.append("risk-on" if spy_r20 > 0 and qqq_r20 > 0 else "mixed/risk-off")
    if vix_proxy_level > 25:
        summary_parts.append("volatility elevated")
    elif vix_proxy_level < 18:
        summary_parts.append("volatility calm")
    else:
        summary_parts.append("volatility normal")
    if (tlt_r20 + ief_r20) / 2.0 > shy_r20:
        summary_parts.append("duration bid")
    else:
        summary_parts.append("duration flat")
    regime_summary = ", ".join(summary_parts)

    return {
        "spy_r20": round(spy_r20, 6),
        "qqq_r20": round(qqq_r20, 6),
        "vix_proxy_symbol": "VIXY",
        "vix_proxy_level": round(vix_proxy_level, 4),
        "vix_proxy_5d_change": round(vix_proxy_5d_change, 6),
        "tlt_r20": round(tlt_r20, 6),
        "ief_r20": round(ief_r20, 6),
        "shy_r20": round(shy_r20, 6),
        "regime_penalty": round(regime_penalty, 4),
        "regime_summary": regime_summary,
        "regime_flags": [label for label, _value in penalties],
    }, []


def compute_news_features(
    now_utc: datetime,
    news_rows: list[tuple],
) -> dict[str, dict[str, float | int | None]]:
    by_symbol: dict[str, dict[str, float | int | None]] = {}
    since_24h = now_utc - timedelta(hours=24)
    for symbol in FOCUS_SEMIS:
        by_symbol[symbol] = {
            "news_count_24h": 0,
            "sentiment_avg_24h": None,
            "news_adjustment": 0.0,
        }

    sentiment_sums: dict[str, float] = {s: 0.0 for s in FOCUS_SEMIS}
    sentiment_counts: dict[str, int] = {s: 0 for s in FOCUS_SEMIS}

    for symbol, published_at_utc, sentiment_score in news_rows:
        if symbol not in by_symbol:
            continue
        if isinstance(published_at_utc, datetime):
            published_at_utc = from_db_utc(published_at_utc)
        if published_at_utc < since_24h:
            continue
        by_symbol[symbol]["news_count_24h"] = int(by_symbol[symbol]["news_count_24h"]) + 1
        if sentiment_score is not None:
            sentiment_sums[symbol] += float(sentiment_score)
            sentiment_counts[symbol] += 1

    for symbol in FOCUS_SEMIS:
        count_24h = int(by_symbol[symbol]["news_count_24h"])
        avg_sent = None
        if sentiment_counts[symbol] > 0:
            avg_sent = sentiment_sums[symbol] / sentiment_counts[symbol]
            by_symbol[symbol]["sentiment_avg_24h"] = round(avg_sent, 4)

        adjustment = 0.0
        if avg_sent is not None:
            adjustment = clamp(avg_sent, -1.0, 1.0) * 6.0
            if count_24h >= 5:
                adjustment *= 1.2
        by_symbol[symbol]["news_adjustment"] = round(adjustment, 4)
    return by_symbol


def _load_daily_request_budget(repo_root: Path) -> int:
    watchlist = repo_root / WATCHLIST_PATH
    if not watchlist.exists():
        return DEFAULT_DAILY_REQUEST_BUDGET
    try:
        payload = json.loads(watchlist.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return DEFAULT_DAILY_REQUEST_BUDGET
    if not isinstance(payload, dict):
        return DEFAULT_DAILY_REQUEST_BUDGET
    value = payload.get("daily_request_budget", DEFAULT_DAILY_REQUEST_BUDGET)
    try:
        budget = int(value)
    except (TypeError, ValueError):
        return DEFAULT_DAILY_REQUEST_BUDGET
    return budget if budget > 0 else DEFAULT_DAILY_REQUEST_BUDGET


def compute_interpreted_news_features(
    now_utc: datetime,
    interpreted_rows: list[tuple],
) -> tuple[dict[str, dict[str, float | int | str | bool | None]], dict]:
    since_24h = now_utc - timedelta(hours=24)
    by_symbol: dict[str, dict[str, float | int | str | bool | None]] = {}
    for symbol in FOCUS_SEMIS:
        by_symbol[symbol] = {
            "llm_news_count_24h": 0,
            "llm_bullish_semis_high_24h": 0,
            "llm_bearish_semis_high_24h": 0,
            "llm_latest_impact_note": None,
            "llm_news_adjustment": 0.0,
            "macro_risk_off_high_24h": 0,
        }

    global_summary = {
        "interpreted_rows_24h": 0,
        "macro_risk_off_high_24h": 0,
        "latest_interpreted_at_utc": None,
    }

    for (
        _provider_news_id,
        bucket,
        impact_direction,
        confidence_label,
        relevance_symbols_json,
        market_impact_note,
        interpreted_at_utc,
    ) in interpreted_rows:
        if interpreted_at_utc is None:
            continue
        if isinstance(interpreted_at_utc, datetime):
            interpreted_at_utc = from_db_utc(interpreted_at_utc)
        if interpreted_at_utc < since_24h:
            continue

        global_summary["interpreted_rows_24h"] = int(global_summary["interpreted_rows_24h"]) + 1
        current_latest = global_summary["latest_interpreted_at_utc"]
        if current_latest is None or interpreted_at_utc > current_latest:
            global_summary["latest_interpreted_at_utc"] = interpreted_at_utc

        direction = str(impact_direction or "").strip().lower()
        confidence = str(confidence_label or "").strip().lower()
        bucket_text = str(bucket or "").strip().lower()

        if bucket_text == "macro" and direction == "risk_off" and confidence == "high":
            global_summary["macro_risk_off_high_24h"] = int(global_summary["macro_risk_off_high_24h"]) + 1

        symbols: list[str] = []
        if isinstance(relevance_symbols_json, str) and relevance_symbols_json.strip():
            try:
                parsed = json.loads(relevance_symbols_json)
                if isinstance(parsed, list):
                    symbols = [str(item).strip().upper() for item in parsed if str(item).strip()]
            except json.JSONDecodeError:
                symbols = []

        targets = [s for s in symbols if s in by_symbol]
        if not targets and bucket_text == "macro":
            targets = list(FOCUS_SEMIS)

        for symbol in targets:
            feature = by_symbol[symbol]
            feature["llm_news_count_24h"] = int(feature["llm_news_count_24h"]) + 1
            feature["macro_risk_off_high_24h"] = int(global_summary["macro_risk_off_high_24h"])

            if isinstance(market_impact_note, str) and market_impact_note.strip():
                feature["llm_latest_impact_note"] = market_impact_note.strip()[:500]

            if direction == "bullish_semis" and confidence == "high":
                feature["llm_bullish_semis_high_24h"] = int(feature["llm_bullish_semis_high_24h"]) + 1
            if direction == "bearish_semis" and confidence == "high":
                feature["llm_bearish_semis_high_24h"] = int(feature["llm_bearish_semis_high_24h"]) + 1

    for symbol in FOCUS_SEMIS:
        feature = by_symbol[symbol]
        bullish = int(feature["llm_bullish_semis_high_24h"])
        bearish = int(feature["llm_bearish_semis_high_24h"])
        macro_risk_off = int(global_summary["macro_risk_off_high_24h"])
        adjustment = 0.0
        adjustment += min(2.0, bullish * 1.0)
        adjustment -= min(3.0, bearish * 1.5)
        adjustment -= min(2.0, macro_risk_off * 1.0)
        feature["llm_news_adjustment"] = round(adjustment, 4)
        feature["macro_risk_off_high_24h"] = macro_risk_off

    latest_interpreted = global_summary["latest_interpreted_at_utc"]
    latest_age = None
    if isinstance(latest_interpreted, datetime):
        latest_age = int((now_utc - latest_interpreted).total_seconds())

    global_summary["latest_interpreted_age_seconds"] = latest_age
    return by_symbol, global_summary


def compute_news_guardrails(
    *,
    now_utc: datetime,
    interpreted_summary: dict,
    usage_today_requests: int,
    latest_pull_usage_at_utc: datetime | None,
    daily_budget: int,
) -> dict:
    blocking_reasons: list[str] = []
    warning_reasons: list[str] = []

    interpreted_rows_24h = int(interpreted_summary.get("interpreted_rows_24h") or 0)
    latest_interpreted_age_seconds = interpreted_summary.get("latest_interpreted_age_seconds")
    if interpreted_rows_24h < MIN_INTERPRETED_NEWS_24H:
        blocking_reasons.append("interpreted_news_thin")
    if latest_interpreted_age_seconds is None:
        blocking_reasons.append("interpreted_news_missing")
    elif int(latest_interpreted_age_seconds) > MAX_INTERPRETED_NEWS_AGE_SECONDS:
        blocking_reasons.append("interpreted_news_stale")

    latest_pull_age_seconds = None
    if latest_pull_usage_at_utc is not None:
        latest_pull_usage_at_utc = from_db_utc(latest_pull_usage_at_utc)
        latest_pull_age_seconds = int((now_utc - latest_pull_usage_at_utc).total_seconds())
        if latest_pull_age_seconds > MAX_PULL_USAGE_AGE_SECONDS:
            warning_reasons.append("news_pull_usage_stale")
    else:
        warning_reasons.append("news_pull_usage_missing")

    now_ct = now_utc.astimezone(CT_ZONE)
    budget_exhausted_early = usage_today_requests >= daily_budget and now_ct.hour < 16
    if budget_exhausted_early:
        blocking_reasons.append("news_budget_exhausted_early")

    status = "pass"
    if blocking_reasons:
        status = "block"
    elif warning_reasons:
        status = "warn"

    return {
        "status": status,
        "daily_budget": daily_budget,
        "requests_used_today": usage_today_requests,
        "latest_pull_usage_at_utc": latest_pull_usage_at_utc.isoformat() if latest_pull_usage_at_utc else None,
        "latest_pull_age_seconds": latest_pull_age_seconds,
        "interpreted_rows_24h": interpreted_rows_24h,
        "latest_interpreted_age_seconds": latest_interpreted_age_seconds,
        "blocking_reasons": blocking_reasons,
        "warning_reasons": warning_reasons,
        "coverage_blocked": bool(blocking_reasons),
    }


def _fmt_ct(value: datetime) -> str:
    return value.astimezone(CT_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")


def _build_execution_regime(action: str, horizon: str, now_utc: datetime) -> dict:
    action_norm = action.strip()
    horizon_norm = horizon.strip().lower()

    if action_norm in {"Strong Buy", "Buy"}:
        if horizon_norm == "hourly":
            offsets = [0, 2, 4]
            unit = "hours"
        elif horizon_norm == "weekly":
            offsets = [0, 48, 96]
            unit = "hours"
        else:
            offsets = [0, 24, 48]
            unit = "hours"
        weights = [0.34, 0.33, 0.33]
        tranches = []
        for idx, (offset, weight) in enumerate(zip(offsets, weights), start=1):
            execute_at = now_utc + timedelta(hours=offset)
            tranches.append(
                {
                    "tranche_number": idx,
                    "allocation_pct": round(weight * 100, 1),
                    "execute_after_offset": offset,
                    "offset_unit": unit,
                    "execute_after_utc": execute_at.isoformat(),
                    "execute_after_ct": _fmt_ct(execute_at),
                    "condition": "Execute only if latest action remains Buy/Strong Buy and no investability block.",
                }
            )
        return {
            "planned_side": "BUY",
            "tranche_count": len(tranches),
            "plan_label": "3-tranche accumulation",
            "tranches": tranches,
            "cancel_rule": "Cancel remaining tranches immediately if action downgrades to Watch/Trim/Exit.",
        }

    if action_norm == "Trim":
        tranches = []
        for idx, offset in enumerate([0, 24], start=1):
            execute_at = now_utc + timedelta(hours=offset)
            tranches.append(
                {
                    "tranche_number": idx,
                    "allocation_pct": 50.0,
                    "execute_after_offset": offset,
                    "offset_unit": "hours",
                    "execute_after_utc": execute_at.isoformat(),
                    "execute_after_ct": _fmt_ct(execute_at),
                    "condition": "Execute if action remains Trim or worsens to Exit.",
                }
            )
        return {
            "planned_side": "SELL",
            "tranche_count": len(tranches),
            "plan_label": "2-tranche reduction",
            "tranches": tranches,
            "cancel_rule": "Cancel tranche 2 if action upgrades to Buy/Strong Buy.",
        }

    if action_norm == "Exit":
        execute_at = now_utc
        return {
            "planned_side": "SELL",
            "tranche_count": 1,
            "plan_label": "Immediate exit",
            "tranches": [
                {
                    "tranche_number": 1,
                    "allocation_pct": 100.0,
                    "execute_after_offset": 0,
                    "offset_unit": "hours",
                    "execute_after_utc": execute_at.isoformat(),
                    "execute_after_ct": _fmt_ct(execute_at),
                    "condition": "Execute now.",
                }
            ],
            "cancel_rule": "No cancellation rule; single immediate exit decision.",
        }

    return {
        "planned_side": "NONE",
        "tranche_count": 0,
        "plan_label": "No-entry watch mode",
        "tranches": [],
        "cancel_rule": "No tranches scheduled while in Watch.",
    }


def score_symbol(
    symbol: str,
    halal_flag: str,
    sector: str | None,
    bars: list[Bar],
    regime_context: dict,
    news_feature: dict[str, float | int | None] | None,
    interpreted_feature: dict[str, float | int | str | bool | None] | None,
    coverage_blocked: bool,
    now_utc: datetime,
) -> dict | None:
    if len(bars) < MIN_DAILY_BARS:
        return None

    closes = [b.close for b in bars]
    latest = bars[-1]

    r20 = closes[-1] / closes[-21] - 1.0
    r60 = closes[-1] / closes[-61] - 1.0

    high20 = max(closes[-20:])
    pullback_pct = 0.0 if high20 <= 0 else max(0.0, (high20 - closes[-1]) / high20)
    bounce_3d = closes[-1] / closes[-4] - 1.0

    trend_score = clamp(50.0 + 900.0 * r20 + 400.0 * r60, 0.0, 100.0)
    pullback_score = clamp(55.0 + 350.0 * pullback_pct + 500.0 * bounce_3d, 0.0, 100.0)
    m_price = 0.60 * trend_score + 0.40 * pullback_score

    ret20 = compute_returns(closes[-21:])
    vol20_ann = pstdev(ret20) * math.sqrt(252) if ret20 else 0.0
    adv20 = sum((b.close * float(b.volume)) for b in bars[-REQUIRED_ADV_BARS:]) / REQUIRED_ADV_BARS

    vol_penalty = clamp((vol20_ann - 0.25) / 0.35 * 40.0, 0.0, 40.0)
    liq_penalty = 0.0 if adv20 >= 1e9 else clamp((1e9 - adv20) / 1e9 * 25.0, 0.0, 25.0)
    feed_penalty = 10.0 if (latest.data_status or "").upper() != "REALTIME" else 0.0
    m_risk = vol_penalty + liq_penalty + feed_penalty

    regime_penalty = float(regime_context["regime_penalty"])
    news_adjustment = 0.0
    news_count_24h = 0
    sentiment_avg_24h = None
    llm_news_adjustment = 0.0
    llm_bullish_semis_high_24h = 0
    llm_bearish_semis_high_24h = 0
    macro_risk_off_high_24h = 0
    llm_latest_impact_note = None
    if news_feature:
        news_adjustment = float(news_feature.get("news_adjustment") or 0.0)
        news_count_24h = int(news_feature.get("news_count_24h") or 0)
        sentiment_avg_24h = news_feature.get("sentiment_avg_24h")
    if interpreted_feature:
        llm_news_adjustment = float(interpreted_feature.get("llm_news_adjustment") or 0.0)
        llm_bullish_semis_high_24h = int(interpreted_feature.get("llm_bullish_semis_high_24h") or 0)
        llm_bearish_semis_high_24h = int(interpreted_feature.get("llm_bearish_semis_high_24h") or 0)
        macro_risk_off_high_24h = int(interpreted_feature.get("macro_risk_off_high_24h") or 0)
        llm_latest_impact_note = interpreted_feature.get("llm_latest_impact_note")
    score = clamp(m_price - m_risk - regime_penalty + news_adjustment + llm_news_adjustment, 0.0, 100.0)

    if score >= 75:
        proposed_action = "Strong Buy"
    elif score >= 60:
        proposed_action = "Buy"
    elif score >= 45:
        proposed_action = "Watch"
    elif score >= 30:
        proposed_action = "Trim"
    else:
        proposed_action = "Exit"

    gate = apply_investability_gate(proposed_action, halal_flag)
    final_action = gate.final_action
    hard_downgrade_reason = None
    macro_semis_bearish = macro_risk_off_high_24h > 0 and llm_bearish_semis_high_24h > llm_bullish_semis_high_24h
    if final_action in {"Strong Buy", "Buy"} and macro_semis_bearish:
        final_action = "Watch"
        hard_downgrade_reason = "macro_riskoff_plus_semis_bearish"
    if final_action in {"Strong Buy", "Buy"} and coverage_blocked:
        final_action = "Watch"
        hard_downgrade_reason = "news_coverage_blocked"

    if score >= 75 and vol20_ann < 0.30:
        horizon = "weekly"
    elif score >= 60:
        horizon = "daily"
    elif score >= 45:
        horizon = "hourly"
    else:
        horizon = "daily"

    why_now = (
        f"{'Uptrend' if r20 > 0 and r60 > 0 else 'Weak trend'}; "
        f"{'healthy pullback' if pullback_pct >= 0.02 else 'little pullback'}; "
        f"{'lower volatility' if vol20_ann < 0.25 else 'higher volatility'}; "
        f"{'strong liquidity' if adv20 >= 1e9 else 'lighter liquidity'}; "
        f"regime: {regime_context['regime_summary']}; "
        f"news24h={news_count_24h}, sentiment={sentiment_avg_24h if sentiment_avg_24h is not None else 'n/a'}; "
        f"llm_news_adj={llm_news_adjustment}, macro_riskoff_high_24h={macro_risk_off_high_24h}"
    )
    if isinstance(llm_latest_impact_note, str) and llm_latest_impact_note:
        why_now = f"{why_now}; llm_note={llm_latest_impact_note}"

    current_close = closes[-1]
    hard_stop_price = round(current_close * 0.93, 2)
    if final_action in {"Strong Buy", "Buy"}:
        sell_plan = f"Trim if score < 45. Exit if score < 30 or close < ${hard_stop_price}."
    elif final_action == "Watch":
        sell_plan = "No new position. Re-evaluate next run; only buy if score >= 60."
    elif final_action == "Trim":
        sell_plan = "Reduce exposure in tranches. Fully exit if score < 30."
    else:
        sell_plan = "Exit now. Re-enter only if score >= 60 and investability allows."

    execution_regime = _build_execution_regime(final_action, horizon, now_utc)
    if execution_regime["tranches"]:
        buy_til_utc = execution_regime["tranches"][-1]["execute_after_utc"]
    else:
        buy_til_utc = None
    hold_til_utc = (now_utc + timedelta(days=2)).isoformat()
    sell_by_utc = (now_utc + timedelta(days=2)).isoformat()

    return {
        "symbol": symbol,
        "sector": sector,
        "halal_flag": halal_flag,
        "latest_bar_utc": latest.ts_utc.isoformat(),
        "price_model_score": round(m_price, 4),
        "risk_penalty": round(m_risk, 4),
        "regime_penalty": round(regime_penalty, 4),
        "news_adjustment": round(news_adjustment, 4),
        "llm_news_adjustment": round(llm_news_adjustment, 4),
        "news_count_24h": news_count_24h,
        "news_sentiment_avg_24h": sentiment_avg_24h,
        "llm_bullish_semis_high_24h": llm_bullish_semis_high_24h,
        "llm_bearish_semis_high_24h": llm_bearish_semis_high_24h,
        "macro_risk_off_high_24h": macro_risk_off_high_24h,
        "llm_latest_impact_note": llm_latest_impact_note,
        "vol20_ann": round(vol20_ann, 6),
        "final_score": round(score, 4),
        "proposed_action": proposed_action,
        "final_action": final_action,
        "investability_blocked": gate.blocked,
        "block_reason_code": gate.reason_code,
        "hard_downgrade_reason": hard_downgrade_reason,
        "horizon_type": horizon,
        "why_now": why_now,
        "sell_plan": sell_plan,
        "invalidation_price": hard_stop_price,
        "latest_close": round(current_close, 4),
        "reference_price": round(current_close, 4),
        "buy_til_utc": buy_til_utc,
        "hold_til_utc": hold_til_utc,
        "sell_by_utc": sell_by_utc,
        "target_order_side": execution_regime["planned_side"],
        "target_order_qty": None,
        "target_order_notional": None,
        "execution_note": execution_regime["plan_label"],
        "execution_regime": execution_regime,
        "model_version": "model_v0_price_risk_3_news_guardrails",
        "data_status": latest.data_status,
        "regime_summary": regime_context["regime_summary"],
        "regime_flags": regime_context["regime_flags"],
    }


def _validate_inputs(instrument_rows: list[tuple], bars_by_symbol: dict[str, list[Bar]]) -> list[str]:
    errors: list[str] = []
    for symbol, halal_flag, _sector in instrument_rows:
        if symbol is None or str(symbol).strip() == "":
            errors.append("instrument_symbol_missing")
            continue
        if halal_flag is None or str(halal_flag).strip() == "":
            errors.append(f"{symbol}:halal_flag_missing")
            continue

        bars = bars_by_symbol.get(symbol, [])
        if len(bars) < MIN_DAILY_BARS:
            errors.append(f"{symbol}:insufficient_daily_bars:{len(bars)}")
            continue

        for idx, bar in enumerate(bars):
            if bar.close <= 0:
                errors.append(f"{symbol}:non_positive_close_at_index:{idx}")
                break

        latest = bars[-1]
        status = (latest.data_status or "").upper()
        if status not in VALID_DATA_STATUS:
            errors.append(f"{symbol}:invalid_data_status:{latest.data_status}")

        for idx, bar in enumerate(bars[-REQUIRED_ADV_BARS:]):
            if bar.volume is None:
                errors.append(f"{symbol}:missing_volume_last20_index:{idx}")
                break
            if float(bar.volume) <= 0:
                errors.append(f"{symbol}:non_positive_volume_last20_index:{idx}")
                break

    return errors


def main() -> int:
    repo_root = get_repo_root()
    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        print("run: python scripts/setup/init_db.py")
        return 1

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 2

    time_ctx = get_time_context()
    conn = duckdb.connect(str(db_path))
    try:
        instrument_rows = conn.execute(
            """
            SELECT symbol, halal_flag, sector
            FROM instruments
            WHERE active = TRUE
            ORDER BY symbol
            """
        ).fetchall()

        bar_rows = conn.execute(
            """
            SELECT symbol, ts_utc, close, volume, data_status, correction_seq
            FROM market_bars
            WHERE timeframe = '1d'
            ORDER BY symbol, ts_utc
            """
        ).fetchall()
        news_rows = conn.execute(
            """
            SELECT ns.symbol, ne.published_at_utc, ne.sentiment_score
            FROM news_symbols ns
            JOIN news_events ne
              ON ne.provider = ns.provider
             AND ne.provider_news_id = ns.provider_news_id
            WHERE ns.symbol IN ('MU', 'SNDK', 'NVDA', 'NVTS')
              AND ne.published_at_utc >= ?
            """,
            (time_ctx.now_utc - timedelta(days=3),),
        ).fetchall()
        interpreted_source = conn.execute(
            """
            SELECT model, prompt_version, MAX(interpreted_at_utc) AS latest_interpreted_at
            FROM news_interpretations
            WHERE interpreted_at_utc >= ?
              AND prompt_version = ?
            GROUP BY model, prompt_version
            ORDER BY latest_interpreted_at DESC
            LIMIT 1
            """,
            (time_ctx.now_utc - timedelta(days=3), NEWS_INTERPRETER_PROMPT_VERSION),
        ).fetchone()
        interpreted_rows: list[tuple] = []
        interpreted_source_model = None
        interpreted_source_prompt = None
        if interpreted_source:
            interpreted_source_model = interpreted_source[0]
            interpreted_source_prompt = interpreted_source[1]
            interpreted_rows = conn.execute(
                """
                SELECT
                  provider_news_id,
                  bucket,
                  impact_direction,
                  confidence_label,
                  relevance_symbols_json,
                  market_impact_note,
                  interpreted_at_utc
                FROM news_interpretations
                WHERE interpreted_at_utc >= ?
                  AND model = ?
                  AND prompt_version = ?
                ORDER BY interpreted_at_utc DESC
                """,
                (
                    time_ctx.now_utc - timedelta(days=3),
                    interpreted_source_model,
                    interpreted_source_prompt,
                ),
            ).fetchall()
        pull_usage = conn.execute(
            """
            SELECT
              COALESCE(SUM(request_count), 0) AS used_today,
              MAX(created_at_utc) AS latest_pull_at
            FROM news_pull_usage
            WHERE request_date_utc = ?
            """,
            (time_ctx.now_local.date(),),
        ).fetchone()
        success_pull_usage = conn.execute(
            """
            SELECT
              COUNT(*) AS success_pulls_today,
              MAX(created_at_utc) AS latest_success_pull_at
            FROM news_pull_usage
            WHERE request_date_utc = ?
              AND response_status = 'success'
            """,
            (time_ctx.now_local.date(),),
        ).fetchone()
        latest_macro_ts = conn.execute("SELECT MAX(ts_utc) FROM macro_points").fetchone()[0]
    finally:
        conn.close()

    if not instrument_rows:
        print("no instruments found. run: python scripts/setup/load_universe.py")
        return 3

    if not bar_rows:
        print("no daily bars found in market_bars. ingest market data first.")
        return 4

    bars_by_symbol = latest_bar_by_day(bar_rows)
    market_recency_errors = _validate_market_recency(time_ctx, bars_by_symbol)
    if market_recency_errors:
        print("model_v0_market_freshness_failed")
        for err in market_recency_errors:
            print(f"error={err}")
        return 5

    # Hard required-source gate: do not score when required inputs are stale.
    source_block_reasons: list[str] = []
    market_hours = _is_market_hours(time_ctx.now_local)

    news_max_age_min_market = int(os.getenv("TRADLY_NEWS_MAX_AGE_MINUTES_MARKET", "45"))
    news_max_age_min_offhours = int(os.getenv("TRADLY_NEWS_MAX_AGE_MINUTES_OFFHOURS", "240"))
    news_min_success_pulls_market = int(os.getenv("TRADLY_NEWS_MIN_SUCCESS_PULLS_MARKET", "1"))
    news_min_success_pulls_offhours = int(os.getenv("TRADLY_NEWS_MIN_SUCCESS_PULLS_OFFHOURS", "1"))
    macro_max_age_days = int(os.getenv("TRADLY_PREFLIGHT_MACRO_MAX_AGE_DAYS", "2"))

    success_pulls_today = int(success_pull_usage[0]) if success_pull_usage and success_pull_usage[0] is not None else 0
    latest_success_pull_at = success_pull_usage[1] if success_pull_usage else None
    latest_success_pull_age = _age_seconds_from_db_ts(latest_success_pull_at, time_ctx.now_utc)
    news_max_age_sec = (news_max_age_min_market if market_hours else news_max_age_min_offhours) * 60
    news_min_success = news_min_success_pulls_market if market_hours else news_min_success_pulls_offhours

    if latest_success_pull_age is None or latest_success_pull_age > news_max_age_sec:
        source_block_reasons.append("required_source_stale:news_pull_recency")
    if success_pulls_today < news_min_success:
        source_block_reasons.append("required_source_stale:news_pull_success_missing")

    if latest_macro_ts is None:
        source_block_reasons.append("required_source_stale:macro_missing")
        macro_age_days = None
    else:
        latest_macro_ts = from_db_utc(latest_macro_ts)
        macro_age_days = int((time_ctx.now_utc.date() - latest_macro_ts.date()).days)
        if macro_age_days > macro_max_age_days:
            source_block_reasons.append("required_source_stale:macro_recency")

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "model_v0_actions.json"

    if source_block_reasons:
        blocked_payload = {
            "run_timestamp_utc": time_ctx.now_utc.isoformat(),
            "run_timestamp_local": time_ctx.now_local.isoformat(),
            "local_timezone": time_ctx.local_timezone,
            "model": "v0_price_risk_stateless",
            "model_status": "BLOCKED",
            "blocked_reason_codes": source_block_reasons,
            "source_gate": {
                "market_hours": market_hours,
                "latest_success_pull_at_utc": latest_success_pull_at.isoformat() if latest_success_pull_at else None,
                "latest_success_pull_age_sec": latest_success_pull_age,
                "news_max_age_sec": news_max_age_sec,
                "success_pulls_today": success_pulls_today,
                "news_min_success": news_min_success,
                "latest_macro_ts_utc": latest_macro_ts.isoformat() if latest_macro_ts else None,
                "macro_age_days": macro_age_days,
                "macro_max_age_days": macro_max_age_days,
            },
            "actions": [],
            "scored_count": 0,
        }
        out_path.write_text(json.dumps(blocked_payload, indent=2), encoding="utf-8")
        print("model_v0_required_source_gate_blocked")
        for reason in source_block_reasons:
            print(f"reason={reason}")
        print(f"output={out_path}")
        return 8

    news_features = compute_news_features(time_ctx.now_utc, news_rows)
    interpreted_by_symbol, interpreted_summary = compute_interpreted_news_features(time_ctx.now_utc, interpreted_rows)
    usage_today = int(pull_usage[0]) if pull_usage and pull_usage[0] is not None else 0
    latest_pull_at = pull_usage[1] if pull_usage else None
    daily_budget = _load_daily_request_budget(repo_root)
    news_guardrails = compute_news_guardrails(
        now_utc=time_ctx.now_utc,
        interpreted_summary=interpreted_summary,
        usage_today_requests=usage_today,
        latest_pull_usage_at_utc=latest_pull_at,
        daily_budget=daily_budget,
    )

    validation_errors = _validate_inputs(instrument_rows, bars_by_symbol)
    regime_context, regime_errors = compute_regime_context(bars_by_symbol)
    validation_errors.extend(regime_errors)
    if validation_errors:
        print("model_v0_input_validation_failed")
        for err in validation_errors:
            print(f"error={err}")
        return 6

    scored = []

    for symbol, halal_flag, sector in instrument_rows:
        bars = bars_by_symbol.get(symbol, [])
        result = score_symbol(
            symbol=symbol,
            halal_flag=halal_flag,
            sector=sector,
            bars=bars,
            regime_context=regime_context,
            news_feature=news_features.get(symbol),
            interpreted_feature=interpreted_by_symbol.get(symbol),
            coverage_blocked=bool(news_guardrails.get("coverage_blocked")),
            now_utc=time_ctx.now_utc,
        )
        if result is None:
            print(f"unexpected_scoring_none_for_symbol={symbol}")
            return 7
        scored.append(result)

    scored.sort(key=lambda x: x["final_score"], reverse=True)

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model": "v0_price_risk_stateless",
        "decision_mode": "symbol_tagging_no_portfolio_sizing",
        "input_summary": {
            "instrument_count": len(instrument_rows),
            "daily_bar_rows": len(bar_rows),
            "regime_symbols": list(REGIME_SYMBOLS),
            "focus_semis": list(FOCUS_SEMIS),
        },
        "regime_context": regime_context,
        "news_context": news_features,
        "interpreted_news_context": interpreted_by_symbol,
        "interpreted_news_source": {
            "model": interpreted_source_model,
            "prompt_version": interpreted_source_prompt,
            "row_count": len(interpreted_rows),
        },
        "news_guardrails": news_guardrails,
        "scored_count": len(scored),
        "actions": scored,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"output={out_path}")
    print(f"scored_count={len(scored)}")
    if scored:
        top = scored[0]
        print(
            "top=",
            {
                "symbol": top["symbol"],
                "final_action": top["final_action"],
                "final_score": top["final_score"],
                "halal_flag": top["halal_flag"],
            },
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
