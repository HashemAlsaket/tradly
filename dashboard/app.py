from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import streamlit as st

from tradly.models.recommendation import action_priority
from tradly.services.time_context import get_time_context


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = REPO_ROOT / "data" / "runs"
FRESHNESS_SNAPSHOT_PATH = REPO_ROOT / "data" / "journal" / "freshness_snapshot.json"
CYCLE_RUNS_PATH = REPO_ROOT / "data" / "journal" / "cycle_runs.jsonl"
CT_ZONE = ZoneInfo("America/Chicago")


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _fmt_age_from_iso(value: Any, now_utc: datetime) -> str:
    parsed = _parse_dt(value)
    if parsed is None:
        return "UNSET"
    delta = now_utc - parsed.astimezone(timezone.utc)
    total_seconds = max(int(delta.total_seconds()), 0)
    if total_seconds < 60:
        return f"{total_seconds}s old"
    if total_seconds < 3600:
        return f"{total_seconds // 60}m old"
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours}h {minutes}m old"


def _fmt_ct_from_iso(value: Any) -> str:
    parsed = _parse_dt(value)
    if parsed is None:
        return "UNSET"
    return parsed.astimezone(CT_ZONE).strftime("%a %Y-%m-%d %I:%M %p %Z")


def _fmt_now_ct(value: datetime) -> str:
    return value.astimezone(CT_ZONE).strftime("%a %Y-%m-%d %I:%M %p %Z")


def _fmt_session_date(value: Any) -> str:
    parsed = _parse_dt(value)
    if parsed is not None:
        return parsed.astimezone(CT_ZONE).strftime("%a %b %-d")
    if isinstance(value, str) and value.strip():
        try:
            parsed_date = datetime.fromisoformat(value.strip()).date()
            return parsed_date.strftime("%a %b %-d")
        except ValueError:
            return value
    return "UNSET"


def _fmt_market_date(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        try:
            parsed_date = datetime.fromisoformat(value.strip()).date()
            return parsed_date.strftime("%a %Y-%m-%d")
        except ValueError:
            return value
    return "UNSET"


def _render_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
          --tradly-buy: #74c69d;
          --tradly-buy-bg: rgba(116, 198, 157, 0.10);
          --tradly-sell: #f28482;
          --tradly-sell-bg: rgba(242, 132, 130, 0.10);
          --tradly-watch: #f6bd60;
          --tradly-watch-bg: rgba(246, 189, 96, 0.10);
          --tradly-card-border: rgba(255, 255, 255, 0.10);
        }
        .tradly-section-title {
          font-size: 1.7rem;
          font-weight: 800;
          line-height: 1.1;
          margin: 0 0 0.15rem 0;
        }
        .tradly-section-title.buy { color: var(--tradly-buy); }
        .tradly-section-title.sell { color: var(--tradly-sell); }
        .tradly-section-title.watch { color: var(--tradly-watch); }
        .tradly-section-subtle {
          font-size: 0.84rem;
          opacity: 0.78;
          margin-bottom: 0.55rem;
        }
        .tradly-now-line {
          font-size: 0.84rem;
          opacity: 0.7;
          margin: -0.35rem 0 0.65rem 0.05rem;
        }
        .tradly-utility-box {
          border: 1px solid rgba(255, 255, 255, 0.09);
          border-radius: 12px;
          padding: 0.45rem 0.65rem 0.15rem 0.65rem;
          background: rgba(255, 255, 255, 0.02);
          max-width: 16rem;
          margin-bottom: 0.4rem;
        }
        .tradly-command-card {
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 12px;
          padding: 0.5rem 0.7rem 0.45rem 0.7rem;
          background: rgba(255, 255, 255, 0.02);
          min-height: 4.4rem;
          margin-bottom: 0.4rem;
        }
        .tradly-command-label {
          font-size: 0.72rem;
          letter-spacing: 0.06em;
          text-transform: uppercase;
          opacity: 0.62;
          margin-bottom: 0.2rem;
        }
        .tradly-command-value {
          font-size: 1rem;
          font-weight: 800;
          line-height: 1.15;
          margin-bottom: 0.12rem;
        }
        .tradly-command-note {
          font-size: 0.79rem;
          opacity: 0.84;
          line-height: 1.25;
        }
        .tradly-command-detail {
          margin-top: 0.18rem;
          font-size: 0.72rem;
          opacity: 0.66;
          line-height: 1.2;
        }
        .tradly-card {
          border: 1px solid var(--tradly-card-border);
          border-left-width: 4px;
          border-radius: 14px;
          padding: 0.7rem 0.85rem 0.65rem 0.85rem;
          margin: 0 0 0.55rem 0;
          background: rgba(255, 255, 255, 0.02);
          max-width: 22rem;
        }
        .tradly-card.buy {
          border-left-color: var(--tradly-buy);
          background: linear-gradient(90deg, rgba(116, 198, 157, 0.12), rgba(255, 255, 255, 0.01) 22%);
          box-shadow: inset 0 0 0 1px rgba(116, 198, 157, 0.08);
        }
        .tradly-card.sell {
          border-left-color: var(--tradly-sell);
          background: linear-gradient(90deg, rgba(242, 132, 130, 0.12), rgba(255, 255, 255, 0.01) 22%);
          box-shadow: inset 0 0 0 1px rgba(242, 132, 130, 0.08);
        }
        .tradly-card.watch {
          border-left-color: var(--tradly-watch);
          background: linear-gradient(90deg, rgba(246, 189, 96, 0.14), rgba(255, 255, 255, 0.01) 22%);
          box-shadow: inset 0 0 0 1px rgba(246, 189, 96, 0.08);
        }
        .tradly-card-top {
          display: flex;
          align-items: center;
          gap: 0.55rem;
        }
        .tradly-symbol {
          font-size: 1.1rem;
          font-weight: 800;
          line-height: 1.1;
        }
        .tradly-symbol-line {
          display: flex;
          align-items: center;
          gap: 0.45rem;
          flex-wrap: wrap;
        }
        .tradly-confidence {
          font-size: 0.88rem;
          font-weight: 800;
          letter-spacing: -0.01em;
          padding: 0.1rem 0.42rem;
          border-radius: 999px;
          line-height: 1.2;
        }
        .tradly-card.buy .tradly-confidence {
          color: #173f2b;
          background: rgba(116, 198, 157, 0.95);
        }
        .tradly-card.sell .tradly-confidence {
          color: #4a1818;
          background: rgba(242, 132, 130, 0.95);
        }
        .tradly-card.watch .tradly-confidence {
          color: #4c3606;
          background: rgba(246, 189, 96, 0.95);
        }
        .tradly-confidence-label {
          display: none;
        }
        .tradly-meta {
          margin-top: 0.18rem;
          font-size: 0.8rem;
          opacity: 0.82;
        }
        .tradly-reason {
          margin-top: 0.22rem;
          font-size: 0.8rem;
          opacity: 0.78;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _load_latest_json(pattern: str) -> tuple[dict, Path | None]:
    if not RUNS_DIR.exists():
        return {}, None
    candidates = sorted(RUNS_DIR.glob(pattern))
    if not candidates:
        return {}, None
    latest = candidates[-1]
    try:
        payload = json.loads(latest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}, latest
    return payload if isinstance(payload, dict) else {}, latest


def _load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_latest_cycle_run(path: Path) -> dict:
    if not path.exists():
        return {}
    latest: dict = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            latest = payload
    return latest


def _latest_run_dir() -> Path | None:
    if not RUNS_DIR.exists():
        return None
    candidates = sorted(path for path in RUNS_DIR.iterdir() if path.is_dir())
    if not candidates:
        return None
    return candidates[-1]


def _load_latest_run_artifact(filename: str) -> tuple[dict, Path | None]:
    latest_dir = _latest_run_dir()
    if latest_dir is None:
        return {}, None
    path = latest_dir / filename
    return _load_json_file(path), path if path.exists() else path


def _latest_cycle_failed(freshness_snapshot: dict, latest_cycle_run: dict) -> bool:
    snapshot_overall = str(freshness_snapshot.get("overall_status", "UNKNOWN")).strip().upper()
    cycle_status = str(latest_cycle_run.get("status", "UNKNOWN")).strip().upper()
    if snapshot_overall == "FAIL":
        return True
    if cycle_status != "FAIL":
        return False
    cycle_started = _parse_dt(latest_cycle_run.get("started_at_utc"))
    snapshot_written = _parse_dt(freshness_snapshot.get("written_at_utc"))
    if cycle_started is None:
        return True
    if snapshot_written is None:
        return True
    return snapshot_written < cycle_started


def _quality_status(payload: dict) -> str:
    quality = payload.get("quality_audit", {})
    if not isinstance(quality, dict):
        return "missing"
    return str(quality.get("status", "missing")).strip().lower()


def _latest_run_timestamp(*payloads: dict) -> datetime | None:
    latest: datetime | None = None
    for payload in payloads:
        ts = _parse_dt(payload.get("run_timestamp_utc"))
        if ts is not None and (latest is None or ts > latest):
            latest = ts
    return latest


def _compute_system_state(
    *,
    freshness_snapshot: dict,
    market_payload: dict,
    sector_payload: dict,
    symbol_payload: dict,
    symbol_news_payload: dict,
    sector_news_payload: dict,
    range_payload: dict,
    ensemble_payload: dict,
    recommendation_payload: dict,
    review_payload: dict,
) -> tuple[str, list[str], list[str]]:
    reasons: list[str] = []
    warnings: list[str] = []

    snapshot_overall = str(freshness_snapshot.get("overall_status", "UNKNOWN")).strip().upper()
    if snapshot_overall not in {"PASS", "UNKNOWN"}:
        reasons.append(f"freshness_snapshot:{snapshot_overall.lower()}")

    market_rows = market_payload.get("rows")
    sector_rows = sector_payload.get("rows")
    symbol_rows = symbol_payload.get("rows")
    symbol_news_rows = symbol_news_payload.get("rows")
    sector_news_rows = sector_news_payload.get("rows")
    range_rows = range_payload.get("rows")
    ensemble_rows = ensemble_payload.get("rows")
    recommendation_rows = recommendation_payload.get("rows")
    review_rows = review_payload.get("rows")
    if not isinstance(market_rows, list) or not market_rows:
        reasons.append("market_regime_missing")
    if not isinstance(sector_rows, list) or not sector_rows:
        reasons.append("sector_movement_missing")
    if not isinstance(symbol_rows, list) or not symbol_rows:
        reasons.append("symbol_movement_missing")
    if not isinstance(symbol_news_rows, list) or not symbol_news_rows:
        reasons.append("symbol_news_missing")
    if not isinstance(sector_news_rows, list) or not sector_news_rows:
        reasons.append("sector_news_missing")
    if not isinstance(range_rows, list) or not range_rows:
        reasons.append("range_expectation_missing")
    if not isinstance(ensemble_rows, list) or not ensemble_rows:
        reasons.append("ensemble_missing")
    if not isinstance(recommendation_rows, list) or not recommendation_rows:
        reasons.append("recommendation_missing")
    if not isinstance(review_rows, list) or not review_rows:
        reasons.append("recommendation_review_missing")

    market_quality = _quality_status(market_payload)
    sector_quality = _quality_status(sector_payload)
    symbol_quality = _quality_status(symbol_payload)
    symbol_news_quality = _quality_status(symbol_news_payload)
    sector_news_quality = _quality_status(sector_news_payload)
    range_quality = _quality_status(range_payload)
    ensemble_quality = _quality_status(ensemble_payload)
    recommendation_quality = _quality_status(recommendation_payload)
    review_quality = _quality_status(review_payload)
    if market_quality == "fail":
        reasons.append("market_regime_quality_fail")
    elif market_quality == "missing":
        warnings.append("market_regime_quality_missing")
    if sector_quality == "fail":
        reasons.append("sector_movement_quality_fail")
    elif sector_quality == "missing":
        warnings.append("sector_movement_quality_missing")
    if symbol_quality == "fail":
        reasons.append("symbol_movement_quality_fail")
    elif symbol_quality == "missing":
        warnings.append("symbol_movement_quality_missing")
    if symbol_news_quality == "fail":
        reasons.append("symbol_news_quality_fail")
    elif symbol_news_quality == "missing":
        warnings.append("symbol_news_quality_missing")
    if sector_news_quality == "fail":
        reasons.append("sector_news_quality_fail")
    elif sector_news_quality == "missing":
        warnings.append("sector_news_quality_missing")
    if range_quality == "fail":
        reasons.append("range_expectation_quality_fail")
    elif range_quality == "missing":
        warnings.append("range_expectation_quality_missing")
    if ensemble_quality == "fail":
        reasons.append("ensemble_quality_fail")
    elif ensemble_quality == "missing":
        warnings.append("ensemble_quality_missing")
    if recommendation_quality == "fail":
        reasons.append("recommendation_quality_fail")
    elif recommendation_quality == "missing":
        warnings.append("recommendation_quality_missing")
    if review_quality == "fail":
        reasons.append("recommendation_review_quality_fail")
    elif review_quality == "missing":
        warnings.append("recommendation_review_quality_missing")

    snapshot_written_at = _parse_dt(freshness_snapshot.get("written_at_utc"))
    latest_model_run = _latest_run_timestamp(
        market_payload,
        sector_payload,
        symbol_payload,
        symbol_news_payload,
        sector_news_payload,
        range_payload,
        ensemble_payload,
        recommendation_payload,
        review_payload,
    )
    if snapshot_written_at is not None and latest_model_run is not None and snapshot_written_at < latest_model_run:
        reasons.append("freshness_snapshot_outdated_for_latest_model_runs")

    if reasons:
        warnings.extend(reasons)
        return ("research_only", [], warnings)

    research_signals = []
    for label, payload in (
        ("symbol_news", symbol_news_payload),
        ("sector_news", sector_news_payload),
        ("ensemble", ensemble_payload),
        ("recommendation", recommendation_payload),
        ("recommendation_review", review_payload),
    ):
        input_audit = payload.get("input_audit", {}) if isinstance(payload, dict) else {}
        status = str(input_audit.get("status", "")).strip().lower()
        if status and status != "ready":
            research_signals.append(f"{label}_{status}")
    if research_signals:
        warnings.extend(research_signals)
        return ("research_only", reasons, warnings)

    return ("ready", reasons, warnings)


def _render_kpi(label: str, value: str) -> None:
    st.metric(label, value)


def _market_status_copy(freshness: dict[str, Any], metrics: dict[str, Any], now_utc: datetime) -> tuple[str, str]:
    session = str(freshness.get("market_session_state", metrics.get("market_session_state", ""))).strip().lower()
    last_cash_session = metrics.get("last_cash_session_date")
    if session == "weekend":
        return "Closed weekend", f"Last cash session { _fmt_session_date(last_cash_session) }"
    if session == "holiday":
        return "Closed holiday", f"Last cash session { _fmt_session_date(last_cash_session) }"
    if session == "overnight":
        return "Overnight", f"Last cash session { _fmt_session_date(last_cash_session) }"
    if session == "pre_market":
        return "Pre-market", f"Last cash session { _fmt_session_date(last_cash_session) }"
    if session == "after_hours":
        return "After hours", f"Last cash session { _fmt_session_date(last_cash_session) }"
    if session == "market_hours":
        return "Market open", "Live cash session"
    return _fmt_age_from_iso(metrics.get("latest_daily_bar_utc"), now_utc), "Market session unclear"


def _market_tape_caution(market_payload: dict[str, Any] | None) -> str:
    if not isinstance(market_payload, dict):
        return "Tape state unavailable"
    rows = market_payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return "Tape state unavailable"
    row = rows[0] if isinstance(rows[0], dict) else {}
    signal = str(row.get("signal_direction", "")).strip().lower()
    confidence = int(row.get("confidence_score", 0) or 0)
    why_codes = {str(code).strip().lower() for code in row.get("why_code", []) if str(code).strip()}
    if signal == "bearish" and confidence >= 55:
        if {"vix_elevated", "macro_rates_pressure", "macro_energy_stress"} & why_codes:
            return "Hostile tape"
        return "Macro unstable"
    if signal == "neutral":
        return "Macro mixed"
    if signal == "bullish" and confidence >= 55:
        return "Risk-on improving"
    return "Risk-on not confirmed"


def _action_section_blurb(title: str, market_payload: dict[str, Any] | None = None) -> str:
    if title != "Buy":
        return {
            "Sell / Trim": "Best bearish or reduce-risk setups.",
            "Watch": "Worth monitoring, not ready yet.",
        }.get(title, "")
    if not isinstance(market_payload, dict):
        return "Best bullish setups."
    rows = market_payload.get("rows")
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        return "Best bullish setups."
    evidence = rows[0].get("evidence")
    macro_hostility = evidence.get("macro_hostility") if isinstance(evidence, dict) else None
    macro_state = str(macro_hostility.get("macro_state", "")).strip().lower() if isinstance(macro_hostility, dict) else ""
    signal = str(rows[0].get("signal_direction", "")).strip().lower()
    confidence = int(rows[0].get("confidence_score", 0) or 0)
    if macro_state in {"risk_off", "macro_unstable"}:
        if signal == "bearish" and confidence >= 70:
            return "Only the strongest survivors. Hostile tape, stay selective."
        return "Bullish setups, macro not confirmed."
    return "Best bullish setups."


def _market_stress_level(market_payload: dict[str, Any] | None) -> str:
    if not isinstance(market_payload, dict):
        return "low"
    rows = market_payload.get("rows")
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        return "low"
    row = rows[0]
    signal = str(row.get("signal_direction", "")).strip().lower()
    confidence = int(row.get("confidence_score", 0) or 0)
    why_codes = {str(code).strip().lower() for code in row.get("why_code", []) if str(code).strip()}
    if signal == "bearish" and confidence >= 70 and {"vix_elevated", "macro_rates_pressure", "macro_energy_stress"} & why_codes:
        return "high"
    if signal == "bearish" and confidence >= 60:
        return "medium"
    return "low"


def _freshness_brief(value: Any, now_utc: datetime) -> str:
    age_text = _fmt_age_from_iso(value, now_utc)
    parsed = _parse_dt(value)
    if parsed is None:
        return age_text
    age_hours = max(0.0, (now_utc - parsed.astimezone(timezone.utc)).total_seconds() / 3600.0)
    if age_hours <= 18:
        return "Fresh today"
    if age_hours <= 36:
        return "Fresh yesterday"
    return age_text


def _render_status_card(label: str, value: str, note: str, detail: str | None = None) -> None:
    detail_html = f'<div class="tradly-status-detail">{detail}</div>' if detail else ""
    st.markdown(
        f"""
        <div class="tradly-command-card">
          <div class="tradly-command-label">{label}</div>
          <div class="tradly-command-value">{value}</div>
          <div class="tradly-command-note">{note}</div>
          <div class="tradly-command-detail">{detail or ''}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _latest_cycle_failure_notice(freshness_snapshot: dict[str, Any], latest_cycle_run: dict[str, Any]) -> str | None:
    if not isinstance(latest_cycle_run, dict) or not latest_cycle_run:
        return None
    latest_status = str(latest_cycle_run.get("status", "")).strip().upper()
    if latest_status != "FAIL":
        return None
    cycle_started = _parse_dt(latest_cycle_run.get("started_at_utc"))
    snapshot_written = _parse_dt(freshness_snapshot.get("written_at_utc"))
    if cycle_started is None:
        return None
    if snapshot_written is not None and cycle_started <= snapshot_written:
        return None
    reason = str(latest_cycle_run.get("reason", "unknown")).strip() or "unknown"
    return (
        f"Latest refresh failed at {_fmt_ct_from_iso(latest_cycle_run.get('started_at_utc'))}. "
        f"Showing last successful snapshot from {_fmt_ct_from_iso(freshness_snapshot.get('written_at_utc'))}. "
        f"Reason: `{reason}`."
    )


def _render_top_status(snapshot: dict, now_utc: datetime, state: str, reasons: list[str], warnings: list[str]) -> None:
    freshness = snapshot.get("freshness", {}) if isinstance(snapshot, dict) else {}
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}
    market_value, market_note = _market_status_copy(freshness, metrics, now_utc)
    short_term_ready = bool(metrics.get("short_horizon_execution_ready", False))
    short_value = "Active" if short_term_ready else "Deferred"
    short_note = "Tactical 1-3 day execution is live." if short_term_ready else "Tactical 1-3 day execution waits for the next cash session."
    medium_ready = bool(metrics.get("medium_horizon_thesis_usable", False))
    medium_value = "Active" if medium_ready else "Limited"
    medium_note = (
        f"Medium-horizon thesis is active. News { _freshness_brief(metrics.get('latest_news_pull_utc'), now_utc) }, LLM { _freshness_brief(metrics.get('latest_interp_utc'), now_utc) }."
        if medium_ready
        else "Medium-horizon thesis is limited until fresher context arrives."
    )
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_status_card(
            "Market",
            market_value,
            market_note,
            f"Now: {_fmt_now_ct(now_utc)}<br/>Latest market data: {_fmt_ct_from_iso(metrics.get('latest_daily_bar_utc'))}",
        )
    with c2:
        _render_status_card(
            "Short-Term",
            short_value,
            short_note,
            f"Now: {_fmt_now_ct(now_utc)}<br/>Latest market data: {_fmt_ct_from_iso(metrics.get('latest_daily_bar_utc'))}",
        )
    with c3:
        _render_status_card(
            "1-2w / 2-6w",
            medium_value,
            medium_note,
            f"Now: {_fmt_now_ct(now_utc)}<br/>Latest news: {_fmt_ct_from_iso(metrics.get('latest_news_pull_utc'))}<br/>Latest LLM review: {_fmt_ct_from_iso(metrics.get('latest_interp_utc'))}",
        )
    if state == "ready":
        st.success("Ready")


def _render_market_context_compact(payload: dict) -> None:
    rows = payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        return
    row = rows[0]
    st.subheader("Market Context")
    st.write(
        f"{str(row.get('signal_direction', 'unset')).upper()} | "
        f"score {row.get('score_normalized', 'UNSET')} | "
        f"confidence {row.get('confidence_score', 'UNSET')}"
    )


def _summarize_horizon_states(ensemble_payload: dict, global_state: str) -> list[dict[str, object]]:
    rows = ensemble_payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        return []
    summaries: list[dict[str, object]] = []
    for horizon in ("1to3d", "1to2w", "2to6w"):
        counts: dict[str, int] = {}
        for row in rows:
            horizon_summary = row.get("horizon_summary", {}) if isinstance(row, dict) else {}
            horizon_row = horizon_summary.get(horizon, {}) if isinstance(horizon_summary, dict) else {}
            state = str(horizon_row.get("state", "missing"))
            counts[state] = counts.get(state, 0) + 1
        dominant_state = max(counts, key=counts.get) if counts else "missing"
        summaries.append(
            {
                "Horizon": horizon,
                "State": dominant_state,
                "Actionable": counts.get("actionable", 0),
                "Research": counts.get("research_only", 0),
                "Blocked": counts.get("blocked", 0),
                "Not Supported": counts.get("not_supported", 0),
            }
        )
    return summaries


def _top_horizon_reasons(ensemble_payload: dict, horizon: str, state: str, *, limit: int = 4) -> list[str]:
    rows = ensemble_payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        return []
    counts: dict[str, int] = {}
    for row in rows:
        horizon_summary = row.get("horizon_summary", {}) if isinstance(row, dict) else {}
        horizon_row = horizon_summary.get(horizon, {}) if isinstance(horizon_summary, dict) else {}
        if str(horizon_row.get("state", "")) != state:
            continue
        for code in horizon_row.get("why_code", []):
            text = str(code).strip()
            if text:
                counts[text] = counts.get(text, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [f"{reason} ({count})" for reason, count in ranked[:limit]]


def _humanize_reason(code: str) -> str:
    text = str(code).strip().replace("_", " ")
    replacements = {
        "market context headwind": "market headwind",
        "market context supportive": "market support",
        "sector context supportive": "sector support",
        "sector context headwind": "sector headwind",
        "sector news supportive": "sector news support",
        "sector news headwind": "sector news headwind",
        "symbol news supports bullish": "symbol news support",
        "symbol news supports bearish": "symbol news headwind",
        "symbol movement supports bullish": "price support",
        "symbol movement supports bearish": "price weakness",
        "component conflict high": "mixed signals",
        "range expanding conviction reduced": "wide range",
        "market closed weekend": "market closed for weekend",
        "market closed holiday": "market closed for holiday",
        "event buy capped to watch": "event damaged; wait for confirmation",
        "event reaction damage": "event damage; reassess before acting",
        "event reaction caution": "event caution; let reaction settle",
        "market stress watch": "high-quality name blocked by hostile tape",
        "mixed cautious actionable": "cautious but actionable",
        "risk off survivor": "surviving long despite hostile tape",
        "recommendation not actionable": "not actionable yet",
        "healthcare probationary modeled": "newly onboarded; keep on watch",
        "healthcare thin evidence": "thin evidence; wait for confirmation",
    }
    return replacements.get(text, text)


def _humanize_bucket(code: str) -> str:
    text = str(code).strip()
    return {
        "top_longs": "Top Long",
        "top_shorts": "Top Short",
        "top_ideas": "Top Idea",
        "contrarian_review": "Contrarian Review",
        "contrarian_rebound": "Contrarian Rebound",
        "review_high_priority": "High-Priority Review",
        "manual_review": "Manual Review",
        "watch_event_damaged": "Watch - Event Damaged",
        "watch_tape_blocked": "Watch - Tape Blocked",
        "watch_needs_confirmation": "Watch - Needs Confirmation",
        "deferred": "Deferred",
        "blocked": "Blocked",
    }.get(text, text.replace("_", " ").title())


def _watch_bucket_title(bucket: str) -> str:
    return {
        "watch_event_damaged": "Watch - Damaged",
        "watch_tape_blocked": "Watch - Tape Blocked",
        "watch_needs_confirmation": "Watch - Needs Confirmation",
        "review_high_priority": "Review - High Priority",
        "manual_review": "Review - Manual",
        "deferred": "Deferred",
    }.get(str(bucket).strip(), "Watch")


def _format_horizon_label(horizon: str) -> str:
    return {
        "1to3d": "1-3 days",
        "1to2w": "1-2 weeks",
        "2to6w": "2-6 weeks",
    }.get(str(horizon), str(horizon))


def _horizon_lane_name(horizon: str) -> str:
    return {
        "1to3d": "tactical",
        "1to2w": "swing",
        "2to6w": "position",
    }.get(str(horizon), "unknown")


def _decision_rows(review_payload: dict) -> list[dict[str, Any]]:
    rows = review_payload.get("rows", [])
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "Symbol": str(row.get("symbol") or row.get("scope_id", "UNSET")),
                "Sector": str(row.get("sector", "UNSET")),
                "Action": str(row.get("recommended_action", "Unknown")),
                "Horizon": str(row.get("recommended_horizon", "UNSET")),
                "Confidence": int(row.get("display_confidence_score", row.get("confidence_score", 0)) or 0),
                "RawConfidence": int(row.get("confidence_score", 0) or 0),
                "Reason": _humanize_reason(str(row.get("primary_reason_code", ""))),
                "ExecutionReady": bool(row.get("execution_ready", True)),
                "RecommendationClass": str(row.get("recommendation_class", "unknown")),
                "ReviewDisposition": str(row.get("review_disposition", "watch")),
                "ReviewBucket": str(row.get("review_bucket", "watch_needs_confirmation")),
                "ReviewState": _humanize_bucket(str(row.get("review_bucket", "watch_needs_confirmation"))),
                "ReviewReason": str(row.get("review_reason_code", "")),
                "ReviewReasonText": _humanize_reason(str(row.get("review_reason_code", ""))),
                "EventActive": bool(row.get("event_active", False)),
                "EventReaction": str(row.get("event_reaction_state", "")),
                "MarketStress": str(row.get("market_stress_level", "")),
            }
        )
    return sorted(
        out,
        key=lambda row: (
            {"promote": 3, "review_required": 2, "watch": 1, "defer": 0, "blocked": -1}.get(str(row["ReviewDisposition"]), -1),
            action_priority(str(row["Action"])),
            int(row["RawConfidence"]),
        ),
        reverse=True,
    )


def _render_action_list(title: str, rows: list[dict[str, Any]], market_payload: dict[str, Any] | None = None) -> None:
    section_class = {
        "Buy": "buy",
        "Sell / Trim": "sell",
        "Watch": "watch",
    }.get(title, "watch")
    if title:
        st.markdown(
            f'<div class="tradly-section-title {section_class}">{title}</div>',
            unsafe_allow_html=True,
        )
        blurb = _action_section_blurb(title, market_payload)
        if blurb:
            st.markdown(f'<div class="tradly-section-subtle">{blurb}</div>', unsafe_allow_html=True)
    if not rows:
        st.caption("None")
        return
    if title:
        st.markdown(f'<div class="tradly-section-subtle">{len(rows)} shown</div>', unsafe_allow_html=True)

    def _render_rows(group_rows: list[dict[str, Any]]) -> None:
        for row in group_rows:
            symbol = str(row["Symbol"])
            horizon = str(row["Horizon"])
            confidence = int(row["Confidence"])
            reason = str(row.get("ReviewReasonText") or row["Reason"]).strip()
            execution_ready = bool(row.get("ExecutionReady", True))
            horizon_label = _format_horizon_label(horizon)
            lane_name = _horizon_lane_name(horizon)
            if execution_ready:
                context_note = f"{lane_name.title()} • {horizon_label}"
            else:
                context_note = f"{lane_name.title()} • {horizon_label} • deferred until next session"
            st.markdown(
                f"""
                <div class="tradly-card {section_class}">
                  <div class="tradly-card-top">
                    <div class="tradly-symbol-line">
                      <div class="tradly-symbol">{symbol}</div>
                      <div class="tradly-confidence">{confidence}</div>
                    </div>
                  </div>
                  <div class="tradly-meta">{context_note}</div>
                  <div class="tradly-reason">{reason}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    _render_rows(rows)


def _render_watch_groups(rows: list[dict[str, Any]], market_payload: dict[str, Any] | None = None) -> None:
    st.markdown('<div class="tradly-section-title watch">Watch</div>', unsafe_allow_html=True)
    blurb = _action_section_blurb("Watch", market_payload)
    if blurb:
        st.markdown(f'<div class="tradly-section-subtle">{blurb}</div>', unsafe_allow_html=True)
    if not rows:
        st.caption("None")
        return
    st.markdown(f'<div class="tradly-section-subtle">{len(rows)} shown</div>', unsafe_allow_html=True)
    ordered_buckets = [
        "watch_event_damaged",
        "watch_tape_blocked",
        "watch_needs_confirmation",
        "review_high_priority",
        "manual_review",
        "deferred",
    ]
    for bucket in ordered_buckets:
        bucket_rows = [row for row in rows if str(row.get("ReviewBucket")) == bucket]
        if not bucket_rows:
            continue
        st.caption(_watch_bucket_title(bucket))
        _render_action_list("", bucket_rows, market_payload)


def _render_action_board(review_payload: dict, market_payload: dict[str, Any] | None = None) -> None:
    ranked_rows = _decision_rows(review_payload)
    if not ranked_rows:
        st.caption("No decisions available.")
        return
    market_stress = _market_stress_level(market_payload)
    promote_or_review_sell = {
        str(row["Symbol"])
        for row in ranked_rows
        if str(row["Action"]) == "Sell/Trim" and str(row["ReviewDisposition"]) in {"promote", "review_required"}
    }
    buy_rows = sorted(
        [
            row
            for row in ranked_rows
            if str(row["Action"]) == "Buy"
            and (
                str(row["ReviewDisposition"]) == "promote"
                or (market_stress == "low" and str(row["ReviewDisposition"]) == "review_required")
            )
        ],
        key=lambda row: (
            1 if str(row["ReviewDisposition"]) == "promote" else 0,
            int(row["Confidence"]),
        ),
        reverse=True,
    )[: (4 if market_stress == "high" else 6)]
    sell_rows = sorted(
        [
            row
            for row in ranked_rows
            if str(row["Action"]) == "Sell/Trim" and str(row["ReviewDisposition"]) in {"promote", "review_required"}
        ],
        key=lambda row: (
            1 if str(row["ReviewDisposition"]) == "promote" else 0,
            int(row["Confidence"]),
        ),
        reverse=True,
    )[:6]
    watch_rows = sorted(
        [
            row
            for row in ranked_rows
            if str(row["ReviewDisposition"]) in {"watch", "defer"}
            or (
                str(row["ReviewDisposition"]) == "review_required"
                and (
                    str(row["Action"]) not in {"Buy", "Sell/Trim"}
                    or market_stress in {"medium", "high"}
                )
                and str(row["Symbol"]) not in promote_or_review_sell
            )
        ],
        key=lambda row: int(row["Confidence"]),
        reverse=True,
    )[:8]
    c1, c2, c3 = st.columns([0.96, 0.96, 1.08], gap="medium")
    with c1:
        _render_action_list("Buy", buy_rows, market_payload)
    with c2:
        _render_action_list("Sell / Trim", sell_rows, market_payload)
    with c3:
        _render_watch_groups(watch_rows, market_payload)


def _portfolio_rows(payload: dict) -> list[dict[str, Any]]:
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "Symbol": str(row.get("symbol", "UNSET")),
                "Action": str(row.get("action_recommendation", "do_not_trade")),
                "CurrentWt": float(row.get("current_weight", 0.0) or 0.0),
                "TargetWt": float(row.get("target_weight", 0.0) or 0.0),
                "DeltaWt": float(row.get("weight_delta", 0.0) or 0.0),
                "Tier": str(row.get("idea_tier", "tier_blocked")),
                "Theme": str(row.get("theme_id", "unknown")),
                "Horizon": str(row.get("horizon_bucket", "unknown")),
                "ExecState": str(row.get("action_execution_state", "informational")),
                "Size": str(row.get("action_size_recommendation", "")),
                "ReasonCodes": ", ".join(row.get("policy_reason_codes", [])),
                "Blockers": ", ".join(row.get("policy_blockers", [])),
                "Confidence": int(row.get("confidence_score", 0) or 0),
            }
        )
    return sorted(
        out,
        key=lambda row: (
            {"buy": 6, "add": 5, "trim": 4, "exit": 4, "hold": 3, "do_not_trade": 2}.get(str(row["Action"]), 1),
            abs(float(row["DeltaWt"])),
            int(row["Confidence"]),
        ),
        reverse=True,
    )


def _render_portfolio_policy(payload: dict) -> None:
    if not payload:
        st.warning("Portfolio policy artifact missing.")
        return
    rows = _portfolio_rows(payload)
    if not rows:
        st.warning("Portfolio policy rows missing.")
        return

    st.subheader("Portfolio Policy")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _render_kpi("Portfolio Mode", str(payload.get("portfolio_mode", "UNSET")).replace("_", " ").title())
    with c2:
        _render_kpi("Current Gross Long", f"{float(payload.get('current_gross_long_exposure', 0.0) or 0.0) * 100:.1f}%")
    with c3:
        _render_kpi("Target Gross Long", f"{float(payload.get('target_gross_long_exposure', 0.0) or 0.0) * 100:.1f}%")
    with c4:
        _render_kpi("Available Cash", f"${float(payload.get('available_cash', 0.0) or 0.0):,.0f}")

    reason_codes = payload.get("portfolio_mode_reason_codes", [])
    if reason_codes:
        st.caption(f"Mode reasons: {', '.join(str(code) for code in reason_codes)}")

    violation_counts = payload.get("policy_violation_counts", {})
    if isinstance(violation_counts, dict):
        st.caption(
            "Policy violations: "
            f"single-name {violation_counts.get('single_name', 0)}, "
            f"theme {violation_counts.get('theme', 0)}, "
            f"horizon {violation_counts.get('horizon', 0)}, "
            f"blocked rows {violation_counts.get('blocked_rows', 0)}"
        )

    buys = [row for row in rows if row["Action"] in {"buy", "add"}][:8]
    risk = [row for row in rows if row["Action"] in {"trim", "exit"}][:8]

    left, right = st.columns(2)
    with left:
        st.markdown("**Top Buys / Adds**")
        if buys:
            st.dataframe(
                [
                    {
                        "Symbol": row["Symbol"],
                        "Action": row["Action"],
                        "Tier": row["Tier"],
                        "Current": f"{row['CurrentWt'] * 100:.1f}%",
                        "Target": f"{row['TargetWt'] * 100:.1f}%",
                        "Delta": f"{row['DeltaWt'] * 100:.1f}%",
                        "Exec": row["ExecState"],
                        "Size": row["Size"],
                    }
                    for row in buys
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No buy/add candidates.")
    with right:
        st.markdown("**Top Trims / Exits**")
        if risk:
            st.dataframe(
                [
                    {
                        "Symbol": row["Symbol"],
                        "Action": row["Action"],
                        "Tier": row["Tier"],
                        "Current": f"{row['CurrentWt'] * 100:.1f}%",
                        "Target": f"{row['TargetWt'] * 100:.1f}%",
                        "Delta": f"{row['DeltaWt'] * 100:.1f}%",
                        "Exec": row["ExecState"],
                        "Size": row["Size"],
                    }
                    for row in risk
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No trim/exit actions.")

    st.markdown("**All Portfolio Actions**")
    st.dataframe(
        [
            {
                "Symbol": row["Symbol"],
                "Action": row["Action"],
                "Current Weight": f"{row['CurrentWt'] * 100:.1f}%",
                "Target Weight": f"{row['TargetWt'] * 100:.1f}%",
                "Delta": f"{row['DeltaWt'] * 100:.1f}%",
                "Tier": row["Tier"],
                "Theme": row["Theme"],
                "Horizon": row["Horizon"],
                "Execution": row["ExecState"],
                "Reason Codes": row["ReasonCodes"],
                "Blockers": row["Blockers"],
            }
            for row in rows
        ],
        use_container_width=True,
        hide_index=True,
    )


def _render_horizon_landscape(ensemble_payload: dict, global_state: str) -> None:
    horizon_rows = _summarize_horizon_states(ensemble_payload, global_state)
    if not horizon_rows:
        st.caption("Horizon summary unavailable.")
        return
    blurbs = {
        "1to3d": "tactical lane",
        "1to2w": "swing lane",
        "2to6w": "position lane",
    }
    cols = st.columns(len(horizon_rows))
    for col, row in zip(cols, horizon_rows):
        with col:
            horizon = str(row["Horizon"])
            st.metric(
                _format_horizon_label(horizon),
                f"{row['Actionable']} actionable",
                f"{row['Research']} watch",
            )
            st.caption(blurbs.get(horizon, ""))


def _render_freshness(snapshot: dict) -> None:
    if not snapshot:
        st.warning("Freshness snapshot missing.")
        return
    freshness = snapshot.get("freshness", {}) if isinstance(snapshot, dict) else {}
    checks = freshness.get("checks", []) if isinstance(freshness, dict) else []
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}
    st.subheader("Freshness")
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_kpi("Latest Market Bar", _fmt_ct_from_iso(metrics.get("latest_daily_bar_utc")))
    with c2:
        _render_kpi("Latest News Pull", _fmt_ct_from_iso(metrics.get("latest_news_pull_utc")))
    with c3:
        _render_kpi("Latest Interpretation", _fmt_ct_from_iso(metrics.get("latest_interp_utc")))
    for check in checks:
        st.caption(f"{check.get('name', 'UNKNOWN')}: {check.get('status', 'UNKNOWN')}")


def _render_market_regime(payload: dict) -> None:
    rows = payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        st.warning("Market regime artifact missing.")
        return
    row = rows[0]
    quality = payload.get("quality_audit", {})
    st.subheader("Market Regime")
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_kpi("Direction", str(row.get("signal_direction", "UNSET")).upper())
    with c2:
        _render_kpi("Score", str(row.get("score_normalized", "UNSET")))
    with c3:
        _render_kpi("Confidence", f"{row.get('confidence_score', 'UNSET')} / 100")
    st.write("Why:", ", ".join(row.get("why_code", [])) or "UNSET")
    st.write("As of:", _fmt_ct_from_iso(row.get("as_of_utc")))
    if isinstance(quality, dict):
        st.write("Quality Audit:", f"{quality.get('status', 'UNSET').upper()} {quality.get('failure_reasons', [])}")


def _render_ensemble_summary(payload: dict) -> None:
    rows = payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        st.warning("Ensemble artifact missing.")
        return
    st.subheader("Ensemble Snapshot")
    quality = payload.get("quality_audit", {})
    input_audit = payload.get("input_audit", {})
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_kpi("Rows", str(len(rows)))
    with c2:
        _render_kpi("Quality", str(quality.get("status", "UNSET")).upper())
    with c3:
        _render_kpi("Input Status", str(input_audit.get("status", "UNSET")).upper())
    st.caption("Use Decisions for actions. This is just system summary.")


def _render_symbol_stack(review_payload: dict) -> None:
    top_symbols = _decision_rows(review_payload)
    if not top_symbols:
        st.caption("No modeled symbols available.")
        return
    st.markdown("**Modeled Universe**")
    table_rows = []
    for row in top_symbols:
        table_rows.append(
            {
                "Symbol": row["Symbol"],
                "Sector": row["Sector"],
                "Action": row["Action"],
                "Horizon": row["Horizon"],
                "Confidence": row["Confidence"],
                "State": row["ReviewState"],
                "Review Reason": row["ReviewReasonText"],
                "Event": row["EventReaction"] if row["EventActive"] else "",
                "Market Stress": row["MarketStress"],
                "Why": row["Reason"],
            }
        )
    st.dataframe(table_rows, use_container_width=True, hide_index=True)


def _render_sector_table(payload: dict) -> None:
    rows = payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        st.warning("Sector movement artifact missing.")
        return
    st.subheader("Sector Movement")
    table_rows = []
    for row in rows:
        diagnostics = row.get("diagnostics", {}) if isinstance(row.get("diagnostics"), dict) else {}
        table_rows.append(
            {
                "Sector": row.get("scope_id", "UNSET"),
                "Direction": row.get("signal_direction", "UNSET"),
                "Score": row.get("score_normalized", "UNSET"),
                "Confidence": row.get("confidence_score", "UNSET"),
                "Coverage": row.get("coverage_state", "UNSET"),
                "Cap Reasons": ", ".join(diagnostics.get("cap_reasons", [])),
                "Latency": (diagnostics.get("latency_assessment", {}) or {}).get("latency_class", "UNSET"),
            }
        )
    st.dataframe(table_rows, use_container_width=True, hide_index=True)


def _render_ops(market_payload: dict, sector_payload: dict, symbol_payload: dict, symbol_news_payload: dict, sector_news_payload: dict, range_payload: dict, ensemble_payload: dict) -> None:
    st.subheader("Ops")
    for label, payload in (
        ("Market Regime", market_payload),
        ("Sector Movement", sector_payload),
        ("Symbol Movement", symbol_payload),
        ("Symbol News", symbol_news_payload),
        ("Sector News", sector_news_payload),
        ("Range Expectation", range_payload),
        ("Ensemble", ensemble_payload),
    ):
        quality = payload.get("quality_audit", {})
        input_audit = payload.get("input_audit", {})
        st.markdown(f"**{label}**")
        if isinstance(quality, dict):
            st.write("Quality:", quality.get("status", "UNSET"), quality.get("failure_reasons", []))
        if isinstance(input_audit, dict) and input_audit:
            st.write("Input Audit:", input_audit.get("status", "UNSET"))


def main() -> None:
    st.set_page_config(page_title="tradly dashboard", layout="wide")
    _render_theme()
    time_ctx = get_time_context()
    freshness_snapshot = _load_json_file(FRESHNESS_SNAPSHOT_PATH)
    latest_cycle_run = _load_latest_cycle_run(CYCLE_RUNS_PATH)
    market_payload, _ = _load_latest_run_artifact("market_regime_v1.json")
    sector_payload, _ = _load_latest_run_artifact("sector_movement_v1.json")
    symbol_payload, _ = _load_latest_run_artifact("symbol_movement_v1.json")
    symbol_news_payload, _ = _load_latest_run_artifact("symbol_news_v1.json")
    sector_news_payload, _ = _load_latest_run_artifact("sector_news_v1.json")
    range_payload, _ = _load_latest_run_artifact("range_expectation_v1.json")
    ensemble_payload, _ = _load_latest_run_artifact("ensemble_v1.json")
    recommendation_payload, _ = _load_latest_run_artifact("recommendation_v1.json")
    review_payload, _ = _load_latest_run_artifact("recommendation_review_v1.json")
    portfolio_payload, _ = _load_latest_run_artifact("portfolio_policy_v1.json")

    state, reasons, warnings = _compute_system_state(
        freshness_snapshot=freshness_snapshot,
        market_payload=market_payload,
        sector_payload=sector_payload,
        symbol_payload=symbol_payload,
        symbol_news_payload=symbol_news_payload,
        sector_news_payload=sector_news_payload,
        range_payload=range_payload,
        ensemble_payload=ensemble_payload,
        recommendation_payload=recommendation_payload,
        review_payload=review_payload,
    )

    st.title("tradly")
    st.markdown(f'<div class="tradly-now-line">Now { _fmt_now_ct(time_ctx.now_utc) }</div>', unsafe_allow_html=True)
    latest_failure_notice = _latest_cycle_failure_notice(freshness_snapshot, latest_cycle_run)
    if latest_failure_notice:
        st.error(latest_failure_notice)
    failed_cycle_decisions = _latest_cycle_failed(freshness_snapshot, latest_cycle_run)
    if failed_cycle_decisions:
        review_payload = {}
        recommendation_payload = {}

    freshness = freshness_snapshot.get("freshness", {}) if isinstance(freshness_snapshot, dict) else {}
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}

    bar1, bar2, bar3, bar4 = st.columns([0.95, 1.2, 1.15, 1.3])
    with bar1:
        st.markdown('<div class="tradly-utility-box">', unsafe_allow_html=True)
        section = st.selectbox(
            "Navigate",
            ["Decisions", "Universe", "Portfolio", "Market", "System"],
            index=0,
            label_visibility="collapsed",
            key="top_nav",
        )
        show_more_symbols = st.toggle("More Symbols", value=False, key="show_more_symbols")
        st.markdown("</div>", unsafe_allow_html=True)
    with bar2:
        market_value, market_note = _market_status_copy(freshness, metrics, time_ctx.now_utc)
        _render_status_card(
            "Market",
            market_value,
            _market_tape_caution(market_payload),
            (
                f"{market_note}<br/>"
                f"Daily market date: {_fmt_market_date(metrics.get('latest_daily_bar_market_date'))}<br/>"
                f"Intraday: {_fmt_ct_from_iso(metrics.get('latest_intraday_bar_utc'))}"
            ),
        )
    with bar3:
        short_term_ready = bool(metrics.get("short_horizon_execution_ready", False))
        _render_status_card(
            "Short-Term",
            "Active" if short_term_ready else "Deferred",
            "Execution live" if short_term_ready else "Execution waits for next cash session",
            (
                f"Intraday: {_fmt_ct_from_iso(metrics.get('latest_intraday_bar_utc'))}<br/>"
                f"Snapshot: {_fmt_ct_from_iso(metrics.get('latest_snapshot_utc'))}"
            ),
        )
    with bar4:
        medium_ready = bool(metrics.get("medium_horizon_thesis_usable", False))
        medium_note = "Thesis active" if medium_ready else "Thesis limited"
        _render_status_card(
            "1-2w / 2-6w",
            "Active" if medium_ready else "Limited",
            medium_note,
            f"News: {_fmt_ct_from_iso(metrics.get('latest_news_pull_utc'))}<br/>LLM: {_fmt_ct_from_iso(metrics.get('latest_interp_utc'))}",
        )

    if state == "ready":
        st.success("Ready")

    if section == "Decisions":
        if failed_cycle_decisions:
            st.caption("Decisions withheld because the latest cycle failed postflight validation.")
        _render_action_board(review_payload if review_payload else recommendation_payload, market_payload)
        if show_more_symbols:
            _render_symbol_stack(review_payload if review_payload else recommendation_payload)
    elif section == "Universe":
        _render_symbol_stack(review_payload if review_payload else recommendation_payload)
    elif section == "Portfolio":
        _render_portfolio_policy(portfolio_payload)
    elif section == "Market":
        _render_market_context_compact(market_payload)
        _render_horizon_landscape(ensemble_payload, state)
        _render_ensemble_summary(ensemble_payload)
    else:
        _render_freshness(freshness_snapshot)
        _render_ops(
            market_payload,
            sector_payload,
            symbol_payload,
            symbol_news_payload,
            sector_news_payload,
            range_payload,
            ensemble_payload,
        )


if __name__ == "__main__":
    main()
