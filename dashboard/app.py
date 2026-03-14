from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import streamlit as st

from tradly.services.time_context import get_time_context


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = REPO_ROOT / "data" / "runs"
EARNINGS_WATCHLIST_PATH = REPO_ROOT / "data" / "manual" / "earnings_watchlist.json"
FRESHNESS_SNAPSHOT_PATH = REPO_ROOT / "data" / "journal" / "freshness_snapshot.json"
SEMIS_FOCUS_SYMBOLS = ("MU", "SNDK", "NVDA", "NVTS")
ET_ZONE = ZoneInfo("America/New_York")
CT_ZONE = ZoneInfo("America/Chicago")
DB_PATH = REPO_ROOT / "data" / "tradly.duckdb"


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .main .block-container {
            padding-top: 1.4rem;
            padding-bottom: 2rem;
            max-width: 860px;
        }
        h1, h2, h3 {
            letter-spacing: -0.03em;
        }
        .hero {
            background: linear-gradient(135deg, #fffdf8 0%, #f7f3ea 100%);
            border: 1px solid #e7dfcf;
            border-radius: 18px;
            padding: 1rem 1rem 0.9rem 1rem;
            margin-bottom: 1rem;
        }
        .hero-title {
            font-size: 0.8rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #6b7280;
            margin-bottom: 0.4rem;
        }
        .hero-value {
            font-size: 1.9rem;
            font-weight: 800;
            color: #111827;
            line-height: 1.1;
        }
        .hero-note {
            margin-top: 0.45rem;
            color: #4b5563;
            font-size: 0.95rem;
        }
        .mini-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.5rem;
            margin: 0.3rem 0 0.15rem 0;
        }
        .mini-card {
            background: linear-gradient(180deg, rgba(17,24,39,0.92) 0%, rgba(15,23,42,0.82) 100%);
            border: 1px solid #243041;
            border-radius: 12px;
            padding: 0.5rem 0.6rem;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.02);
        }
        .move-card {
            border-radius: 14px;
            padding: 0.55rem 0.7rem;
            margin-bottom: 0.4rem;
            border: 1px solid #e5e7eb;
            background: #ffffff;
        }
        .lane-title {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.75rem;
            margin-bottom: 0.65rem;
        }
        .lane-count {
            color: #94a3b8;
            font-size: 0.78rem;
            font-weight: 700;
        }
        .section-chip {
            display: inline-block;
            padding: 0.26rem 0.62rem;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            margin-bottom: 0.6rem;
            border: 1px solid #e5e7eb;
        }
        .chip-buy {
            color: #166534;
            background: #f0fdf4;
            border-color: #bbf7d0;
        }
        .chip-trim {
            color: #92400e;
            background: #fffbeb;
            border-color: #fde68a;
        }
        .rail-title {
            font-size: 0.74rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: #9ca3af;
            margin-bottom: 0.65rem;
        }
        .move-buy {
            border-left: 6px solid #16a34a;
            background: linear-gradient(135deg, #0f1d14 0%, #12281a 100%);
            border-color: #1f5130;
        }
        .move-trim {
            border-left: 6px solid #d97706;
            background: linear-gradient(135deg, #22180b 0%, #2a1d0d 100%);
            border-color: #6b4d16;
        }
        .move-exit {
            border-left: 6px solid #dc2626;
            background: linear-gradient(135deg, #261112 0%, #301315 100%);
            border-color: #6f2529;
        }
        .move-watch {
            border-left: 6px solid #2563eb;
            background: linear-gradient(135deg, #101b31 0%, #142241 100%);
            border-color: #254a94;
        }
        .move-head {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 0.75rem;
            margin-bottom: 0.35rem;
        }
        .move-symbol {
            font-size: 1.0rem;
            font-weight: 800;
            color: #111827;
        }
        .move-call {
            font-size: 0.74rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: #374151;
        }
        .move-line {
            font-size: 0.82rem;
            color: #dbe4f0;
            margin-top: 0.18rem;
            line-height: 1.45;
        }
        .move-why {
            font-size: 0.82rem;
            color: #c1ccd9;
            margin-top: 0.38rem;
            line-height: 1.5;
        }
        .move-label {
            color: #8ea3bb;
            font-weight: 800;
            text-transform: uppercase;
            font-size: 0.7rem;
            letter-spacing: 0.05em;
        }
        .detail-block {
            background: #111827;
            border: 1px solid #1f2937;
            border-radius: 14px;
            padding: 0.65rem 0.75rem;
            margin-top: 0.5rem;
        }
        .detail-label {
            font-size: 0.72rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: #9ca3af;
            margin-bottom: 0.2rem;
        }
        .detail-value {
            font-size: 0.9rem;
            line-height: 1.35;
            color: #f3f4f6;
            margin-bottom: 0.7rem;
        }
        .detail-value:last-child {
            margin-bottom: 0;
        }
        .rail-note {
            color: #cbd5e1;
            font-size: 0.9rem;
            margin-bottom: 0.7rem;
        }
        .compact-list {
            margin-top: 0.2rem;
        }
        .compact-row {
            display: flex;
            justify-content: space-between;
            gap: 0.75rem;
            padding: 0.35rem 0;
            border-bottom: 1px solid #1f2937;
        }
        .compact-row:last-child {
            border-bottom: none;
        }
        .compact-key {
            color: #94a3b8;
            font-size: 0.8rem;
            font-weight: 700;
        }
        .compact-val {
            color: #f8fafc;
            font-size: 0.8rem;
            text-align: right;
        }
        div[data-baseweb="select"] > div {
            min-height: 2.4rem;
            border-radius: 12px;
        }
        label[data-testid="stWidgetLabel"] p {
            font-size: 0.74rem !important;
            font-weight: 800 !important;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: #9ca3af !important;
        }
        .mini-label {
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: #94a3b8;
            margin-bottom: 0.15rem;
        }
        .mini-value {
            font-size: 0.9rem;
            font-weight: 700;
            color: #f8fafc;
        }
        .state-line {
            margin: 0.35rem 0 0.8rem 0;
            color: #374151;
        }
        .state-pill {
            display: inline-block;
            border-radius: 999px;
            padding: 0.24rem 0.64rem;
            font-size: 0.74rem;
            font-weight: 700;
            border: 1px solid #d1d5db;
            margin-left: 0.45rem;
            background: #ffffff;
        }
        .status-ok {
            color: #166534;
            border-color: #bbf7d0;
            background: #f0fdf4;
        }
        .status-warn {
            color: #92400e;
            border-color: #fde68a;
            background: #fffbeb;
        }
        .status-bad {
            color: #991b1b;
            border-color: #fecaca;
            background: #fef2f2;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid #e5e7eb;
            border-radius: 14px;
            overflow: hidden;
        }
        div[data-testid="stExpander"] {
            border: 1px solid #1f2937;
            border-radius: 14px;
            overflow: hidden;
            margin-bottom: 0.5rem;
        }
        div[data-testid="stExpander"] details {
            background: transparent;
        }
        div[data-testid="stExpander"] summary {
            padding-top: 0.15rem;
            padding-bottom: 0.15rem;
        }
        div[data-testid="stExpander"] summary p {
            color: #f8fafc !important;
            font-weight: 700 !important;
        }
        div[data-testid="stButton"] > button {
            min-height: 4.1rem;
            border-radius: 16px;
            border: 1px solid #2b3647;
            background: linear-gradient(180deg, #121926 0%, #0f1520 100%);
            color: #f8fafc;
            font-weight: 800;
            letter-spacing: -0.01em;
            line-height: 1.35;
            box-shadow: none;
        }
        div[data-testid="stButton"] > button:hover {
            border-color: #3b4a61;
            background: linear-gradient(180deg, #182131 0%, #121a28 100%);
            color: #ffffff;
        }
        .status-pass {
            display: inline-block;
            padding: 0.18rem 0.5rem;
            border-radius: 999px;
            background: #0f2e1a;
            color: #86efac;
            border: 1px solid #14532d;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .call-pill {
            display: inline-block;
            padding: 0.18rem 0.48rem;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            margin-right: 0.35rem;
        }
        .pill-buy {
            background: #10361e;
            color: #86efac;
            border: 1px solid #166534;
        }
        .pill-exit {
            background: #391416;
            color: #fca5a5;
            border: 1px solid #991b1b;
        }
        .pill-trim {
            background: #3a250d;
            color: #fcd34d;
            border: 1px solid #92400e;
        }
        .pill-watch {
            background: #132848;
            color: #93c5fd;
            border: 1px solid #1d4ed8;
        }
        .pill-score {
            display: inline-block;
            padding: 0.18rem 0.46rem;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 800;
            color: #e5e7eb;
            background: #1f2937;
            border: 1px solid #334155;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _fmt_ct_from_iso(value: Any) -> str:
    parsed = _parse_dt(value)
    if parsed is None:
        return "UNSET"
    return parsed.astimezone(CT_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")


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


def _load_latest_json(pattern: str) -> tuple[dict, Path | None]:
    if not RUNS_DIR.exists():
        return {}, None
    candidates = sorted(RUNS_DIR.glob(pattern))
    if not candidates:
        return {}, None
    latest = candidates[-1]
    payload = json.loads(latest.read_text(encoding="utf-8"))
    return payload, latest


def _load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_earnings_watchlist() -> list[dict]:
    if not EARNINGS_WATCHLIST_PATH.exists():
        return []
    payload = json.loads(EARNINGS_WATCHLIST_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        events = payload.get("events", [])
        return events if isinstance(events, list) else []
    if isinstance(payload, list):
        return payload
    return []


def _load_symbol_metadata() -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}
    if DB_PATH.exists():
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            for symbol, sector, industry in conn.execute(
                "SELECT symbol, sector, industry FROM instruments"
            ).fetchall():
                metadata[str(symbol).upper()] = {
                    "sector": str(sector or ""),
                    "industry": str(industry or ""),
                    "company": "",
                }
        finally:
            conn.close()
    for event in _load_earnings_watchlist():
        symbol = str(event.get("symbol", "")).upper()
        if not symbol:
            continue
        metadata.setdefault(symbol, {"sector": "", "industry": "", "company": ""})
        metadata[symbol]["company"] = str(event.get("company", "") or "")
    return metadata


def _compute_state(model_payload: dict, reviewed_payload: dict) -> tuple[str, str, list[str], list[str]]:
    blocking_reasons: list[str] = []
    warnings: list[str] = []

    model_actions = model_payload.get("actions")
    reviewed_actions = reviewed_payload.get("actions")
    news_guardrails = model_payload.get("news_guardrails", {}) if isinstance(model_payload, dict) else {}

    if not model_payload:
        blocking_reasons.append("model_output_missing")
    if not reviewed_payload:
        blocking_reasons.append("llm_review_missing")

    if not isinstance(model_actions, list) or not model_actions:
        blocking_reasons.append("model_actions_missing")
    if not isinstance(reviewed_actions, list) or not reviewed_actions:
        blocking_reasons.append("reviewed_actions_missing")

    model_ts = model_payload.get("run_timestamp_utc")
    source_ts = reviewed_payload.get("source_run_timestamp_utc")
    if model_ts and source_ts and model_ts != source_ts:
        blocking_reasons.append("review_not_for_latest_model_run")

    if isinstance(news_guardrails, dict):
        ng_status = str(news_guardrails.get("status", "")).strip().lower()
        ng_blocking = news_guardrails.get("blocking_reasons", [])
        ng_warning = news_guardrails.get("warning_reasons", [])
        if ng_status == "block":
            if isinstance(ng_blocking, list) and ng_blocking:
                for reason in ng_blocking:
                    blocking_reasons.append(f"news_guardrail:{reason}")
            else:
                blocking_reasons.append("news_guardrail:block")
        if isinstance(ng_warning, list):
            for reason in ng_warning:
                warnings.append(f"news_guardrail:{reason}")

    if isinstance(reviewed_actions, list) and reviewed_actions:
        delayed_count = sum(1 for a in reviewed_actions if (a.get("data_status") or "").upper() != "REALTIME")
        abstain_count = sum(1 for a in reviewed_actions if (a.get("llm_action") or "").strip() == "Abstain")
        if delayed_count > 0:
            warnings.append(f"delayed_feed_rows:{delayed_count}")
        if abstain_count > 0:
            warnings.append(f"llm_abstain_rows:{abstain_count}")

    if blocking_reasons:
        return "blocked", "Critical model/review data missing or mismatched", blocking_reasons, warnings
    return "action_safe", "Model + LLM review available", [], warnings


def _render_kpi(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="mini-card">
            <div class="mini-label">{label}</div>
            <div class="mini-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _call_pill_class(call: str) -> str:
    normalized = call.strip().lower()
    if normalized in {"buy", "strong buy"}:
        return "pill-buy"
    if normalized == "trim":
        return "pill-trim"
    if normalized == "exit":
        return "pill-exit"
    return "pill-watch"


def _status_class(status: str) -> str:
    normalized = status.strip().upper()
    if normalized == "PASS":
        return "status-ok"
    if normalized in {"WARN", "WARNING"}:
        return "status-warn"
    return "status-bad"


def _render_top_status(snapshot: dict, now_utc: datetime, reasons: list[str], warnings: list[str]) -> None:
    freshness = snapshot.get("freshness", {}) if isinstance(snapshot, dict) else {}
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_kpi("Market Data", _fmt_age_from_iso(metrics.get("latest_daily_bar_utc"), now_utc))
    with c2:
        _render_kpi("News Pull", _fmt_age_from_iso(metrics.get("latest_news_pull_utc"), now_utc))
    with c3:
        _render_kpi("Interpretation", _fmt_age_from_iso(metrics.get("latest_interp_utc"), now_utc))
    if reasons:
        st.error("Blocked by: " + ", ".join(reasons))


def _render_freshness(snapshot: dict) -> None:
    if not snapshot:
        st.warning("Freshness snapshot missing.")
        return

    overall_status = str(snapshot.get("overall_status", "UNKNOWN")).upper()
    freshness = snapshot.get("freshness", {})
    checks = freshness.get("checks", []) if isinstance(freshness, dict) else []
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}

    f1, f2, f3 = st.columns(3)
    with f1:
        _render_kpi("Latest Market Bar", _fmt_ct_from_iso(metrics.get("latest_daily_bar_utc")))
    with f2:
        _render_kpi("Latest News Pull", _fmt_ct_from_iso(metrics.get("latest_news_pull_utc")))
    with f3:
        _render_kpi("Latest Interpretation", _fmt_ct_from_iso(metrics.get("latest_interp_utc")))

    rows = []
    for check in checks:
        rows.append(
            {
                "Check": check.get("name", "UNKNOWN"),
                "Status": check.get("status", "UNKNOWN"),
                "Detail": check.get("detail", ""),
            }
        )
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)

    preflight_lags = snapshot.get("preflight_lags", [])
    if isinstance(preflight_lags, list) and preflight_lags:
        lag_rows = []
        for lag in preflight_lags:
            lag_rows.append(
                {
                    "Source": lag.get("source", "UNKNOWN"),
                    "Status": lag.get("status", "UNKNOWN"),
                    "Detail": lag.get("detail", ""),
                    "Backfill From (CT)": _fmt_ct_from_iso(lag.get("backfill_from")),
                    "Backfill To (CT)": _fmt_ct_from_iso(lag.get("backfill_to")),
                }
            )
        with st.expander("Preflight Catch-Up Details", expanded=False):
            st.dataframe(lag_rows, use_container_width=True, hide_index=True)


def _format_eps_band(low: Any, high: Any) -> str:
    if isinstance(low, (int, float)) and isinstance(high, (int, float)):
        return f"{low:.3f} to {high:.3f}"
    return "UNSET"


def _parse_report_dt_et(report_date_text: Any, call_time_text: Any) -> datetime | None:
    if not isinstance(report_date_text, str) or not report_date_text.strip():
        return None
    try:
        date_part = datetime.fromisoformat(report_date_text.strip()).date()
    except ValueError:
        return None
    hour = 16
    minute = 0
    if isinstance(call_time_text, str) and call_time_text.strip():
        hhmm = call_time_text.strip().split(":")
        if len(hhmm) == 2 and hhmm[0].isdigit() and hhmm[1].isdigit():
            hour = int(hhmm[0])
            minute = int(hhmm[1])
    dt_local = datetime(
        year=date_part.year,
        month=date_part.month,
        day=date_part.day,
        hour=hour,
        minute=minute,
        tzinfo=ET_ZONE,
    )
    return dt_local.astimezone(timezone.utc)


def _tranche_summary(regime: Any) -> str:
    if not isinstance(regime, dict):
        return "UNSET"
    plan_label = str(regime.get("plan_label", "UNSET"))
    side = str(regime.get("planned_side", "UNSET"))
    count = regime.get("tranche_count")
    return f"{plan_label} | side={side} | tranches={count}"


def _fmt_price(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"${value:,.2f}"
    return "UNSET"


def _confidence_score(row: dict) -> int:
    try:
        return int(row.get("llm_decision_confidence_score"))
    except (TypeError, ValueError):
        return -1


def _search_blob(row: dict, metadata: dict[str, dict[str, str]]) -> str:
    symbol = str(row.get("symbol", "")).upper()
    meta = metadata.get(symbol, {})
    return " ".join(
        [
            symbol,
            str(meta.get("company", "")),
            str(meta.get("sector", "")),
            str(meta.get("industry", "")),
            str(row.get("llm_action", "")),
            str(row.get("llm_rationale", "")),
            str(row.get("why_now", "")),
        ]
    ).lower()


def _find_row_by_symbol(rows: list[dict], symbol: str | None) -> dict | None:
    if not symbol:
        return None
    symbol = symbol.upper()
    for row in rows:
        if str(row.get("symbol", "")).upper() == symbol:
            return row
    return None


def _urgency_label(row: dict) -> str:
    call = str(row.get("llm_action", "")).strip().lower()
    horizon = str(row.get("horizon_type", "")).strip().lower()
    if call in {"exit", "trim"}:
        return "High"
    if horizon in {"hourly", "intraday"}:
        return "High"
    if call in {"buy", "strong buy"}:
        return "Medium"
    return "Low"


def _execution_summary(row: dict) -> str:
    call = str(row.get("llm_action", "UNSET"))
    ref_price = _fmt_price(row.get("reference_price"))
    invalidation = _fmt_price(row.get("invalidation_price"))
    sell_by = _fmt_ct_from_iso(row.get("sell_by_utc"))
    if call.lower() in {"buy", "strong buy"}:
        return f"Buy near {ref_price}. Exit if thesis breaks below {invalidation}. Reassess by {sell_by}."
    if call.lower() == "trim":
        return f"Trim strength near {ref_price}. Re-enter only if setup rebuilds. Reassess by {sell_by}."
    if call.lower() == "exit":
        return f"Exit near market/reference around {ref_price}. Re-enter only if score rebuilds and thesis improves."
    if call.lower() == "watch":
        return f"Watch near {ref_price}. No action until setup improves."
    return f"Reference {ref_price}. Invalidation {invalidation}. Reassess by {sell_by}."


def _timing_summary(row: dict) -> str:
    call = str(row.get("llm_action", "")).strip().lower()
    buy_til = _fmt_ct_from_iso(row.get("buy_til_utc"))
    hold_til = _fmt_ct_from_iso(row.get("hold_til_utc"))
    sell_by = _fmt_ct_from_iso(row.get("sell_by_utc"))
    if call in {"buy", "strong buy"}:
        return f"Enter by {buy_til}. Hold through {hold_til} unless invalidated."
    if call == "trim":
        return f"Trim now or on strength. Reassess by {sell_by}."
    if call == "exit":
        return f"Exit now. Reassess for re-entry by {sell_by}."
    if call == "watch":
        return f"Watch through {hold_til}. No entry unless setup improves."
    return f"Reassess by {sell_by}."


def _move_card_class(call: str) -> str:
    normalized = call.strip().lower()
    if normalized in {"buy", "strong buy"}:
        return "move-buy"
    if normalized == "trim":
        return "move-trim"
    if normalized == "exit":
        return "move-exit"
    return "move-watch"


def _render_move_detail(row: dict) -> None:
    symbol = str(row.get("symbol", "UNKNOWN"))
    call = str(row.get("llm_action", "UNSET"))
    confidence = row.get("llm_decision_confidence_score", "UNSET")
    with st.expander(f"{symbol} | {call} | {confidence}/100", expanded=False):
        st.markdown(f'<div class="move-card {_move_card_class(call)}">', unsafe_allow_html=True)
        st.markdown(
            f'<div class="move-line"><span class="move-label">Reference</span><br>{_fmt_price(row.get("reference_price"))}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="move-line"><span class="move-label">Plan</span><br>{_timing_summary(row)}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="move-why"><span class="move-label">Why</span><br>{row.get("llm_rationale") or row.get("why_now") or "UNSET"}</div>',
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)


def _detail_block(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="detail-block">
            <div class="detail-label">{label}</div>
            <div class="detail-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_overview_panel(snapshot: dict, reviewed_actions: list[dict]) -> None:
    st.markdown('<div class="rail-note">System trust first. Check recency, then act.</div>', unsafe_allow_html=True)
    freshness = snapshot.get("freshness", {}) if isinstance(snapshot, dict) else {}
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}
    overall = str(snapshot.get("overall_status", "UNKNOWN")).upper()
    checks = freshness.get("checks", []) if isinstance(freshness, dict) else []
    check_rows = "".join(
        f'<div class="compact-row"><div class="compact-key">{c.get("name","UNKNOWN")}</div><div class="compact-val">{c.get("status","UNKNOWN")}</div></div>'
        for c in checks
    )
    st.markdown(
        f"""
        <div class="detail-block">
            <div class="detail-label">Freshness</div>
            <div class="detail-value">Overall: <span class="status-pass">{overall}</span></div>
            <div class="compact-list">
                <div class="compact-row"><div class="compact-key">Market Data</div><div class="compact-val">{_fmt_age_from_iso(metrics.get("latest_daily_bar_utc"), datetime.now(timezone.utc))}</div></div>
                <div class="compact-row"><div class="compact-key">News Pull</div><div class="compact-val">{_fmt_age_from_iso(metrics.get("latest_news_pull_utc"), datetime.now(timezone.utc))}</div></div>
                <div class="compact-row"><div class="compact-key">Interpretation</div><div class="compact-val">{_fmt_age_from_iso(metrics.get("latest_interp_utc"), datetime.now(timezone.utc))}</div></div>
                {check_rows}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    top = reviewed_actions[0] if reviewed_actions else None
    if top:
        top_call = str(top.get("llm_action", "UNSET"))
        _detail_block(
            "Top Call",
            (
                f"<strong>{top.get('symbol', 'UNSET')}</strong><br>"
                f'<span class="call-pill {_call_pill_class(top_call)}">{top_call}</span>'
                f'<span class="pill-score">{top.get("llm_decision_confidence_score", "UNSET")}/100</span><br><br>'
                f"<span class='move-label'>Plan</span><br>{_timing_summary(top)}<br><br>"
                f"<span class='move-label'>Reference</span><br>{_fmt_price(top.get('reference_price'))}<br><br>"
                f"<span class='move-label'>Why</span><br>{top.get('llm_rationale') or top.get('why_now') or 'UNSET'}"
            ),
        )


def _render_ops_panel(model_payload: dict, model_path: Path | None, reviewed_path: Path | None, reviewed_actions: list[dict]) -> None:
    if model_path:
        st.caption(f"Model file: {model_path}")
    if reviewed_path:
        st.caption(f"Review file: {reviewed_path}")
    if reviewed_actions:
        delayed_count = sum(1 for a in reviewed_actions if (a.get("data_status") or "").upper() != "REALTIME")
        blocked_count = sum(1 for a in reviewed_actions if a.get("investability_blocked"))
        abstain_count = sum(1 for a in reviewed_actions if (a.get("llm_action") or "").strip() == "Abstain")
        news_guardrails = model_payload.get("news_guardrails", {}) if isinstance(model_payload, dict) else {}
        requests_used = news_guardrails.get("requests_used_today", "UNSET")
        daily_budget = news_guardrails.get("daily_budget", "UNSET")
        interpreted_rows_24h = news_guardrails.get("interpreted_rows_24h", "UNSET")
        latest_interpreted_age_seconds = news_guardrails.get("latest_interpreted_age_seconds", "UNSET")
        _detail_block(
            "Diagnostics",
            "<br>".join(
                [
                    f"Delayed feed rows: {delayed_count}",
                    f"Investability blocks: {blocked_count}",
                    f"LLM abstains: {abstain_count}",
                    f"News requests used: {requests_used}/{daily_budget}",
                    f"Interpreted news 24h: {interpreted_rows_24h}",
                    f"Interpretation freshness: {latest_interpreted_age_seconds}s",
                ]
            ),
        )
    _detail_block(
        "Execution Mode",
        "LLM mode: interpretation_only_no_calculations<br>Programmatic execution: blocked (manual Robinhood only)",
    )


def _render_execution_board(reviewed_actions: list[dict]) -> None:
    st.subheader("Execution Board")
    if not reviewed_actions:
        st.warning("No reviewed actions available.")
        return

    ordered = sorted(
        reviewed_actions,
        key=lambda row: (
            {"Exit": 0, "Trim": 1, "Buy": 2, "Strong Buy": 3, "Watch": 4}.get(str(row.get("llm_action", "")), 9),
            -(float(row.get("final_score", 0) or 0)),
        ),
    )

    board_rows = []
    for row in ordered[:10]:
        board_rows.append(
            {
                "Symbol": row.get("symbol"),
                "Call": row.get("llm_action"),
                "Urgency": _urgency_label(row),
                "Reference": _fmt_price(row.get("reference_price")),
                "When": _timing_summary(row),
                "Execution": _execution_summary(row),
            }
        )
    st.dataframe(board_rows, use_container_width=True, hide_index=True)

    st.subheader("Symbol Detail")
    for row in ordered[:8]:
        symbol = str(row.get("symbol", "UNKNOWN"))
        call = str(row.get("llm_action", "UNSET"))
        urgency = _urgency_label(row)
        with st.expander(f"{symbol} | {call} | {urgency}", expanded=False):
            a1, a2, a3 = st.columns(3)
            with a1:
                _render_kpi("Reference", _fmt_price(row.get("reference_price")))
            with a2:
                _render_kpi("Invalidation", _fmt_price(row.get("invalidation_price")))
            with a3:
                _render_kpi("Timing", _timing_summary(row))

            b1, b2, b3 = st.columns(3)
            with b1:
                _render_kpi("Buy Til", _fmt_ct_from_iso(row.get("buy_til_utc")))
            with b2:
                _render_kpi("Hold Til", _fmt_ct_from_iso(row.get("hold_til_utc")))
            with b3:
                _render_kpi("Sell By", _fmt_ct_from_iso(row.get("sell_by_utc")))

            st.markdown("**Action**")
            st.write(_execution_summary(row))
            st.markdown("**Sell / Exit Conditions**")
            st.write(row.get("sell_plan") or "UNSET")
            st.markdown("**Why**")
            st.write(row.get("llm_rationale") or row.get("why_now") or "UNSET")
            st.markdown("**Model Context**")
            st.write(row.get("why_now") or "UNSET")
            st.caption(
                "Score="
                + str(row.get("final_score", "UNSET"))
                + " | LLM Confidence Score="
                + str(row.get("llm_decision_confidence_score", "UNSET"))
                + " | Confidence="
                + str(row.get("llm_confidence_label", "UNSET"))
            )


def _render_immediate_moves(reviewed_actions: list[dict]) -> None:
    st.subheader("Immediate Moves")
    if not reviewed_actions:
        st.warning("No reviewed actions available.")
        return

    symbol_metadata = _load_symbol_metadata()
    query = st.text_input(
        "Search Calls",
        value=st.session_state.get("call_search", ""),
        placeholder="Search symbol or company",
        key="call_search",
    )
    filtered_actions = reviewed_actions
    if query.strip():
        q = query.strip().lower()
        filtered_actions = [row for row in reviewed_actions if q in _search_blob(row, symbol_metadata)]

    buys = []
    reductions = []
    for row in filtered_actions:
        call = str(row.get("llm_action", "")).strip()
        if call in {"Strong Buy", "Buy"}:
            buys.append((_confidence_score(row), row))
        elif call in {"Exit", "Trim"}:
            reductions.append((_confidence_score(row), row))

    left, right = st.columns(2, gap="medium")
    with left:
        header_left, count_left = st.columns([1, 1])
        with header_left:
            st.markdown('<div class="section-chip chip-buy">Buy / Add</div>', unsafe_allow_html=True)
        with count_left:
            st.markdown(f'<div class="lane-count">{min(len(buys), 5)} of {len(buys)} current</div>', unsafe_allow_html=True)
        if buys:
            for _, row in sorted(buys, key=lambda x: x[0], reverse=True)[:5]:
                _render_move_detail(row)
        else:
            st.caption("No matching buys.")
    with right:
        header_right, count_right = st.columns([1, 1])
        with header_right:
            st.markdown('<div class="section-chip chip-trim">Trim / Exit</div>', unsafe_allow_html=True)
        with count_right:
            st.markdown(f'<div class="lane-count">{min(len(reductions), 5)} of {len(reductions)} current</div>', unsafe_allow_html=True)
        if reductions:
            for _, row in sorted(reductions, key=lambda x: x[0], reverse=True)[:5]:
                _render_move_detail(row)
        else:
            st.caption("No matching trims or exits.")


def _render_earnings_section(now_utc: datetime, reviewed_actions: list[dict]) -> None:
    st.subheader("Important Earnings Coming Up")
    entries = _load_earnings_watchlist()
    if not entries:
        st.warning("Earnings watchlist missing. Add data/manual/earnings_watchlist.json.")
        return

    by_symbol: dict[str, dict] = {}
    for row in reviewed_actions:
        symbol = str(row.get("symbol", "")).strip().upper()
        if symbol:
            by_symbol[symbol] = row

    events_by_symbol = {}
    for event in entries:
        symbol = str(event.get("symbol", "")).strip().upper()
        if symbol:
            events_by_symbol[symbol] = event

    rows = []
    for symbol in SEMIS_FOCUS_SYMBOLS:
        event = events_by_symbol.get(symbol, {"symbol": symbol})
        action_row = by_symbol.get(symbol, {})
        report_dt = _parse_report_dt_et(event.get("report_date_et"), event.get("call_time_et"))
        if report_dt is not None:
            days_to_report = int((report_dt.date() - now_utc.date()).days)
            report_date_ct = report_dt.astimezone(CT_ZONE).strftime("%Y-%m-%d")
            call_time_ct = report_dt.astimezone(CT_ZONE).strftime("%H:%M %Z")
        else:
            days_to_report = "UNSET"
            report_date_ct = "UNSET"
            call_time_ct = "UNSET"

        rows.append(
            {
                "Symbol": symbol,
                "LLM Action": action_row.get("llm_action", "NOT_REVIEWED"),
                "Model Action": action_row.get("final_action", "NOT_SCORED"),
                "Timing": _timing_summary(action_row),
                "Buy Til (CT)": _fmt_ct_from_iso(action_row.get("buy_til_utc")),
                "Hold Til (CT)": _fmt_ct_from_iso(action_row.get("hold_til_utc")),
                "Sell By (CT)": _fmt_ct_from_iso(action_row.get("sell_by_utc")),
                "Tranche Regime": _tranche_summary(action_row.get("execution_regime")),
                "Sell / Exit Conditions": action_row.get("sell_plan", "UNSET"),
                "Why": action_row.get("llm_rationale", action_row.get("why_now", "UNSET")),
                "Report Date (CT)": report_date_ct,
                "Days To Report": str(days_to_report),
                "Call Time (CT)": call_time_ct,
                "EPS Consensus Band": _format_eps_band(
                    event.get("eps_consensus_low"), event.get("eps_consensus_high")
                ),
                "EPS Guidance Band": _format_eps_band(
                    event.get("eps_guidance_low"), event.get("eps_guidance_high")
                ),
                "Earnings Note": event.get("notes", "UNSET"),
            }
        )

    st.dataframe(rows, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(
        page_title="Tradly Pulse",
        page_icon="📈",
        layout="centered",
        initial_sidebar_state="collapsed",
    )
    _inject_styles()
    time_ctx = get_time_context()

    model_payload, model_path = _load_latest_json("*/model_v0_actions.json")
    reviewed_payload, reviewed_path = _load_latest_json("*/model_v0_reviewed.json")
    freshness_snapshot = _load_json_file(FRESHNESS_SNAPSHOT_PATH)
    reviewed_actions = reviewed_payload.get("actions", []) if isinstance(reviewed_payload, dict) else []
    if not isinstance(reviewed_actions, list):
        reviewed_actions = []

    state, state_msg, reasons, warnings = _compute_state(model_payload, reviewed_payload)

    left_col, right_col = st.columns([1.45, 1], gap="large")

    with left_col:
        _render_top_status(freshness_snapshot, time_ctx.now_utc, reasons, warnings)
        if state != "blocked":
            _render_immediate_moves(reviewed_actions)
        else:
            st.subheader("Immediate Moves")
            st.warning("Action sheet suppressed: system is BLOCKED.")

    with right_col:
        section = st.selectbox(
            "Navigate",
            ["Overview", "Execute", "Earnings", "Ops"],
            index=0,
        )

        if section == "Overview":
            _render_overview_panel(freshness_snapshot, reviewed_actions)

        elif section == "Execute":
            if state != "blocked":
                _render_execution_board(reviewed_actions)
                with st.expander("Full Action Table", expanded=False):
                    full_rows = []
                    for row in reviewed_actions[:15]:
                        full_rows.append(
                            {
                                "Symbol": row.get("symbol"),
                                "LLM Final Call": row.get("llm_action"),
                                "LLM Confidence Score": row.get("llm_decision_confidence_score"),
                                "LLM Confidence": row.get("llm_confidence_label"),
                                "Model Action": row.get("final_action"),
                                "Score": row.get("final_score"),
                                "Horizon": row.get("horizon_type"),
                                "Reference Price": row.get("reference_price"),
                                "Invalidation Price": row.get("invalidation_price"),
                                "Planned Side": row.get("target_order_side"),
                                "Tranche Regime": _tranche_summary(row.get("execution_regime")),
                                "Buy Til (CT)": _fmt_ct_from_iso(row.get("buy_til_utc")),
                                "Hold Til (CT)": _fmt_ct_from_iso(row.get("hold_til_utc")),
                                "Sell By (CT)": _fmt_ct_from_iso(row.get("sell_by_utc")),
                                "Sell / Exit Conditions": row.get("sell_plan"),
                                "LLM Why": row.get("llm_rationale"),
                                "Model Why": row.get("why_now"),
                                "Investability": row.get("halal_flag"),
                            }
                        )
                    st.dataframe(full_rows, use_container_width=True, hide_index=True)
            else:
                st.warning("Action sheet suppressed: system is BLOCKED.")

        elif section == "Earnings":
            _render_earnings_section(time_ctx.now_utc, reviewed_actions)

        else:
            _render_ops_panel(model_payload, model_path, reviewed_path, reviewed_actions)


if __name__ == "__main__":
    main()
