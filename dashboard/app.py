from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import streamlit as st

from tradly.services.time_context import get_time_context


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = REPO_ROOT / "data" / "runs"
EARNINGS_WATCHLIST_PATH = REPO_ROOT / "data" / "manual" / "earnings_watchlist.json"
FRESHNESS_SNAPSHOT_PATH = REPO_ROOT / "data" / "journal" / "freshness_snapshot.json"
SEMIS_FOCUS_SYMBOLS = ("MU", "SNDK", "NVDA", "NVTS")
ET_ZONE = ZoneInfo("America/New_York")
CT_ZONE = ZoneInfo("America/Chicago")


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
            gap: 0.75rem;
            margin: 0.8rem 0 0.2rem 0;
        }
        .mini-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 14px;
            padding: 0.8rem 0.9rem;
        }
        .mini-label {
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: #6b7280;
            margin-bottom: 0.25rem;
        }
        .mini-value {
            font-size: 1rem;
            font-weight: 700;
            color: #111827;
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


def _status_class(status: str) -> str:
    normalized = status.strip().upper()
    if normalized == "PASS":
        return "status-ok"
    if normalized in {"WARN", "WARNING"}:
        return "status-warn"
    return "status-bad"


def _render_hero(
    state: str,
    state_msg: str,
    reasons: list[str],
    freshness_snapshot: dict,
    model_payload: dict,
) -> None:
    freshness = freshness_snapshot.get("freshness", {}) if isinstance(freshness_snapshot, dict) else {}
    overall_status = str(freshness_snapshot.get("overall_status", "UNKNOWN")).upper()
    scored_count = model_payload.get("scored_count", "MISSING")
    latest_run = _fmt_ct_from_iso(model_payload.get("run_timestamp_utc"))
    st.markdown(
        f"""
        <div class="hero">
            <div class="hero-title">Tradly Pulse</div>
            <div class="hero-value">System {state.upper()} <span class="state-pill {_status_class(overall_status)}">{overall_status}</span></div>
            <div class="hero-note">{state_msg}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if reasons:
        st.error("Blocked by: " + ", ".join(reasons))
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_kpi("Last Model Run", latest_run)
    with c2:
        _render_kpi("Scored Symbols", str(scored_count))
    with c3:
        _render_kpi("Freshness", overall_status)


def _render_freshness(snapshot: dict) -> None:
    st.subheader("Readiness")
    if not snapshot:
        st.warning("Freshness snapshot missing.")
        return

    overall_status = str(snapshot.get("overall_status", "UNKNOWN")).upper()
    freshness = snapshot.get("freshness", {})
    checks = freshness.get("checks", []) if isinstance(freshness, dict) else []
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}

    f1, f2, f3 = st.columns(3)
    with f1:
        _render_kpi("Latest Market Bar (CT)", _fmt_ct_from_iso(metrics.get("latest_daily_bar_utc")))
    with f2:
        _render_kpi("Latest News Pull (CT)", _fmt_ct_from_iso(metrics.get("latest_news_pull_utc")))
    with f3:
        _render_kpi("Latest Interpretation (CT)", _fmt_ct_from_iso(metrics.get("latest_interp_utc")))

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
                "Horizon": action_row.get("horizon_type", "NOT_SCORED"),
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

    _render_hero(state, state_msg, reasons, freshness_snapshot, model_payload)
    if warnings:
        st.warning("Warnings: " + ", ".join(warnings))
    st.caption(
        "Manual Robinhood execution only. Current time (CT): "
        + time_ctx.now_utc.astimezone(CT_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
    )

    _render_freshness(freshness_snapshot)

    st.subheader("Actions")
    if state == "blocked":
        st.warning("Action sheet suppressed: system is BLOCKED.")
    elif not reviewed_actions:
        st.warning("No reviewed actions available.")
    else:
        rows = []
        for row in reviewed_actions[:8]:
            rows.append(
                {
                    "Symbol": row.get("symbol"),
                    "Call": row.get("llm_action"),
                    "Horizon": row.get("horizon_type"),
                    "Sell By (CT)": _fmt_ct_from_iso(row.get("sell_by_utc")),
                    "Why": row.get("llm_rationale") or row.get("why_now"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)
        with st.expander("Full Action Table", expanded=False):
            full_rows = []
            for row in reviewed_actions[:15]:
                full_rows.append(
                    {
                        "Symbol": row.get("symbol"),
                        "LLM Final Call": row.get("llm_action"),
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
                        "Data Status": row.get("data_status"),
                    }
                )
            st.dataframe(full_rows, use_container_width=True, hide_index=True)

    _render_earnings_section(time_ctx.now_utc, reviewed_actions)

    with st.expander("Operational Details", expanded=False):
        if model_path:
            st.caption(f"Model file: {model_path}")
        if reviewed_path:
            st.caption(f"Review file: {reviewed_path}")
        if state == "blocked":
            st.caption("Guardrail metrics suppressed while blocked.")
        elif reviewed_actions:
            delayed_count = sum(1 for a in reviewed_actions if (a.get("data_status") or "").upper() != "REALTIME")
            blocked_count = sum(1 for a in reviewed_actions if a.get("investability_blocked"))
            abstain_count = sum(1 for a in reviewed_actions if (a.get("llm_action") or "").strip() == "Abstain")
            news_guardrails = model_payload.get("news_guardrails", {}) if isinstance(model_payload, dict) else {}
            requests_used = news_guardrails.get("requests_used_today", "UNSET")
            daily_budget = news_guardrails.get("daily_budget", "UNSET")
            interpreted_rows_24h = news_guardrails.get("interpreted_rows_24h", "UNSET")
            latest_interpreted_age_seconds = news_guardrails.get("latest_interpreted_age_seconds", "UNSET")
            g1, g2, g3 = st.columns(3)
            with g1:
                _render_kpi("Delayed Feed Rows", str(delayed_count))
            with g2:
                _render_kpi("Investability Blocks", str(blocked_count))
            with g3:
                _render_kpi("LLM Abstains", str(abstain_count))
            g4, g5, g6 = st.columns(3)
            with g4:
                _render_kpi("News Requests Used", f"{requests_used}/{daily_budget}")
            with g5:
                _render_kpi("Interpreted News 24h", str(interpreted_rows_24h))
            with g6:
                _render_kpi("Interp Freshness (s)", str(latest_interpreted_age_seconds))
        st.code(
            "LLM mode: interpretation_only_no_calculations\n"
            "Programmatic execution: BLOCKED (manual-only in Robinhood)",
            language="text",
        )


if __name__ == "__main__":
    main()
