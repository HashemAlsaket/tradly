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
FRESHNESS_SNAPSHOT_PATH = REPO_ROOT / "data" / "journal" / "freshness_snapshot.json"
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
    return parsed.astimezone(CT_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")


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
        .tradly-utility-box {
          border: 1px solid rgba(255, 255, 255, 0.09);
          border-radius: 12px;
          padding: 0.55rem 0.75rem 0.35rem 0.75rem;
          background: rgba(255, 255, 255, 0.02);
          max-width: 18rem;
          margin-bottom: 0.85rem;
        }
        .tradly-card {
          border: 1px solid var(--tradly-card-border);
          border-left-width: 4px;
          border-radius: 14px;
          padding: 0.9rem 1rem 0.8rem 1rem;
          margin: 0 0 0.7rem 0;
          background: rgba(255, 255, 255, 0.015);
        }
        .tradly-card.buy { border-left-color: var(--tradly-buy); }
        .tradly-card.sell { border-left-color: var(--tradly-sell); }
        .tradly-card.watch { border-left-color: var(--tradly-watch); }
        .tradly-card-top {
          display: flex;
          justify-content: space-between;
          align-items: baseline;
          gap: 1rem;
        }
        .tradly-symbol {
          font-size: 1.65rem;
          font-weight: 800;
          line-height: 1.05;
        }
        .tradly-confidence {
          font-size: 1.45rem;
          font-weight: 800;
          letter-spacing: -0.02em;
        }
        .tradly-confidence-label {
          font-size: 0.72rem;
          opacity: 0.7;
          text-transform: uppercase;
          letter-spacing: 0.06em;
        }
        .tradly-meta {
          margin-top: 0.25rem;
          font-size: 0.88rem;
          opacity: 0.82;
        }
        .tradly-reason {
          margin-top: 0.55rem;
          font-size: 0.86rem;
          opacity: 0.92;
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

    market_quality = _quality_status(market_payload)
    sector_quality = _quality_status(sector_payload)
    symbol_quality = _quality_status(symbol_payload)
    symbol_news_quality = _quality_status(symbol_news_payload)
    sector_news_quality = _quality_status(sector_news_payload)
    range_quality = _quality_status(range_payload)
    ensemble_quality = _quality_status(ensemble_payload)
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

    snapshot_written_at = _parse_dt(freshness_snapshot.get("written_at_utc"))
    latest_model_run = _latest_run_timestamp(
        market_payload,
        sector_payload,
        symbol_payload,
        symbol_news_payload,
        sector_news_payload,
        range_payload,
        ensemble_payload,
    )
    if snapshot_written_at is not None and latest_model_run is not None and snapshot_written_at < latest_model_run:
        reasons.append("freshness_snapshot_outdated_for_latest_model_runs")

    if reasons:
        return ("blocked", reasons, warnings)

    research_signals = []
    for label, payload in (
        ("symbol_news", symbol_news_payload),
        ("sector_news", sector_news_payload),
        ("ensemble", ensemble_payload),
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


def _render_top_status(snapshot: dict, now_utc: datetime, state: str, reasons: list[str], warnings: list[str]) -> None:
    freshness = snapshot.get("freshness", {}) if isinstance(snapshot, dict) else {}
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_kpi("Market", _fmt_age_from_iso(metrics.get("latest_daily_bar_utc"), now_utc))
    with c2:
        _render_kpi("News", _fmt_age_from_iso(metrics.get("latest_news_pull_utc"), now_utc))
    with c3:
        _render_kpi("LLM", _fmt_age_from_iso(metrics.get("latest_interp_utc"), now_utc))
    if reasons:
        st.error("Blocked by: " + ", ".join(reasons))
    elif state == "ready":
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
        if global_state == "blocked":
            dominant_state = "blocked"
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


def _action_for_horizon(horizon_row: dict[str, Any]) -> str:
    state = str(horizon_row.get("state", "missing")).strip().lower()
    direction = str(horizon_row.get("signal_direction", "neutral")).strip().lower()
    confidence = int(horizon_row.get("confidence_score", 0) or 0)
    if state == "actionable":
        if direction == "bullish":
            return "Buy"
        if direction == "bearish":
            return "Sell/Trim"
        return "Hold"
    if state == "research_only":
        if direction == "bullish" and confidence >= 60:
            return "Watch Buy"
        if direction == "bearish" and confidence >= 55:
            return "Watch Trim"
        return "Hold/Watch"
    if state == "blocked":
        return "Blocked"
    if state == "not_supported":
        return "N/A"
    return "Unknown"


def _action_priority(action: str) -> int:
    return {
        "Buy": 5,
        "Sell/Trim": 5,
        "Watch Buy": 4,
        "Watch Trim": 4,
        "Hold": 3,
        "Hold/Watch": 2,
        "Blocked": 1,
        "N/A": 0,
        "Unknown": 0,
    }.get(action, 0)


def _best_decision(summary: dict[str, Any]) -> tuple[str, str]:
    horizon_rows = summary if isinstance(summary, dict) else {}
    ranked: list[tuple[int, int, str, str]] = []
    for horizon in ("1to3d", "1to2w", "2to6w"):
        horizon_row = horizon_rows.get(horizon, {}) if isinstance(horizon_rows, dict) else {}
        action = _action_for_horizon(horizon_row if isinstance(horizon_row, dict) else {})
        confidence = int((horizon_row if isinstance(horizon_row, dict) else {}).get("confidence_score", 0) or 0)
        ranked.append((_action_priority(action), confidence, horizon, action))
    ranked.sort(reverse=True)
    _, _, horizon, action = ranked[0]
    return action, horizon


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
    }
    return replacements.get(text, text)


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


def _decision_rows(ensemble_payload: dict) -> list[dict[str, Any]]:
    rows = ensemble_payload.get("rows", [])
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        summary = row.get("horizon_summary", {}) if isinstance(row, dict) else {}
        if not isinstance(summary, dict):
            continue
        action, horizon = _best_decision(summary)
        horizon_row = summary.get(horizon, {}) if isinstance(summary.get(horizon, {}), dict) else {}
        out.append(
            {
                "Symbol": str(row.get("scope_id", "UNSET")),
                "Action": action,
                "Horizon": horizon,
                "Confidence": int(horizon_row.get("confidence_score", row.get("confidence_score", 0)) or 0),
                "Reason": _humanize_reason(str(((horizon_row.get("why_code", []) or [""])[:1] or [""])[0])),
            }
        )
    return sorted(
        out,
        key=lambda row: (
            _action_priority(str(row["Action"])),
            int(row["Confidence"]),
            str(row["Horizon"]) == "2to6w",
            str(row["Horizon"]) == "1to2w",
        ),
        reverse=True,
    )


def _render_action_list(title: str, rows: list[dict[str, Any]]) -> None:
    section_class = {
        "Buy": "buy",
        "Sell / Trim": "sell",
        "Watch": "watch",
    }.get(title, "watch")
    st.markdown(
        f'<div class="tradly-section-title {section_class}">{title}</div>',
        unsafe_allow_html=True,
    )
    section_blurbs = {
        "Buy": "Actionable bullish setups.",
        "Sell / Trim": "Actionable bearish or reduce-risk setups.",
        "Watch": "Not actionable yet, but worth monitoring.",
    }
    blurb = section_blurbs.get(title, "")
    if blurb:
        st.markdown(f'<div class="tradly-section-subtle">{blurb}</div>', unsafe_allow_html=True)
    if not rows:
        st.caption("None")
        return
    st.markdown(f'<div class="tradly-section-subtle">{len(rows)} shown</div>', unsafe_allow_html=True)
    for row in rows:
        symbol = str(row["Symbol"])
        horizon = str(row["Horizon"])
        confidence = int(row["Confidence"])
        reason = str(row["Reason"]).strip()
        horizon_label = _format_horizon_label(horizon)
        lane_name = _horizon_lane_name(horizon)
        st.markdown(
            f"""
            <div class="tradly-card {section_class}">
              <div class="tradly-card-top">
                <div class="tradly-symbol">{symbol}</div>
                <div>
                  <div class="tradly-confidence-label">Confidence</div>
                  <div class="tradly-confidence">{confidence}</div>
                </div>
              </div>
              <div class="tradly-meta">{lane_name.title()} • {horizon_label}</div>
              <div class="tradly-reason">{reason}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_action_board(ensemble_payload: dict) -> None:
    ranked_rows = _decision_rows(ensemble_payload)
    if not ranked_rows:
        st.caption("No decisions available.")
        return
    buy_rows = sorted(
        [row for row in ranked_rows if str(row["Action"]) == "Buy"],
        key=lambda row: int(row["Confidence"]),
        reverse=True,
    )[:6]
    sell_rows = sorted(
        [row for row in ranked_rows if str(row["Action"]) == "Sell/Trim"],
        key=lambda row: int(row["Confidence"]),
        reverse=True,
    )[:6]
    watch_rows = sorted(
        [row for row in ranked_rows if str(row["Action"]).startswith("Watch")],
        key=lambda row: int(row["Confidence"]),
        reverse=True,
    )[:8]
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_action_list("Buy", buy_rows)
    with c2:
        _render_action_list("Sell / Trim", sell_rows)
    with c3:
        _render_action_list("Watch", watch_rows)


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


def _render_symbol_stack(symbol_payload: dict, symbol_news_payload: dict, range_payload: dict, ensemble_payload: dict) -> None:
    symbol_rows = symbol_payload.get("rows", []) if isinstance(symbol_payload.get("rows"), list) else []
    news_rows = {
        str(row.get("scope_id", "")): row
        for row in (symbol_news_payload.get("rows", []) if isinstance(symbol_news_payload.get("rows"), list) else [])
        if isinstance(row, dict)
    }
    range_rows = {
        str(row.get("scope_id", "")): row
        for row in (range_payload.get("rows", []) if isinstance(range_payload.get("rows"), list) else [])
        if isinstance(row, dict)
    }
    ensemble_rows = {
        str(row.get("scope_id", "")): row
        for row in (ensemble_payload.get("rows", []) if isinstance(ensemble_payload.get("rows"), list) else [])
        if isinstance(row, dict)
    }
    top_symbols = sorted(
        symbol_rows,
        key=lambda row: (
            _action_priority(
                _best_decision(
                    (ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("horizon_summary", {})
                    if isinstance((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("horizon_summary", {}), dict)
                    else {}
                )[0]
            ),
            str(((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("horizon_summary", {}) or {}).get("2to6w", {}).get("state", "")) == "actionable",
            str(((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("horizon_summary", {}) or {}).get("1to2w", {}).get("state", "")) == "actionable",
            str(((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("horizon_summary", {}) or {}).get("1to3d", {}).get("state", "")) == "actionable",
            str(((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("horizon_summary", {}) or {}).get("2to6w", {}).get("state", "")) == "research_only",
            str(((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("horizon_summary", {}) or {}).get("1to2w", {}).get("state", "")) == "research_only",
            abs(float((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("score_normalized", 0.0) or 0.0)),
            int((ensemble_rows.get(str(row.get("scope_id", "")), {}) or {}).get("confidence_score", 0) or 0),
            abs(float(row.get("score_normalized", 0.0) or 0.0)),
        ),
        reverse=True,
    )[:12]
    table_rows = []
    for row in top_symbols:
        symbol = str(row.get("scope_id", ""))
        ensemble_row = ensemble_rows.get(symbol, {})
        range_row = range_rows.get(symbol, {})
        lane_id = str(ensemble_row.get("lane_primary", ""))
        lane_diag = (
            range_row.get("lane_diagnostics", {}).get(lane_id, {})
            if isinstance(range_row.get("lane_diagnostics"), dict)
            else {}
        )
        horizon_summary = ensemble_row.get("horizon_summary", {}) if isinstance(ensemble_row.get("horizon_summary"), dict) else {}
        best_action, best_horizon = _best_decision(horizon_summary)
        table_rows.append(
            {
                "Symbol": symbol,
                "Action": best_action,
                "Horizon": best_horizon,
                "Confidence": ensemble_row.get("confidence_score", "UNSET") if ensemble_row else "UNSET",
                "Why": _humanize_reason(str(((horizon_summary.get(best_horizon, {}) or {}).get("why_code", [])[:1] or [""])[0])),
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
    market_payload, _ = _load_latest_run_artifact("market_regime_v1.json")
    sector_payload, _ = _load_latest_run_artifact("sector_movement_v1.json")
    symbol_payload, _ = _load_latest_run_artifact("symbol_movement_v1.json")
    symbol_news_payload, _ = _load_latest_run_artifact("symbol_news_v1.json")
    sector_news_payload, _ = _load_latest_run_artifact("sector_news_v1.json")
    range_payload, _ = _load_latest_run_artifact("range_expectation_v1.json")
    ensemble_payload, _ = _load_latest_run_artifact("ensemble_v1.json")

    state, reasons, warnings = _compute_system_state(
        freshness_snapshot=freshness_snapshot,
        market_payload=market_payload,
        sector_payload=sector_payload,
        symbol_payload=symbol_payload,
        symbol_news_payload=symbol_news_payload,
        sector_news_payload=sector_news_payload,
        range_payload=range_payload,
        ensemble_payload=ensemble_payload,
    )

    st.title("tradly")

    utility_col, _ = st.columns([1, 5])
    with utility_col:
        st.markdown('<div class="tradly-utility-box">', unsafe_allow_html=True)
        section = st.selectbox(
            "Navigate",
            ["Decisions", "Market", "System"],
            index=0,
            label_visibility="collapsed",
            key="top_nav",
        )
        show_more_symbols = st.toggle("More Symbols", value=False, key="show_more_symbols")
        st.markdown("</div>", unsafe_allow_html=True)

    _render_top_status(freshness_snapshot, time_ctx.now_utc, state, reasons, warnings)

    if section == "Decisions":
        if state == "blocked":
            st.warning("System blocked. Do not act until blockers are cleared.")
        _render_action_board(ensemble_payload)
        if show_more_symbols:
            _render_symbol_stack(symbol_payload, symbol_news_payload, range_payload, ensemble_payload)
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
