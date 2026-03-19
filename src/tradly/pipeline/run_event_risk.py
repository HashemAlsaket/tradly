from __future__ import annotations

import http.client
import json
import os
from datetime import timedelta
from pathlib import Path

from tradly.config import get_model_registry_entry
from tradly.config.model_suite import load_openai_model_suite
from tradly.models.event_risk import build_event_risk_row
from tradly.paths import get_repo_root
from tradly.services.artifact_alignment import assess_artifact_alignment
from tradly.services.event_price_context import build_event_price_context
from tradly.services.event_window import load_event_windows
from tradly.services.time_context import get_time_context


MAX_UPSTREAM_AGE = timedelta(hours=6)
MAX_ARTICLES_PER_SYMBOL = 8


def _load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_latest_json(runs_dir: Path, pattern: str) -> dict:
    candidates = sorted(runs_dir.glob(pattern))
    if not candidates:
        return {}
    return _load_json_file(candidates[-1])


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _call_openai(*, model: str, api_key: str, system_text: str, user_text: str) -> dict:
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ],
    }
    conn = http.client.HTTPSConnection("api.openai.com", timeout=90)
    try:
        conn.request(
            "POST",
            "/v1/chat/completions",
            body=json.dumps(payload),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        response = conn.getresponse()
        body = response.read().decode("utf-8")
    finally:
        conn.close()

    if response.status >= 400:
        raise RuntimeError(f"openai_http_error status={response.status} body={body[:500]}")
    parsed = json.loads(body)
    choices = parsed.get("choices", [])
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("openai response missing choices")
    content = choices[0].get("message", {}).get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("openai response missing content")
    return json.loads(content)


def _select_articles(conn, *, symbol: str, event_ts_utc, now_utc) -> list[dict]:
    if event_ts_utc is None:
        return []
    lookback_start = event_ts_utc - timedelta(hours=24)
    rows = conn.execute(
        """
        WITH latest_interpretation AS (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY provider, provider_news_id
                   ORDER BY interpreted_at_utc DESC, prompt_version DESC
                 ) AS rn
          FROM news_interpretations
        )
        SELECT
          ne.provider_news_id,
          ne.published_at_utc,
          ne.source_name,
          ne.headline,
          ne.summary,
          ne.url,
          ns.relevance_score,
          li.impact_scope,
          li.impact_direction,
          li.impact_horizon,
          li.market_impact_note,
          li.thesis_tags_json
        FROM news_symbols ns
        JOIN news_events ne
          ON ne.provider = ns.provider
         AND ne.provider_news_id = ns.provider_news_id
        LEFT JOIN latest_interpretation li
          ON li.provider = ne.provider
         AND li.provider_news_id = ne.provider_news_id
         AND li.rn = 1
        WHERE ns.symbol = ?
          AND ne.published_at_utc >= ?
          AND ne.published_at_utc <= ?
        ORDER BY
          CASE
            WHEN LOWER(ne.headline) LIKE '%earnings call transcript%' THEN 5
            WHEN LOWER(ne.headline) LIKE '%reports%' OR LOWER(ne.headline) LIKE '%results%' OR LOWER(ne.headline) LIKE '%earnings%' THEN 4
            WHEN COALESCE(li.impact_scope, '') = 'symbol_specific' THEN 3
            WHEN COALESCE(li.impact_scope, '') = 'multiple' THEN 2
            ELSE 0
          END DESC,
          CASE
            WHEN ns.relevance_score >= 60 THEN 2
            WHEN ns.relevance_score >= 30 THEN 1
            ELSE 0
          END DESC,
          ns.relevance_score DESC,
          ne.published_at_utc DESC
        LIMIT ?
        """,
        [symbol, lookback_start, now_utc, MAX_ARTICLES_PER_SYMBOL],
    ).fetchall()
    out: list[dict] = []
    for row in rows:
        thesis_tags = []
        try:
            thesis_tags = json.loads(row[11] or "[]")
        except json.JSONDecodeError:
            thesis_tags = []
        out.append(
            {
                "provider_news_id": str(row[0]),
                "published_at_utc": row[1].isoformat() if row[1] else None,
                "source_name": str(row[2] or "").strip(),
                "headline": str(row[3] or "").strip(),
                "summary": str(row[4] or "").strip(),
                "url": str(row[5] or "").strip(),
                "relevance_score": float(row[6] or 0.0),
                "impact_scope": str(row[7] or "").strip(),
                "impact_direction": str(row[8] or "").strip(),
                "impact_horizon": str(row[9] or "").strip(),
                "market_impact_note": str(row[10] or "").strip(),
                "thesis_tags": thesis_tags if isinstance(thesis_tags, list) else [],
            }
        )
    return out


def _event_semantics_pass(*, model: str, api_key: str, symbol: str, event_context: dict, articles: list[dict]) -> dict:
    system_text = (
        "You extract structured event semantics for a trading system.\n"
        "Use only the provided articles and context. Return valid JSON only.\n"
        "When company-specific event articles are present, prioritize them over broad market-wrap or macro context pieces."
    )
    user_text = (
        "Given the event context and related articles, return this exact JSON shape:\n"
        "{\n"
        '  "event_type": "earnings|unknown",\n'
        '  "reported_result_tone": "positive|negative|mixed|unclear",\n'
        '  "guidance_tone": "positive|negative|mixed|unclear",\n'
        '  "dominant_positive": "short phrase",\n'
        '  "dominant_negative": "short phrase",\n'
        '  "dominant_market_concern": "short phrase",\n'
        '  "summary_note": "one sentence summary"\n'
        "}\n"
        "Do not speculate beyond the articles. If the articles are inconclusive, use `unclear`.\n"
        "Prefer the dominant company-event takeaway over generic market color when both are present.\n"
        "If earnings/report/call-transcript articles are present, use them as the primary evidence base.\n"
        "Broad market-wrap or macro articles should only add context and should not dominate the answer.\n"
        f"Symbol: {symbol}\n"
        f"Event context: {json.dumps(event_context, ensure_ascii=True)}\n"
        f"Articles: {json.dumps(articles, ensure_ascii=True)}"
    )
    return _call_openai(model=model, api_key=api_key, system_text=system_text, user_text=user_text)


def _reaction_pass(
    *,
    model: str,
    api_key: str,
    symbol: str,
    event_semantics: dict,
    price_context: dict,
) -> dict:
    system_text = (
        "You interpret post-event market reaction for a trading system.\n"
        "Use the provided structured facts only. Return valid JSON only."
    )
    user_text = (
        "Return this exact JSON shape:\n"
        "{\n"
        '  "reaction_state": "clean_positive_confirmation|beat_but_rejected|miss_and_breakdown|mixed_uncertain|macro_overwhelmed|awaiting_reaction|no_event_active",\n'
        '  "reaction_severity": "low|medium|high",\n'
        '  "confidence_adjustment": -20,\n'
        '  "action_bias": "upgrade|downgrade|hold",\n'
        '  "hard_cap_buy_to_watch": true,\n'
        '  "reason_codes": ["code_one"],\n'
        '  "summary_note": "one sentence explanation"\n'
        "}\n"
        "Interpret the reaction generically, not symbol-specifically. A strong report with a sharply negative after-hours move can still be `beat_but_rejected`.\n"
        "Use `macro_overwhelmed` only when broad tape or macro stress clearly dominates the event narrative.\n"
        "Use negative confidence adjustments for damaged setups and positive ones only for clean confirmation.\n"
        f"Symbol: {symbol}\n"
        f"Event semantics: {json.dumps(event_semantics, ensure_ascii=True)}\n"
        f"Price and market context: {json.dumps(price_context, ensure_ascii=True)}"
    )
    return _call_openai(model=model, api_key=api_key, system_text=system_text, user_text=user_text)


def main() -> int:
    repo_root = get_repo_root()
    runs_dir = repo_root / "data" / "runs"
    manual_dir = repo_root / "data" / "manual"
    db_path = repo_root / "data" / "tradly.duckdb"
    time_ctx = get_time_context()

    market_payload = _load_latest_json(runs_dir, "*/market_regime_v1.json")
    recommendation_payload = _load_latest_json(runs_dir, "*/recommendation_v1.json")
    universe_registry = _load_json_file(manual_dir / "universe_registry.json")
    if not market_payload:
        print("event_risk_v1_failed:market_regime_missing")
        return 1
    if not recommendation_payload:
        print("event_risk_v1_failed:recommendation_missing")
        return 2
    watchlist_path = manual_dir / "earnings_watchlist.json"
    if not watchlist_path.exists():
        print("event_risk_v1_failed:earnings_watchlist_missing")
        return 3

    alignments = {
        "market_regime_v1": assess_artifact_alignment(
            artifact_name="market_regime_v1",
            payload=market_payload,
            now_utc=time_ctx.now_utc,
            max_age=MAX_UPSTREAM_AGE,
        ),
        "recommendation_v1": assess_artifact_alignment(
            artifact_name="recommendation_v1",
            payload=recommendation_payload,
            now_utc=time_ctx.now_utc,
            max_age=MAX_UPSTREAM_AGE,
        ),
    }
    stale = [name for name, alignment in alignments.items() if not alignment.valid]
    if stale:
        print(f"event_risk_v1_failed:stale_upstream:{','.join(sorted(stale))}")
        return 4

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 5

    event_windows = load_event_windows(watchlist_path=watchlist_path, now_utc=time_ctx.now_utc)
    market_row = (market_payload.get("rows") or [{}])[0]
    recommendation_rows = {
        str(row.get("scope_id", "")).strip().upper(): row
        for row in recommendation_payload.get("rows", [])
        if isinstance(row, dict)
    }
    symbol_metadata = {
        str(row.get("symbol", "")).strip().upper(): row
        for row in universe_registry.get("symbols", [])
        if isinstance(row, dict) and str(row.get("symbol", "")).strip()
    }

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        snapshot_rows = conn.execute(
            """
            WITH latest AS (
              SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of_utc DESC) AS rn
              FROM market_snapshots
            )
            SELECT symbol, last_trade_price, prev_close, change_pct, session_close
            FROM latest
            WHERE rn = 1
            """
        ).fetchall()
    finally:
        conn.close()
    snapshot_by_symbol = {
        str(symbol).strip().upper(): {
            "last_trade_price": last_trade_price,
            "prev_close": prev_close,
            "change_pct": change_pct,
            "session_close": session_close,
        }
        for symbol, last_trade_price, prev_close, change_pct, session_close in snapshot_rows
    }

    dotenv_path = repo_root / ".env"
    _load_dotenv(dotenv_path)
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = load_openai_model_suite().llm_model

    rows: list[dict] = []
    active_event_count = 0
    llm_symbols: list[str] = []

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        for symbol, event_window in sorted(event_windows.items()):
            recommendation_row = recommendation_rows.get(symbol)
            price_context_obj = build_event_price_context(
                symbol=symbol,
                snapshot_by_symbol=snapshot_by_symbol,
                symbol_metadata=symbol_metadata,
                market_row=market_row,
                recommendation_row=recommendation_row,
            )
            price_context = {
                "price_reaction_pct": price_context_obj.price_reaction_pct,
                "move_vs_qqq_pct": price_context_obj.move_vs_qqq_pct,
                "move_vs_sector_pct": price_context_obj.move_vs_sector_pct,
                "market_session_state": price_context_obj.market_session_state,
                "market_regime": price_context_obj.market_regime,
                "macro_state": price_context_obj.macro_state,
                "current_action": price_context_obj.current_action,
                "current_confidence": price_context_obj.current_confidence,
                "reaction_window_ready": price_context_obj.reaction_window_ready,
                "sector_proxy_symbol": price_context_obj.sector_proxy_symbol,
            }

            articles = _select_articles(
                conn,
                symbol=symbol,
                event_ts_utc=event_window.event_timestamp_utc,
                now_utc=time_ctx.now_utc,
            )

            if not event_window.event_active:
                rows.append(
                    build_event_risk_row(
                        symbol=symbol,
                        event_active=False,
                        event_type=event_window.event_type,
                        event_phase=event_window.event_phase,
                        event_timestamp_local=event_window.event_timestamp_local,
                        event_source=event_window.source_note or "earnings_watchlist",
                        articles_considered=len(articles),
                        event_semantics={},
                        reaction_interpretation={},
                        price_context=price_context,
                    )
                )
                continue

            active_event_count += 1
            event_context = {
                "company": event_window.company,
                "event_type": event_window.event_type,
                "event_phase": event_window.event_phase,
                "event_timestamp_local": event_window.event_timestamp_local,
                "notes": event_window.notes,
                "source_note": event_window.source_note,
            }
            if not articles:
                event_semantics = {
                    "event_type": event_window.event_type,
                    "reported_result_tone": "unclear",
                    "guidance_tone": "unclear",
                    "dominant_positive": "",
                    "dominant_negative": "",
                    "dominant_market_concern": "",
                    "summary_note": "No event-specific articles were available for synthesis in the current event window.",
                }
                reaction_interpretation = {
                    "reaction_state": "awaiting_reaction" if event_window.event_phase == "pre_event" else "mixed_uncertain",
                    "reaction_severity": "low",
                    "confidence_adjustment": 0,
                    "action_bias": "hold",
                    "hard_cap_buy_to_watch": False,
                    "reason_codes": ["event_articles_missing"],
                    "summary_note": "Event window is active but there was not enough event-specific article coverage to form a stronger reaction judgment.",
                }
            else:
                if not api_key:
                    print("event_risk_v1_failed:openai_api_key_missing")
                    return 6
                llm_symbols.append(symbol)
                event_semantics = _event_semantics_pass(
                    model=model,
                    api_key=api_key,
                    symbol=symbol,
                    event_context=event_context,
                    articles=articles,
                )
                reaction_interpretation = _reaction_pass(
                    model=model,
                    api_key=api_key,
                    symbol=symbol,
                    event_semantics=event_semantics,
                    price_context=price_context,
                )

            rows.append(
                build_event_risk_row(
                    symbol=symbol,
                    event_active=True,
                    event_type=event_window.event_type,
                    event_phase=event_window.event_phase,
                    event_timestamp_local=event_window.event_timestamp_local,
                    event_source=event_window.source_note or "earnings_watchlist",
                    articles_considered=len(articles),
                    event_semantics=event_semantics,
                    reaction_interpretation=reaction_interpretation,
                    price_context=price_context,
                )
            )
    finally:
        conn.close()

    quality_failures: list[str] = []
    if active_event_count and not rows:
        quality_failures.append("active_events_missing_rows")
    input_status = "ready" if not quality_failures else "thin_evidence"

    registry_entry = get_model_registry_entry("event_risk_v1")
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "event_risk_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "watchlist_symbol_count": len(event_windows),
            "active_event_count": active_event_count,
            "llm_symbol_count": len(llm_symbols),
            "llm_symbols": llm_symbols,
        },
        "input_audit": {
            "status": input_status,
            "aligned_artifacts": {
                name: {
                    "run_timestamp_utc": alignment.run_timestamp_utc,
                    "age_sec": alignment.age_sec,
                    "valid": alignment.valid,
                }
                for name, alignment in alignments.items()
            },
            "watchlist_path": str(watchlist_path),
        },
        "quality_audit": {
            "status": "pass" if not quality_failures else "fail",
            "failure_reasons": quality_failures,
            "summary": {
                "row_count": len(rows),
                "active_event_count": active_event_count,
            },
        },
        "rows": rows,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"event_risk_rows={len(rows)}")
    print(f"active_event_count={active_event_count}")
    print(f"quality_audit_status={payload['quality_audit']['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
