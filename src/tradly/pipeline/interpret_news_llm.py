from __future__ import annotations

import http.client
import json
import os
from datetime import timedelta
from pathlib import Path

from tradly.config.model_suite import load_openai_model_suite
from tradly.paths import get_repo_root
from tradly.services.db_time import to_db_utc
from tradly.services.market_calendar import MARKET_TZ, build_trading_calendar_row, market_session_state
from tradly.services.time_context import get_time_context


PROMPT_VERSION = "news_interpreter_v1"
BATCH_SIZE = 12
DEFAULT_LOOKBACK_DAYS = 3
DEFAULT_PENDING_LIMIT = 240
DEFAULT_MAX_ROUNDS = 4
DEFAULT_BATCH_RETRIES = 2

ALLOWED_BUCKET = {"macro", "sector", "symbol", "asia", "ignore"}
ALLOWED_SCOPE = {
    "macro",
    "broad_market",
    "rates",
    "energy",
    "semis",
    "usd",
    "risk_sentiment",
    "technology",
    "healthcare",
    "financial_services",
    "industrials",
    "consumer_defensive",
    "communication_services",
    "consumer_cyclical",
    "basic_materials",
    "real_estate",
    "utilities",
    "symbol_specific",
    "multiple",
}
ALLOWED_DIRECTION = {"bullish", "bearish", "neutral", "mixed", "unclear", "risk_on", "risk_off"}
ALLOWED_HORIZON = {"intraday", "1to3d", "1to2w", "2to6w"}
ALLOWED_CONFIDENCE = {"low", "medium", "high"}
SCOPE_ALIASES = {
    "market": "broad_market",
    "broad market": "broad_market",
    "broad_market": "broad_market",
    "risk sentiment": "risk_sentiment",
    "risk-sentiment": "risk_sentiment",
    "financials": "financial_services",
    "financial services": "financial_services",
    "communication services": "communication_services",
    "communications": "communication_services",
    "consumer defensive": "consumer_defensive",
    "consumer staples": "consumer_defensive",
    "consumer cyclical": "consumer_cyclical",
    "consumer discretionary": "consumer_cyclical",
    "basic materials": "basic_materials",
    "materials": "basic_materials",
    "real estate": "real_estate",
    "media": "communication_services",
    "entertainment": "communication_services",
    "streaming": "communication_services",
    "media entertainment": "communication_services",
    "media and entertainment": "communication_services",
    "streaming media": "communication_services",
    "symbol-specific": "symbol_specific",
    "symbol specific": "symbol_specific",
}
PLACEHOLDER_SCOPE_VALUES = {"unclear", "unknown", "unsure", "n/a", "na", "none", "unspecified"}


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


def _call_openai(model: str, api_key: str, batch_articles: list[dict]) -> dict:
    system_text = (
        "You classify and interpret financial news for a manual-trading research system.\n"
        "You must not calculate numbers. You only interpret provided article evidence.\n"
        "Return valid JSON only."
    )
    user_text = (
        "For each article, output one interpretation object with this exact shape:\n"
        "{\n"
        '  "provider": "marketaux",\n'
        '  "provider_news_id": "id",\n'
        '  "bucket": "macro|sector|symbol|asia|ignore",\n'
        '  "impact_scope": "macro|broad_market|rates|energy|semis|usd|risk_sentiment|technology|healthcare|financial_services|industrials|consumer_defensive|communication_services|consumer_cyclical|basic_materials|real_estate|utilities|symbol_specific|multiple",\n'
        '  "impact_direction": "bullish|bearish|neutral|mixed|unclear|risk_on|risk_off",\n'
        '  "impact_horizon": "intraday|1to3d|1to2w|2to6w",\n'
        '  "relevance_symbols": ["MU"],\n'
        '  "thesis_tags": ["rates"],\n'
        '  "market_impact_note": "short plain English note",\n'
        '  "confidence_label": "low|medium|high",\n'
        '  "based_on_provided_evidence": true,\n'
        '  "calculation_performed": false\n'
        "}\n"
        "Interpretation rules:\n"
        "1. Use `symbol_specific` when the article is primarily about one or more named symbols.\n"
        "2. Use a canonical sector scope when the impact is mainly sector-level.\n"
        "   Canonical sector ids only: `technology`, `healthcare`, `financial_services`, `industrials`,\n"
        "   `consumer_defensive`, `communication_services`, `consumer_cyclical`, `basic_materials`,\n"
        "   `real_estate`, `utilities`, `energy`.\n"
        "   Do not output human-friendly aliases like `financials`, `consumer discretionary`,\n"
        "   `consumer staples`, `materials`, `communication services`, or `real estate`.\n"
        "3. Use exact scope ids from the allowed list only. Prefer underscores, not spaces.\n"
        "   Examples: `broad_market`, `risk_sentiment`, `symbol_specific`, `financial_services`.\n"
        "4. Use `multiple` only when the article clearly affects several distinct scopes and no single scope dominates.\n"
        "   Never output placeholder scopes like `unclear`, `unknown`, or `n/a`.\n"
        "   If scope is uncertain, choose the closest canonical scope instead.\n"
        "5. Use `bullish` or `bearish` for direct directional pressure on a sector or symbol.\n"
        "6. Use `risk_on` or `risk_off` for broader market tone or cross-asset posture.\n"
        "7. Use `2to6w` when the article's impact is more durable than a normal swing horizon.\n"
        "8. Treat `market_session_state`, `day_name`, `is_weekend`, `is_market_holiday`, and `last_cash_session_date` as important context.\n"
        "   Weekend or holiday timing does not mean the data is stale; it means the cash market is closed.\n"
        "9. On weekends or market holidays, avoid overusing very short-horizon calls unless the article is clearly about the next trading session.\n"
        "   Medium and position horizons may still be appropriate when the thesis is durable.\n"
        "10. For healthcare articles, classify the impact using healthcare-aware thesis tags when applicable.\n"
        "    Prefer concise tags such as `trial_readout`, `drug_approval`, `regulatory`, `pricing_reimbursement`,\n"
        "    `utilization_cost_pressure`, `devices_tools_demand`, `patent_litigation`, `defensive_earnings_resilience`.\n"
        "11. When a healthcare article is mainly about large-cap pharma or managed care, keep the impact scope as\n"
        "    `healthcare` or `symbol_specific` rather than forcing it into broad market or macro buckets.\n"
        'Return as: {"interpretations":[...]}.\n'
        f"Articles:\n{json.dumps(batch_articles, ensure_ascii=True)}"
    )

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

    out = json.loads(body)
    choices = out.get("choices", [])
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("openai response missing choices")
    content = choices[0].get("message", {}).get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("openai response missing content")
    return json.loads(content)


def _validate_record(row: dict) -> tuple[bool, str]:
    if not isinstance(row, dict):
        return False, "not_object"
    if str(row.get("bucket", "")).strip() not in ALLOWED_BUCKET:
        return False, "bucket_invalid"
    if str(row.get("impact_scope", "")).strip() not in ALLOWED_SCOPE:
        return False, "impact_scope_invalid"
    if str(row.get("impact_direction", "")).strip() not in ALLOWED_DIRECTION:
        return False, "impact_direction_invalid"
    if str(row.get("impact_horizon", "")).strip() not in ALLOWED_HORIZON:
        return False, "impact_horizon_invalid"
    if str(row.get("confidence_label", "")).strip().lower() not in ALLOWED_CONFIDENCE:
        return False, "confidence_invalid"
    if row.get("based_on_provided_evidence") is not True:
        return False, "not_evidence_based"
    if row.get("calculation_performed") is not False:
        return False, "calculation_flag_invalid"
    note = str(row.get("market_impact_note", "")).strip()
    if not note:
        return False, "missing_impact_note"
    if len(note) > 500:
        return False, "impact_note_too_long"
    if not isinstance(row.get("relevance_symbols"), list):
        return False, "relevance_symbols_invalid"
    if not isinstance(row.get("thesis_tags"), list):
        return False, "thesis_tags_invalid"
    return True, ""


def _normalize_impact_scope(value: object) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = raw.replace("/", " ").replace("-", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    if normalized in PLACEHOLDER_SCOPE_VALUES:
        return ""
    alias_hit = SCOPE_ALIASES.get(normalized)
    if alias_hit:
        return alias_hit
    candidate = normalized.replace(" ", "_")
    return candidate


def _normalize_record(row: dict) -> dict:
    normalized = dict(row)
    normalized["impact_scope"] = _normalize_impact_scope(row.get("impact_scope"))
    return normalized


def _ensure_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_interpretations (
          provider TEXT NOT NULL,
          provider_news_id TEXT NOT NULL,
          model TEXT NOT NULL,
          prompt_version TEXT NOT NULL,
          bucket TEXT NOT NULL,
          impact_scope TEXT NOT NULL,
          impact_direction TEXT NOT NULL,
          impact_horizon TEXT NOT NULL,
          relevance_symbols_json TEXT NOT NULL,
          thesis_tags_json TEXT NOT NULL,
          market_impact_note TEXT NOT NULL,
          confidence_label TEXT NOT NULL,
          based_on_provided_evidence BOOLEAN NOT NULL,
          calculation_performed BOOLEAN NOT NULL,
          interpreted_at_utc TIMESTAMP NOT NULL,
          ingested_at_utc TIMESTAMP NOT NULL,
          PRIMARY KEY (provider, provider_news_id, model, prompt_version)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_interpretation_rejections (
          provider TEXT NOT NULL,
          provider_news_id TEXT NOT NULL,
          model TEXT NOT NULL,
          prompt_version TEXT NOT NULL,
          rejection_reason TEXT NOT NULL,
          raw_impact_scope TEXT,
          raw_payload_json TEXT NOT NULL,
          normalized_payload_json TEXT NOT NULL,
          logged_at_utc TIMESTAMP NOT NULL,
          ingested_at_utc TIMESTAMP NOT NULL,
          PRIMARY KEY (provider, provider_news_id, model, prompt_version)
        )
        """
    )


def _load_pending_rows(conn, model: str, cutoff, limit: int) -> list[tuple]:
    return conn.execute(
        """
        WITH article_symbols AS (
          SELECT
            provider,
            provider_news_id,
            LIST(symbol) AS symbols
          FROM news_symbols
          GROUP BY provider, provider_news_id
        )
        SELECT
          ne.provider,
          ne.provider_news_id,
          ne.published_at_utc,
          ne.source_name,
          ne.headline,
          ne.summary,
          ne.url,
          ne.sentiment_label,
          ne.sentiment_score,
          ne.source_quality,
          COALESCE(asym.symbols, []) AS symbols
        FROM news_events ne
        LEFT JOIN article_symbols asym
          ON asym.provider = ne.provider
         AND asym.provider_news_id = ne.provider_news_id
        LEFT JOIN news_interpretations ni
          ON ni.provider = ne.provider
         AND ni.provider_news_id = ne.provider_news_id
         AND ni.model = ?
         AND ni.prompt_version = ?
        WHERE ne.published_at_utc >= ?
          AND ni.provider_news_id IS NULL
        ORDER BY ne.published_at_utc DESC
        LIMIT ?
        """,
        (model, PROMPT_VERSION, cutoff, limit),
    ).fetchall()


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")
    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        return 1

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY missing")
        return 2

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 3

    model_suite = load_openai_model_suite()
    model = model_suite.llm_model
    if not model:
        print("OPENAI_LLM_MODEL missing")
        return 4

    lookback_days_raw = os.getenv("NEWS_INTERPRET_LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS))
    try:
        lookback_days = int(lookback_days_raw)
    except ValueError:
        print(f"invalid NEWS_INTERPRET_LOOKBACK_DAYS={lookback_days_raw}")
        return 5
    if lookback_days <= 0:
        print(f"invalid NEWS_INTERPRET_LOOKBACK_DAYS={lookback_days}")
        return 6
    pending_limit_raw = os.getenv("NEWS_INTERPRET_PENDING_LIMIT", str(DEFAULT_PENDING_LIMIT))
    max_rounds_raw = os.getenv("NEWS_INTERPRET_MAX_ROUNDS", str(DEFAULT_MAX_ROUNDS))
    batch_retries_raw = os.getenv("NEWS_INTERPRET_BATCH_RETRIES", str(DEFAULT_BATCH_RETRIES))
    try:
        pending_limit = int(pending_limit_raw)
        max_rounds = int(max_rounds_raw)
        batch_retries = int(batch_retries_raw)
    except ValueError:
        print(
            "invalid interpreter config "
            f"pending_limit={pending_limit_raw} max_rounds={max_rounds_raw} batch_retries={batch_retries_raw}"
        )
        return 7
    if pending_limit <= 0 or max_rounds <= 0 or batch_retries <= 0:
        print(
            "invalid interpreter config "
            f"pending_limit={pending_limit} max_rounds={max_rounds} batch_retries={batch_retries}"
        )
        return 8

    time_ctx = get_time_context()
    now_db_utc = to_db_utc(time_ctx.now_utc)
    cutoff = time_ctx.now_utc - timedelta(days=lookback_days)
    recent_cutoff = time_ctx.now_utc - timedelta(hours=24)

    conn = duckdb.connect(str(db_path))
    _ensure_tables(conn)
    try:
        initial_rows = _load_pending_rows(conn, model, cutoff, pending_limit)
        if not initial_rows:
            print("news_to_interpret=0")
            return 0

        inserted = 0
        invalid_reason_counts: dict[str, int] = {}
        invalid_scope_examples: list[dict[str, str]] = []
        missing_for_batch_total = 0
        rounds_completed = 0
        rows_seen_total = 0
        for round_idx in range(max_rounds):
            rows = _load_pending_rows(conn, model, cutoff, pending_limit)
            if not rows:
                break
            rounds_completed += 1
            rows_seen_total += len(rows)

            pending = []
            for row in rows:
                published_at_utc = row[2]
                market_day = published_at_utc.astimezone(MARKET_TZ).date() if published_at_utc.tzinfo else published_at_utc.date()
                calendar_row = build_trading_calendar_row(market_day)
                current_calendar_row = build_trading_calendar_row(time_ctx.now_utc.astimezone(MARKET_TZ).date())
                pending.append(
                    {
                        "provider": row[0],
                        "provider_news_id": row[1],
                        "published_at_utc": str(published_at_utc),
                        "published_day_name": calendar_row.day_name,
                        "published_market_calendar_state": calendar_row.market_calendar_state,
                        "published_last_cash_session_date": calendar_row.last_cash_session_date.isoformat(),
                        "source_name": row[3],
                        "headline": row[4],
                        "summary": row[5],
                        "url": row[6],
                        "sentiment_label": row[7],
                        "sentiment_score": row[8],
                        "source_quality": row[9],
                        "symbols": row[10] if isinstance(row[10], list) else [],
                        "market_session_state_now": market_session_state(time_ctx.now_utc),
                        "current_day_name": current_calendar_row.day_name,
                        "current_market_calendar_state": current_calendar_row.market_calendar_state,
                        "current_last_cash_session_date": current_calendar_row.last_cash_session_date.isoformat(),
                    }
                )

            inserted_before_round = inserted
            for i in range(0, len(pending), BATCH_SIZE):
                batch = pending[i : i + BATCH_SIZE]
                batch_by_key = {(item["provider"], item["provider_news_id"]): item for item in batch}
                out = None
                for attempt in range(1, batch_retries + 1):
                    try:
                        out = _call_openai(model=model, api_key=api_key, batch_articles=batch)
                        break
                    except Exception as exc:
                        print(f"llm_batch_failed round={round_idx+1} index={i} attempt={attempt} err={exc}")
                if out is None:
                    continue

                interpretations = out.get("interpretations", [])
                if not isinstance(interpretations, list):
                    print(f"llm_batch_invalid round={round_idx+1} index={i} reason=missing_interpretations_list")
                    continue

                valid_by_key: dict[tuple[str, str], dict] = {}
                for item in interpretations:
                    raw_item = item if isinstance(item, dict) else None
                    item = _normalize_record(item) if isinstance(item, dict) else item
                    ok, reason = _validate_record(item)
                    if not ok:
                        invalid_reason_counts[reason] = invalid_reason_counts.get(reason, 0) + 1
                        if (
                            reason == "impact_scope_invalid"
                            and isinstance(item, dict)
                            and len(invalid_scope_examples) < 5
                        ):
                            provider = str(item.get("provider", "")).strip()
                            news_id = str(item.get("provider_news_id", "")).strip()
                            batch_item = batch_by_key.get((provider, news_id), {})
                            invalid_scope_examples.append(
                                {
                                    "provider_news_id": news_id,
                                    "raw_scope": str(item.get("impact_scope", "")).strip(),
                                    "headline": str(batch_item.get("headline", "")).strip()[:140],
                                }
                            )
                        if isinstance(item, dict):
                            provider = str(item.get("provider", "")).strip()
                            news_id = str(item.get("provider_news_id", "")).strip()
                            if provider and news_id:
                                raw_scope = ""
                                if isinstance(raw_item, dict):
                                    raw_scope = str(raw_item.get("impact_scope", "")).strip()
                                conn.execute(
                                    """
                                    INSERT INTO news_interpretation_rejections (
                                      provider, provider_news_id, model, prompt_version, rejection_reason, raw_impact_scope,
                                      raw_payload_json, normalized_payload_json, logged_at_utc, ingested_at_utc
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ON CONFLICT(provider, provider_news_id, model, prompt_version) DO UPDATE SET
                                      rejection_reason=excluded.rejection_reason,
                                      raw_impact_scope=excluded.raw_impact_scope,
                                      raw_payload_json=excluded.raw_payload_json,
                                      normalized_payload_json=excluded.normalized_payload_json,
                                      logged_at_utc=excluded.logged_at_utc,
                                      ingested_at_utc=excluded.ingested_at_utc
                                    """,
                                    (
                                        provider,
                                        news_id,
                                        model,
                                        PROMPT_VERSION,
                                        reason,
                                        raw_scope,
                                        json.dumps(raw_item or {}, ensure_ascii=True),
                                        json.dumps(item, ensure_ascii=True),
                                        now_db_utc,
                                        now_db_utc,
                                    ),
                                )
                        continue
                    provider = str(item.get("provider", "")).strip()
                    news_id = str(item.get("provider_news_id", "")).strip()
                    if not provider or not news_id:
                        invalid_reason_counts["missing_provider_or_news_id"] = (
                            invalid_reason_counts.get("missing_provider_or_news_id", 0) + 1
                        )
                        continue
                    valid_by_key[(provider, news_id)] = item

                batch_keys = {(a["provider"], a["provider_news_id"]) for a in batch}
                missing_for_batch = 0
                for key in batch_keys:
                    if key not in valid_by_key:
                        missing_for_batch += 1
                        continue
                    item = valid_by_key[key]
                    conn.execute(
                        """
                        INSERT INTO news_interpretations (
                          provider, provider_news_id, model, prompt_version, bucket, impact_scope, impact_direction,
                          impact_horizon, relevance_symbols_json, thesis_tags_json, market_impact_note, confidence_label,
                          based_on_provided_evidence, calculation_performed, interpreted_at_utc, ingested_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(provider, provider_news_id, model, prompt_version) DO UPDATE SET
                          bucket=excluded.bucket,
                          impact_scope=excluded.impact_scope,
                          impact_direction=excluded.impact_direction,
                          impact_horizon=excluded.impact_horizon,
                          relevance_symbols_json=excluded.relevance_symbols_json,
                          thesis_tags_json=excluded.thesis_tags_json,
                          market_impact_note=excluded.market_impact_note,
                          confidence_label=excluded.confidence_label,
                          based_on_provided_evidence=excluded.based_on_provided_evidence,
                          calculation_performed=excluded.calculation_performed,
                          interpreted_at_utc=excluded.interpreted_at_utc,
                          ingested_at_utc=excluded.ingested_at_utc
                        """,
                        (
                            key[0],
                            key[1],
                            model,
                            PROMPT_VERSION,
                            str(item["bucket"]).strip(),
                            str(item["impact_scope"]).strip(),
                            str(item["impact_direction"]).strip(),
                            str(item["impact_horizon"]).strip(),
                            json.dumps(item.get("relevance_symbols", []), ensure_ascii=True),
                            json.dumps(item.get("thesis_tags", []), ensure_ascii=True),
                            str(item["market_impact_note"]).strip(),
                            str(item["confidence_label"]).strip().lower(),
                            True,
                            False,
                            now_db_utc,
                            now_db_utc,
                        ),
                    )
                    inserted += 1
                missing_for_batch_total += missing_for_batch

            if inserted == inserted_before_round:
                break

        conn.commit()
        remaining_recent_pending = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM news_events ne
                LEFT JOIN news_interpretations ni
                  ON ni.provider = ne.provider
                 AND ni.provider_news_id = ne.provider_news_id
                 AND ni.model = ?
                 AND ni.prompt_version = ?
                WHERE ne.published_at_utc >= ?
                  AND ni.provider_news_id IS NULL
                """,
                (model, PROMPT_VERSION, recent_cutoff),
            ).fetchone()[0]
            or 0
        )
    finally:
        conn.close()

    print(f"news_to_interpret={len(initial_rows)}")
    print(f"rows_seen_total={rows_seen_total}")
    print(f"rounds_completed={rounds_completed}")
    print(f"interpretations_upserted={inserted}")
    print(f"interpretations_missing_for_batch_keys={missing_for_batch_total}")
    print(f"remaining_recent_pending_24h={remaining_recent_pending}")
    if invalid_reason_counts:
        print(f"interpretation_invalid_reasons={invalid_reason_counts}")
    if invalid_scope_examples:
        print(f"invalid_scope_examples={json.dumps(invalid_scope_examples, ensure_ascii=True)}")
    print(f"model={model}")
    print(f"prompt_version={PROMPT_VERSION}")
    print(f"lookback_days={lookback_days}")
    print(f"pending_limit={pending_limit}")
    print(f"max_rounds={max_rounds}")
    print(f"batch_retries={batch_retries}")
    if remaining_recent_pending > 0:
        print("recent_news_interpretation_backlog_remaining")
        return 9
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
