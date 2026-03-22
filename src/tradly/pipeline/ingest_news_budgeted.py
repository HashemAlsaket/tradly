from __future__ import annotations

import http.client
import json
import os
import re
import socket
import time
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from tradly.paths import ensure_path_allowed_for_duckdb_ingest, get_repo_root
from tradly.services.db_time import to_db_utc
from tradly.services.news_bucket_health import REQUIRED_NEWS_BUCKETS
from tradly.services.time_context import get_time_context


WATCHLIST_PATH = Path("data/manual/news_seed_watchlists.json")
DEFAULT_DAILY_BUDGET = 100
DEFAULT_LIMIT_PER_REQUEST = 3
DEFAULT_PULLS_PER_BUCKET_PER_RUN = 1
DEFAULT_MIN_SYMBOL_RELEVANCE = 15.0
DEFAULT_HTTP_RETRY_COUNT = 2
DEFAULT_HTTP_RETRY_SLEEP_SEC = 2.0
NEWS_WATERMARK_SOURCE = "news_events_marketaux_bucket"
REQUIRED_BUCKETS = (
    "core_semis",
    "healthcare_core",
    "us_macro",
    "asia_semis",
    "asia_macro",
    "sector_context",
    "event_reserve",
)

LOW_VALUE_BUCKET_SOURCE_RULES: dict[str, dict[str, tuple[str, ...]]] = {
    "technology_core": {
        "source_names": (
            "quantifiedstrategies.com",
            "zerohedge.com",
        ),
        "headline_patterns": (
            r"\bdow jones\b",
            r"\bmarket slump\b",
            r"\bmarkets\b",
            r"\bopex\b",
            r"\betf\b",
            r"\btech sector under pressure\b",
            r"\bglobal markets\b",
            r"\bstock futures\b",
        ),
    },
    "communication_services_core": {
        "source_names": (
            "thestockmarketwatch.com",
        ),
        "headline_patterns": (
            r"\bapple\b",
            r"\biphone\b",
            r"\bbezos\b",
            r"\bglobal markets\b",
            r"\bstock futures\b",
            r"\bmarket slump\b",
            r"\bmarkets\b",
        ),
    },
    "consumer_defensive_core": {
        "source_names": (),
        "headline_patterns": (
            r"\beur/usd\b",
            r"\bai startup\b",
            r"\bfuture tech giants\b",
        ),
    },
}


def _bucket_override_int(
    overrides: dict[str, dict[str, int]],
    bucket: str,
    field: str,
    default_value: int,
) -> int:
    bucket_overrides = overrides.get(bucket, {})
    value = bucket_overrides.get(field, default_value)
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default_value
    return parsed if parsed > 0 else default_value


def _artifact_output_path(repo_root: Path, run_date: str) -> Path:
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / "news_budgeted_run.json"


def _news_item_filter_reason(bucket: str, source_name: str, headline: str) -> str | None:
    rules = LOW_VALUE_BUCKET_SOURCE_RULES.get(bucket)
    if not rules:
        return None
    source = source_name.strip().lower()
    title = headline.strip().lower()
    for blocked_source in rules.get("source_names", ()):
        if source == blocked_source:
            return f"source:{blocked_source}"
    for pattern in rules.get("headline_patterns", ()):
        if re.search(pattern, title):
            return f"headline:{pattern}"
    return None


def _min_symbol_relevance() -> float:
    raw = os.getenv("TRADLY_MARKETAUX_MIN_SYMBOL_RELEVANCE", str(DEFAULT_MIN_SYMBOL_RELEVANCE)).strip()
    try:
        return float(raw)
    except ValueError:
        print(
            f"warning=invalid_TRADLY_MARKETAUX_MIN_SYMBOL_RELEVANCE value={raw} "
            f"using_default={DEFAULT_MIN_SYMBOL_RELEVANCE}"
        )
        return DEFAULT_MIN_SYMBOL_RELEVANCE


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


def _normalize_published_after(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    # Marketaux accepts naive datetime/date filters (e.g. 2026-03-13T01:00:04 or 2026-03-13).
    # Normalize any timezone-aware input into naive UTC timestamp string.
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        # Fallback to raw value for date-only strings that are not full ISO datetimes.
        return raw


def _load_watchlists(path: Path) -> tuple[int, int, int, dict[str, int], dict[str, list[str]], dict[str, dict[str, int]]]:
    payload = json.loads(ensure_path_allowed_for_duckdb_ingest(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("watchlist config must be object")
    daily_budget = int(payload.get("daily_request_budget", DEFAULT_DAILY_BUDGET))
    limit_per_request = int(payload.get("limit_per_request", DEFAULT_LIMIT_PER_REQUEST))
    pulls_per_bucket_per_run = int(payload.get("pulls_per_bucket_per_run", DEFAULT_PULLS_PER_BUCKET_PER_RUN))
    caps_raw = payload.get("bucket_daily_caps", {})
    buckets_raw = payload.get("buckets", {})
    overrides_raw = payload.get("bucket_request_overrides", {})

    if not isinstance(caps_raw, dict) or not isinstance(buckets_raw, dict) or not isinstance(overrides_raw, dict):
        raise RuntimeError("bucket_daily_caps, bucket_request_overrides, and buckets must be objects")

    caps: dict[str, int] = {}
    for key, value in caps_raw.items():
        caps[str(key)] = int(value)

    buckets: dict[str, list[str]] = {}
    for key, values in buckets_raw.items():
        if not isinstance(values, list):
            continue
        parsed = [str(v).strip().upper() for v in values if str(v).strip()]
        if parsed:
            buckets[str(key)] = parsed
    overrides: dict[str, dict[str, int]] = {}
    for key, value in overrides_raw.items():
        if not isinstance(value, dict):
            continue
        normalized: dict[str, int] = {}
        for field in ("limit_per_request", "pulls_per_bucket_per_run"):
            if field not in value:
                continue
            try:
                parsed = int(value[field])
            except (TypeError, ValueError):
                raise RuntimeError(f"invalid_bucket_request_override:{key}:{field}")
            if parsed <= 0:
                raise RuntimeError(f"invalid_bucket_request_override:{key}:{field}")
            normalized[field] = parsed
        if normalized:
            overrides[str(key)] = normalized
    if not buckets:
        raise RuntimeError("no buckets configured")
    missing_required = [bucket for bucket in REQUIRED_BUCKETS if bucket not in buckets]
    if missing_required:
        raise RuntimeError(f"missing_required_buckets:{','.join(missing_required)}")
    empty_required = [bucket for bucket in REQUIRED_BUCKETS if not buckets.get(bucket)]
    if empty_required:
        raise RuntimeError(f"empty_required_buckets:{','.join(empty_required)}")
    if pulls_per_bucket_per_run <= 0:
        pulls_per_bucket_per_run = DEFAULT_PULLS_PER_BUCKET_PER_RUN
    return daily_budget, limit_per_request, pulls_per_bucket_per_run, caps, buckets, overrides


def _fetch_marketaux_news(
    api_token: str,
    symbols: list[str],
    limit: int,
    published_after_utc: str | None,
    page: int,
) -> tuple[int, str, list[dict], int]:
    params = urllib.parse.urlencode(
        {
            "api_token": api_token,
            "symbols": ",".join(symbols),
            "filter_entities": "true",
            "language": "en",
            "limit": str(limit),
            "page": str(page),
            **({"published_after": published_after_utc} if published_after_utc else {}),
        }
    )
    retry_count_raw = os.getenv("TRADLY_NEWS_HTTP_RETRY_COUNT", str(DEFAULT_HTTP_RETRY_COUNT)).strip()
    retry_sleep_raw = os.getenv("TRADLY_NEWS_HTTP_RETRY_SLEEP_SEC", str(DEFAULT_HTTP_RETRY_SLEEP_SEC)).strip()
    try:
        retry_count = max(0, int(retry_count_raw))
    except ValueError:
        retry_count = DEFAULT_HTTP_RETRY_COUNT
    try:
        retry_sleep_sec = max(0.0, float(retry_sleep_raw))
    except ValueError:
        retry_sleep_sec = DEFAULT_HTTP_RETRY_SLEEP_SEC

    last_error: Exception | None = None
    for attempt in range(retry_count + 1):
        conn = http.client.HTTPSConnection("api.marketaux.com", timeout=30)
        try:
            conn.request("GET", f"/v1/news/all?{params}")
            response = conn.getresponse()
            body = response.read().decode("utf-8")
        except (TimeoutError, socket.timeout, http.client.HTTPException, OSError) as exc:
            last_error = exc
            body = f"network_error:{exc}"
            if attempt >= retry_count:
                return 0, body, [], attempt
            if retry_sleep_sec > 0:
                time.sleep(retry_sleep_sec * (attempt + 1))
            continue
        finally:
            conn.close()

        if response.status >= 400:
            return response.status, body, [], attempt
        payload = json.loads(body)
        data = payload.get("data", [])
        if not isinstance(data, list):
            raise RuntimeError("marketaux response missing data list")
        return response.status, body, data, attempt

    return 0, f"network_error:{last_error}", [], retry_count


def _ensure_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_pull_usage (
          usage_id TEXT PRIMARY KEY,
          provider TEXT NOT NULL,
          bucket TEXT NOT NULL,
          symbols_csv TEXT NOT NULL,
          request_count INTEGER NOT NULL,
          request_date_utc DATE NOT NULL,
          response_status TEXT NOT NULL,
          detail TEXT,
          new_events_upserted INTEGER NOT NULL DEFAULT 0,
          new_symbol_links_upserted INTEGER NOT NULL DEFAULT 0,
          created_at_utc TIMESTAMP NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_watermarks (
          source_name TEXT NOT NULL,
          scope_key TEXT NOT NULL,
          watermark_ts_utc TIMESTAMP,
          watermark_meta_json TEXT,
          updated_at_utc TIMESTAMP NOT NULL,
          PRIMARY KEY (source_name, scope_key)
        )
        """
    )


def _load_news_watermarks(conn, buckets: list[str]) -> dict[str, datetime]:
    if not buckets:
        return {}
    rows = conn.execute(
        """
        SELECT scope_key, watermark_ts_utc
        FROM pipeline_watermarks
        WHERE source_name = ?
          AND scope_key IN ({placeholders})
        """.format(placeholders=",".join("?" for _ in buckets)),
        [NEWS_WATERMARK_SOURCE, *buckets],
    ).fetchall()
    return {str(scope_key): watermark_ts_utc for scope_key, watermark_ts_utc in rows if watermark_ts_utc is not None}


def _effective_published_after(
    env_published_after_utc: str | None,
    watermark_ts_utc: datetime | None,
) -> str | None:
    env_value = _normalize_published_after(env_published_after_utc)
    if env_value:
        return env_value
    if watermark_ts_utc is None:
        return None
    return watermark_ts_utc.strftime("%Y-%m-%dT%H:%M:%S")


def _parse_marketaux_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _should_continue_news_pagination(
    *,
    page_articles: list[dict],
    previous_watermark: datetime | None,
) -> bool:
    if not page_articles:
        return False
    if previous_watermark is None:
        return True
    oldest_article = None
    for item in page_articles:
        published_at = _parse_marketaux_published_at(str(item.get("published_at") or "").strip())
        if published_at is None:
            continue
        if oldest_article is None or published_at < oldest_article:
            oldest_article = published_at
    if oldest_article is None:
        return False
    return oldest_article > previous_watermark


def _upsert_news_watermarks(conn, per_bucket_max_published_at: dict[str, datetime], updated_at: datetime) -> None:
    if not per_bucket_max_published_at:
        return
    conn.executemany(
        """
        INSERT INTO pipeline_watermarks (
          source_name,
          scope_key,
          watermark_ts_utc,
          watermark_meta_json,
          updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (source_name, scope_key) DO UPDATE SET
          watermark_ts_utc = excluded.watermark_ts_utc,
          watermark_meta_json = excluded.watermark_meta_json,
          updated_at_utc = excluded.updated_at_utc
        """,
        [
            (
                NEWS_WATERMARK_SOURCE,
                bucket,
                published_at,
                json.dumps({"mode": "published_at_high_water_mark"}, ensure_ascii=True),
                updated_at,
            )
            for bucket, published_at in per_bucket_max_published_at.items()
        ],
    )


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")

    db_path = repo_root / "data" / "tradly.duckdb"
    watchlist_path = repo_root / WATCHLIST_PATH
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        return 1
    if not watchlist_path.exists():
        print(f"watchlist config missing: {watchlist_path}")
        return 2

    api_token = os.getenv("MARKETAUX_API_KEY")
    if not api_token:
        print("MARKETAUX_API_KEY missing")
        return 3

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 4

    try:
        daily_budget, limit_per_request, pulls_per_bucket_per_run, caps, buckets, bucket_request_overrides = _load_watchlists(watchlist_path)
    except RuntimeError as exc:
        print(f"invalid_watchlist_contract:{exc}")
        return 5
    expected_budget_raw = os.getenv("TRADLY_NEWS_EXPECTED_DAILY_BUDGET", "").strip()
    run_max_requests_raw = os.getenv("TRADLY_NEWS_RUN_MAX_REQUESTS", "60").strip()
    try:
        run_max_requests = int(run_max_requests_raw)
    except ValueError:
        print(f"invalid TRADLY_NEWS_RUN_MAX_REQUESTS={run_max_requests_raw}")
        return 6
    if run_max_requests <= 0:
        print(f"invalid TRADLY_NEWS_RUN_MAX_REQUESTS={run_max_requests}")
        return 7
    if expected_budget_raw:
        try:
            expected_budget = int(expected_budget_raw)
            if expected_budget > 0 and expected_budget != daily_budget:
                print(
                    f"warning=daily_budget_mismatch configured={daily_budget} expected={expected_budget}"
                )
        except ValueError:
            print(f"warning=invalid_expected_budget TRADLY_NEWS_EXPECTED_DAILY_BUDGET={expected_budget_raw}")

    time_ctx = get_time_context()
    now_db_utc = to_db_utc(time_ctx.now_utc)
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    published_after_utc_raw = os.getenv("TRADLY_NEWS_PUBLISHED_AFTER_UTC", "")
    min_symbol_relevance = _min_symbol_relevance()
    # Budgeting is aligned to trader workflow timezone (America/Chicago), not UTC midnight.
    request_date_utc = time_ctx.now_local.date()

    conn = duckdb.connect(str(db_path))
    _ensure_tables(conn)
    try:
        used_total = int(
            conn.execute(
                """
                SELECT COALESCE(SUM(request_count), 0)
                FROM news_pull_usage
                WHERE request_date_utc = ?
                """,
                (request_date_utc,),
            ).fetchone()[0]
        )

        used_by_bucket_rows = conn.execute(
            """
            SELECT bucket, COALESCE(SUM(request_count), 0)
            FROM news_pull_usage
            WHERE request_date_utc = ?
            GROUP BY bucket
            """,
            (request_date_utc,),
        ).fetchall()
        used_by_bucket = {str(bucket): int(count) for bucket, count in used_by_bucket_rows}

        allowed_symbols = {
            row[0]
            for row in conn.execute("SELECT symbol FROM instruments").fetchall()
            if row[0] is not None
        }
        invalid_bucket_symbols: dict[str, list[str]] = {}
        for bucket, symbols in buckets.items():
            invalid = sorted(symbol for symbol in symbols if symbol not in allowed_symbols)
            if invalid:
                invalid_bucket_symbols[bucket] = invalid
        if invalid_bucket_symbols:
            print("invalid_watchlist_contract:bucket_symbols_missing_from_instruments")
            for bucket, invalid in sorted(invalid_bucket_symbols.items()):
                print(f"error=bucket_symbol_missing:{bucket}:{','.join(invalid)}")
            return 8

        events_upserted_total = 0
        symbols_upserted_total = 0
        filtered_symbol_links_total = 0
        requests_made = 0
        stop_all_buckets = False
        news_watermarks = _load_news_watermarks(conn, list(buckets.keys()))
        per_bucket_max_published_at: dict[str, datetime] = {}
        bucket_results: dict[str, dict[str, object]] = {
            bucket: {
                "configured": True,
                "freshness_required": bucket in REQUIRED_NEWS_BUCKETS,
                "previous_watermark_utc": news_watermarks.get(bucket).isoformat() if news_watermarks.get(bucket) else None,
                "latest_published_at_utc": None,
                "last_response_status": None,
                "requests_made": 0,
                "events_upserted": 0,
                "symbol_links_upserted": 0,
                "filtered_article_count": 0,
                "filtered_article_reasons": {},
            }
            for bucket in buckets
        }
        filtered_articles_total = 0
        filtered_articles_by_bucket: dict[str, int] = defaultdict(int)
        filtered_article_reason_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for bucket, symbols in buckets.items():
            if stop_all_buckets:
                break
            if used_total >= daily_budget:
                print(f"budget_stop=daily_budget_reached used={used_total} budget={daily_budget}")
                break
            cap = int(caps.get(bucket, 0))
            if cap <= 0:
                print(f"bucket_skip={bucket} reason=no_cap")
                continue
            bucket_used = int(used_by_bucket.get(bucket, 0))
            if bucket_used >= cap:
                print(f"bucket_skip={bucket} reason=cap_reached used={bucket_used} cap={cap}")
                continue

            previous_watermark = news_watermarks.get(bucket)
            effective_published_after_utc = _effective_published_after(
                published_after_utc_raw,
                previous_watermark,
            )
            bucket_limit_per_request = _bucket_override_int(
                bucket_request_overrides,
                bucket,
                "limit_per_request",
                limit_per_request,
            )
            bucket_pulls_per_run = _bucket_override_int(
                bucket_request_overrides,
                bucket,
                "pulls_per_bucket_per_run",
                pulls_per_bucket_per_run,
            )
            for page in range(1, bucket_pulls_per_run + 1):
                if requests_made >= run_max_requests:
                    stop_all_buckets = True
                    print(
                        f"run_stop=max_requests_reached requests_made={requests_made} run_max_requests={run_max_requests}"
                    )
                    break
                if used_total >= daily_budget:
                    break
                if bucket_used >= cap:
                    break

                try:
                    status_code, body, articles, retry_attempts = _fetch_marketaux_news(
                        api_token,
                        symbols,
                        bucket_limit_per_request,
                        effective_published_after_utc,
                        page,
                    )
                except Exception as exc:
                    status_code = 0
                    body = f"network_error:{exc}"
                    articles = []
                requests_made += 1
                bucket_results[bucket]["requests_made"] = int(bucket_results[bucket]["requests_made"]) + 1
                used_total += 1
                bucket_used += 1
                used_by_bucket[bucket] = bucket_used

                response_status = "success"
                detail = f"http_status={status_code}"
                if status_code == 429:
                    response_status = "limit_reached"
                    detail = "marketaux_rate_limit_reached"
                elif status_code == 0:
                    response_status = "http_error"
                    detail = body[:250]
                elif status_code >= 400:
                    response_status = "http_error"
                    detail = f"http_status={status_code} body={body[:250]}"
                if retry_attempts > 0:
                    detail = f"{detail} retries={retry_attempts}"

                event_rows: list[tuple] = []
                symbol_rows: list[tuple] = []
                filtered_symbol_links = 0
                filtered_articles = 0
                if response_status == "success":
                    bucket_latest_published_at = per_bucket_max_published_at.get(bucket)
                    for item in articles:
                        news_id = str(item.get("uuid") or "").strip()
                        title = str(item.get("title") or "").strip()
                        published_at = str(item.get("published_at") or "").strip()
                        if not news_id or not title or not published_at:
                            continue
                        published_at_dt = _parse_marketaux_published_at(published_at)
                        if published_at_dt is not None and (
                            bucket_latest_published_at is None or published_at_dt > bucket_latest_published_at
                        ):
                            bucket_latest_published_at = published_at_dt

                        source = item.get("source") if isinstance(item.get("source"), str) else "unknown"
                        filter_reason = _news_item_filter_reason(bucket, source, title)
                        if filter_reason:
                            filtered_articles += 1
                            filtered_articles_total += 1
                            filtered_articles_by_bucket[bucket] += 1
                            filtered_article_reason_counts[bucket][filter_reason] += 1
                            continue
                        sentiment = item.get("sentiment")
                        event_rows.append(
                            (
                                "marketaux",
                                news_id,
                                published_at,
                                published_at,
                                source,
                                bucket,
                                title,
                                str(item.get("description") or "").strip() or None,
                                str(item.get("url") or "").strip() or None,
                                str(sentiment).lower() if isinstance(sentiment, str) else None,
                                float(item.get("sentiment_score")) if item.get("sentiment_score") is not None else None,
                                None,
                                now_db_utc,
                            )
                        )

                        entities = item.get("entities") if isinstance(item.get("entities"), list) else []
                        for ent in entities:
                            symbol = str(ent.get("symbol") or "").strip().upper()
                            relevance = float(ent.get("match_score")) if ent.get("match_score") is not None else None
                            if not symbol or symbol not in allowed_symbols:
                                continue
                            if relevance is None or relevance < min_symbol_relevance:
                                filtered_symbol_links += 1
                                continue
                            symbol_rows.append(
                                (
                                    "marketaux",
                                    news_id,
                                    symbol,
                                    relevance,
                                    published_at,
                                    now_db_utc,
                                )
                            )
                    if bucket_latest_published_at is not None:
                        per_bucket_max_published_at[bucket] = bucket_latest_published_at

                if event_rows:
                    conn.executemany(
                        """
                        INSERT INTO news_events (
                          provider, provider_news_id, published_at_utc, as_of_utc, source_name, source_quality,
                          headline, summary, url, sentiment_label, sentiment_score, extraction_confidence, ingested_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(provider, provider_news_id) DO UPDATE SET
                          published_at_utc=excluded.published_at_utc,
                          as_of_utc=excluded.as_of_utc,
                          source_name=excluded.source_name,
                          source_quality=excluded.source_quality,
                          headline=excluded.headline,
                          summary=excluded.summary,
                          url=excluded.url,
                          sentiment_label=excluded.sentiment_label,
                          sentiment_score=excluded.sentiment_score,
                          ingested_at_utc=excluded.ingested_at_utc
                        """,
                        event_rows,
                    )
                if symbol_rows:
                    conn.executemany(
                        """
                        INSERT INTO news_symbols (
                          provider, provider_news_id, symbol, relevance_score, as_of_utc, ingested_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(provider, provider_news_id, symbol) DO UPDATE SET
                          relevance_score=excluded.relevance_score,
                          as_of_utc=excluded.as_of_utc,
                          ingested_at_utc=excluded.ingested_at_utc
                        """,
                        symbol_rows,
                    )

                conn.execute(
                    """
                    INSERT INTO news_pull_usage (
                      usage_id, provider, bucket, symbols_csv, request_count, request_date_utc, response_status,
                      detail, new_events_upserted, new_symbol_links_upserted, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid4()),
                        "marketaux",
                        bucket,
                        ",".join(symbols),
                        1,
                        request_date_utc,
                        response_status,
                        detail,
                        len(event_rows),
                        len(symbol_rows),
                        now_db_utc,
                    ),
                )

                events_upserted_total += len(event_rows)
                symbols_upserted_total += len(symbol_rows)
                filtered_symbol_links_total += filtered_symbol_links
                bucket_results[bucket]["filtered_article_count"] = int(bucket_results[bucket]["filtered_article_count"]) + filtered_articles
                bucket_reason_counts = filtered_article_reason_counts.get(bucket, {})
                bucket_results[bucket]["filtered_article_reasons"] = {
                    reason: count for reason, count in sorted(bucket_reason_counts.items())
                }
                bucket_results[bucket]["last_response_status"] = response_status
                bucket_results[bucket]["events_upserted"] = int(bucket_results[bucket]["events_upserted"]) + len(event_rows)
                bucket_results[bucket]["symbol_links_upserted"] = int(bucket_results[bucket]["symbol_links_upserted"]) + len(symbol_rows)
                if bucket in per_bucket_max_published_at:
                    bucket_results[bucket]["latest_published_at_utc"] = per_bucket_max_published_at[bucket].isoformat()
                print(
                    f"bucket={bucket} status={response_status} req_used={used_by_bucket[bucket]}/{cap} "
                    f"events={len(event_rows)} symbol_links={len(symbol_rows)} "
                    f"filtered_symbol_links={filtered_symbol_links} filtered_articles={filtered_articles}"
                )
                if response_status == "limit_reached":
                    stop_all_buckets = True
                    break
                if response_status != "success":
                    break
                if not _should_continue_news_pagination(
                    page_articles=articles,
                    previous_watermark=previous_watermark,
                ):
                    break

        _upsert_news_watermarks(conn, per_bucket_max_published_at, now_db_utc)
        conn.commit()
    finally:
        conn.close()

    artifact_path = _artifact_output_path(repo_root, run_date)
    artifact_path.write_text(
        json.dumps(
            {
                "run_timestamp_utc": time_ctx.now_utc.isoformat(),
                "run_timestamp_local": time_ctx.now_local.isoformat(),
                "local_timezone": time_ctx.local_timezone,
                "provider": "marketaux",
                "requests_made": requests_made,
                "run_max_requests": run_max_requests,
                "daily_budget": daily_budget,
                "events_upserted": events_upserted_total,
                "symbol_links_upserted": symbols_upserted_total,
                "symbol_links_filtered_below_relevance": filtered_symbol_links_total,
                "filtered_articles_total": filtered_articles_total,
                "filtered_articles_by_bucket": {
                    bucket: filtered_articles_by_bucket[bucket] for bucket in sorted(filtered_articles_by_bucket)
                },
                "bucket_results": bucket_results,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"requests_made={requests_made}")
    print(f"run_max_requests={run_max_requests}")
    print(f"events_upserted={events_upserted_total}")
    print(f"symbol_links_upserted={symbols_upserted_total}")
    print(f"symbol_links_filtered_below_relevance={filtered_symbol_links_total}")
    print(f"filtered_articles_total={filtered_articles_total}")
    print(f"min_symbol_relevance={min_symbol_relevance}")
    print(f"daily_budget={daily_budget}")
    print(f"artifact={artifact_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
