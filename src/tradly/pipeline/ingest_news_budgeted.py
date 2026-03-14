from __future__ import annotations

import http.client
import json
import os
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from tradly.paths import get_repo_root
from tradly.services.db_time import to_db_utc
from tradly.services.time_context import get_time_context


WATCHLIST_PATH = Path("data/manual/news_seed_watchlists.json")
DEFAULT_DAILY_BUDGET = 100
DEFAULT_LIMIT_PER_REQUEST = 3
DEFAULT_PULLS_PER_BUCKET_PER_RUN = 1


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


def _load_watchlists(path: Path) -> tuple[int, int, int, dict[str, int], dict[str, list[str]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("watchlist config must be object")
    daily_budget = int(payload.get("daily_request_budget", DEFAULT_DAILY_BUDGET))
    limit_per_request = int(payload.get("limit_per_request", DEFAULT_LIMIT_PER_REQUEST))
    pulls_per_bucket_per_run = int(payload.get("pulls_per_bucket_per_run", DEFAULT_PULLS_PER_BUCKET_PER_RUN))
    caps_raw = payload.get("bucket_daily_caps", {})
    buckets_raw = payload.get("buckets", {})

    if not isinstance(caps_raw, dict) or not isinstance(buckets_raw, dict):
        raise RuntimeError("bucket_daily_caps and buckets must be objects")

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
    if not buckets:
        raise RuntimeError("no buckets configured")
    if pulls_per_bucket_per_run <= 0:
        pulls_per_bucket_per_run = DEFAULT_PULLS_PER_BUCKET_PER_RUN
    return daily_budget, limit_per_request, pulls_per_bucket_per_run, caps, buckets


def _fetch_marketaux_news(
    api_token: str,
    symbols: list[str],
    limit: int,
    published_after_utc: str | None,
) -> tuple[int, str, list[dict]]:
    params = urllib.parse.urlencode(
        {
            "api_token": api_token,
            "symbols": ",".join(symbols),
            "filter_entities": "true",
            "language": "en",
            "limit": str(limit),
            **({"published_after": published_after_utc} if published_after_utc else {}),
        }
    )
    conn = http.client.HTTPSConnection("api.marketaux.com", timeout=30)
    try:
        conn.request("GET", f"/v1/news/all?{params}")
        response = conn.getresponse()
        body = response.read().decode("utf-8")
    finally:
        conn.close()

    if response.status >= 400:
        return response.status, body, []
    payload = json.loads(body)
    data = payload.get("data", [])
    if not isinstance(data, list):
        raise RuntimeError("marketaux response missing data list")
    return response.status, body, data


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

    daily_budget, limit_per_request, pulls_per_bucket_per_run, caps, buckets = _load_watchlists(watchlist_path)
    expected_budget_raw = os.getenv("TRADLY_NEWS_EXPECTED_DAILY_BUDGET", "").strip()
    run_max_requests_raw = os.getenv("TRADLY_NEWS_RUN_MAX_REQUESTS", "60").strip()
    try:
        run_max_requests = int(run_max_requests_raw)
    except ValueError:
        print(f"invalid TRADLY_NEWS_RUN_MAX_REQUESTS={run_max_requests_raw}")
        return 5
    if run_max_requests <= 0:
        print(f"invalid TRADLY_NEWS_RUN_MAX_REQUESTS={run_max_requests}")
        return 6
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
    published_after_utc = _normalize_published_after(os.getenv("TRADLY_NEWS_PUBLISHED_AFTER_UTC", ""))
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

        events_upserted_total = 0
        symbols_upserted_total = 0
        requests_made = 0
        stop_all_buckets = False

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

            for _pull_idx in range(pulls_per_bucket_per_run):
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
                    status_code, body, articles = _fetch_marketaux_news(
                        api_token,
                        symbols,
                        limit_per_request,
                        published_after_utc,
                    )
                except Exception as exc:
                    status_code = 0
                    body = f"network_error:{exc}"
                    articles = []
                requests_made += 1
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

                event_rows: list[tuple] = []
                symbol_rows: list[tuple] = []
                if response_status == "success":
                    for item in articles:
                        news_id = str(item.get("uuid") or "").strip()
                        title = str(item.get("title") or "").strip()
                        published_at = str(item.get("published_at") or "").strip()
                        if not news_id or not title or not published_at:
                            continue

                        source = item.get("source") if isinstance(item.get("source"), str) else "unknown"
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
                            if not symbol or symbol not in allowed_symbols:
                                continue
                            symbol_rows.append(
                                (
                                    "marketaux",
                                    news_id,
                                    symbol,
                                    float(ent.get("match_score")) if ent.get("match_score") is not None else None,
                                    published_at,
                                    now_db_utc,
                                )
                            )

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
                print(
                    f"bucket={bucket} status={response_status} req_used={used_by_bucket[bucket]}/{cap} "
                    f"events={len(event_rows)} symbol_links={len(symbol_rows)}"
                )
                if response_status == "limit_reached":
                    stop_all_buckets = True
                    break

        conn.commit()
    finally:
        conn.close()

    print(f"requests_made={requests_made}")
    print(f"run_max_requests={run_max_requests}")
    print(f"events_upserted={events_upserted_total}")
    print(f"symbol_links_upserted={symbols_upserted_total}")
    print(f"daily_budget={daily_budget}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
