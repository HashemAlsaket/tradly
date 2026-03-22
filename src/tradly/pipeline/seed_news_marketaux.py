from __future__ import annotations

import http.client
import json
import os
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tradly.paths import ensure_path_allowed_for_duckdb_ingest, get_repo_root


WATCHLIST_PATH = Path("data/manual/news_seed_watchlists.json")
DEFAULT_MIN_SYMBOL_RELEVANCE = 15.0


class RateLimitReached(Exception):
    pass


class UsageLimitReached(Exception):
    pass


class FetchError(Exception):
    pass


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


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _fetch_page(api_token: str, symbols: list[str], limit: int, page: int) -> dict:
    params = urllib.parse.urlencode(
        {
            "api_token": api_token,
            "symbols": ",".join(symbols),
            "filter_entities": "true",
            "language": "en",
            "limit": str(limit),
            "page": str(page),
        }
    )
    conn = http.client.HTTPSConnection("api.marketaux.com", timeout=30)
    try:
        conn.request("GET", f"/v1/news/all?{params}")
        response = conn.getresponse()
        body = response.read().decode("utf-8")
    finally:
        conn.close()
    if response.status == 429:
        raise RateLimitReached(body[:300])
    if response.status == 402:
        raise UsageLimitReached(body[:300])
    if response.status >= 400:
        raise RuntimeError(f"status={response.status} reason={response.reason} body={body[:300]}")
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise RuntimeError("unexpected payload")
    return payload


def _load_watchlists(path: Path) -> tuple[int, int, int, dict[str, list[str]]]:
    payload = json.loads(ensure_path_allowed_for_duckdb_ingest(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("watchlist config must be object")
    lookback_days = int(payload.get("lookback_days", 21))
    limit_per_request = int(payload.get("limit_per_request", 3))
    max_pages_per_bucket = int(payload.get("max_pages_per_bucket", 250))
    buckets = payload.get("buckets", {})
    if not isinstance(buckets, dict):
        raise RuntimeError("buckets must be object")
    parsed_buckets: dict[str, list[str]] = {}
    for key, values in buckets.items():
        if not isinstance(values, list):
            continue
        parsed_buckets[key] = [str(v).strip().upper() for v in values if str(v).strip()]
    return lookback_days, limit_per_request, max_pages_per_bucket, parsed_buckets


def _apply_env_overrides(
    lookback_days: int,
    max_pages_per_bucket: int,
    buckets: dict[str, list[str]],
) -> tuple[int, int, dict[str, list[str]], int | None]:
    lookback_override = os.getenv("NEWS_SEED_LOOKBACK_DAYS", "").strip()
    pages_override = os.getenv("NEWS_SEED_MAX_PAGES_PER_BUCKET", "").strip()
    bucket_filter_raw = os.getenv("NEWS_SEED_BUCKETS", "").strip()
    symbol_filter_raw = os.getenv("NEWS_SEED_SYMBOLS", "").strip()
    request_cap_raw = os.getenv("NEWS_SEED_REQUEST_CAP", "").strip()

    if lookback_override:
        lookback_days = int(lookback_override)
    if pages_override:
        max_pages_per_bucket = int(pages_override)

    filtered_buckets = buckets
    if symbol_filter_raw:
        selected_symbols = [item.strip().upper() for item in symbol_filter_raw.split(",") if item.strip()]
        filtered_buckets = {"targeted_symbols": selected_symbols}
    elif bucket_filter_raw:
        selected = [item.strip() for item in bucket_filter_raw.split(",") if item.strip()]
        filtered_buckets = {name: symbols for name, symbols in buckets.items() if name in selected}

    request_cap = int(request_cap_raw) if request_cap_raw else None
    return lookback_days, max_pages_per_bucket, filtered_buckets, request_cap


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")
    db_path = repo_root / "data" / "tradly.duckdb"
    watchlist_path = repo_root / WATCHLIST_PATH

    if not db_path.exists():
        print(f"db file not found: {db_path}")
        return 1
    if not watchlist_path.exists():
        print(f"watchlist file not found: {watchlist_path}")
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

    lookback_days, limit_per_request, max_pages_per_bucket, buckets = _load_watchlists(watchlist_path)
    lookback_days, max_pages_per_bucket, buckets, request_cap = _apply_env_overrides(
        lookback_days=lookback_days,
        max_pages_per_bucket=max_pages_per_bucket,
        buckets=buckets,
    )
    min_symbol_relevance = _min_symbol_relevance()
    if not buckets:
        print("no buckets configured")
        return 5

    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc - timedelta(days=lookback_days)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        allowed_symbols = {
            row[0]
            for row in conn.execute("SELECT symbol FROM instruments").fetchall()
            if row[0] is not None
        }
    finally:
        conn.close()

    seen_news_ids: set[str] = set()
    event_rows: list[tuple] = []
    symbol_rows: list[tuple] = []
    bucket_counts: dict[str, int] = {}
    filtered_symbol_links_total = 0
    requests_made = 0

    limited = False
    limit_reason = ""
    fetch_error_count = 0
    for bucket_name, symbols in buckets.items():
        bucket_inserted = 0
        for page in range(1, max_pages_per_bucket + 1):
            if request_cap is not None and requests_made >= request_cap:
                limited = True
                limit_reason = f"request_cap_reached:{request_cap}"
                break
            try:
                payload = _fetch_page(api_token, symbols, limit_per_request, page)
                requests_made += 1
            except RateLimitReached as exc:
                limited = True
                limit_reason = f"rate_limit:{exc}"
                break
            except UsageLimitReached as exc:
                limited = True
                limit_reason = f"usage_limit:{exc}"
                break
            except Exception as exc:
                fetch_error_count += 1
                limit_reason = f"fetch_error:{bucket_name}:page={page}:{exc}"
                break
            data = payload.get("data", [])
            if not isinstance(data, list) or not data:
                break

            oldest_on_page: datetime | None = None
            for item in data:
                news_id = str(item.get("uuid") or "").strip()
                title = str(item.get("title") or "").strip()
                published_at_text = str(item.get("published_at") or "").strip()
                published_at_dt = _parse_dt(published_at_text)
                if not news_id or not title or published_at_dt is None:
                    continue
                if oldest_on_page is None or published_at_dt < oldest_on_page:
                    oldest_on_page = published_at_dt
                if published_at_dt < cutoff_utc:
                    continue
                if news_id in seen_news_ids:
                    continue

                seen_news_ids.add(news_id)
                source_name = str(item.get("source") or "").strip() or "unknown"
                sentiment = item.get("sentiment")
                sentiment_label = str(sentiment).lower() if isinstance(sentiment, str) else None
                sentiment_score = item.get("sentiment_score")
                sentiment_score_val = float(sentiment_score) if sentiment_score is not None else None

                event_rows.append(
                    (
                        "marketaux",
                        news_id,
                        published_at_dt,
                        published_at_dt,
                        source_name,
                        bucket_name,
                        title,
                        str(item.get("description") or "").strip() or None,
                        str(item.get("url") or "").strip() or None,
                        sentiment_label,
                        sentiment_score_val,
                        None,
                        now_utc,
                    )
                )
                bucket_inserted += 1

                entities = item.get("entities") if isinstance(item.get("entities"), list) else []
                for ent in entities:
                    symbol = str(ent.get("symbol") or "").strip().upper()
                    if not symbol or symbol not in allowed_symbols:
                        continue
                    match_score = ent.get("match_score")
                    relevance = float(match_score) if match_score is not None else None
                    if relevance is None or relevance < min_symbol_relevance:
                        filtered_symbol_links_total += 1
                        continue
                    symbol_rows.append(
                        (
                            "marketaux",
                            news_id,
                            symbol,
                            relevance,
                            published_at_dt,
                            now_utc,
                        )
                    )

            if oldest_on_page is not None and oldest_on_page < cutoff_utc:
                break
        bucket_counts[bucket_name] = bucket_inserted
        if limited:
            break

    if not event_rows:
        print("news_seed_rows=0")
        print(f"cutoff_utc={cutoff_utc.isoformat()}")
        return 0

    conn = duckdb.connect(str(db_path))
    try:
        conn.begin()
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
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"cutoff_utc={cutoff_utc.isoformat()}")
    print(f"news_events_upserted={len(event_rows)}")
    print(f"news_symbols_upserted={len(symbol_rows)}")
    print(f"news_symbols_filtered_below_relevance={filtered_symbol_links_total}")
    print(f"min_symbol_relevance={min_symbol_relevance}")
    print(f"bucket_counts={bucket_counts}")
    print(f"requests_made={requests_made}")
    print(f"fetch_error_count={fetch_error_count}")
    if limited:
        print("warning=marketaux_limit_reached_partial_seed")
        print(f"limit_reason={limit_reason}")
    elif fetch_error_count > 0:
        print("warning=marketaux_fetch_errors_partial_seed")
        print(f"limit_reason={limit_reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
