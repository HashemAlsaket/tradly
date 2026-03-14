# API Usage Runbook (Proven Only)

Last validated: 2026-03-13 (America/Chicago)

This file records only API usage patterns that have been executed and verified in this repo.

## Providers
- Polygon (market bars)
- Marketaux (news)
- FRED (macro time series)

## 1) Polygon (Proven)

### Endpoint
- `GET https://api.polygon.io/v2/aggs/ticker/{SYMBOL}/range/1/day/{FROM_DATE}/{TO_DATE}`

### Proven query parameters
- `adjusted=true`
- `sort=asc` (pipeline ingest) and `sort=desc` (freshness probes)
- `limit=50000` (pipeline ingest) and `limit=1|3` (freshness probes)
- `apiKey=<POLYGON_API_KEY>`

### Proven response handling
- Accept payload status: `OK`, `DELAYED`
- Read bars from `results[]`
- Upsert key: `(symbol, timeframe, ts_utc, correction_seq)`

### Proven recency interpretation
- Daily bars may appear with UTC timestamps that map to prior/local day boundaries.
- Recency checks must use market-date logic (America/New_York), not naive local date assumptions.

## 2) Marketaux (Proven)

### Endpoint
- `GET https://api.marketaux.com/v1/news/all`

### Proven query parameters
- `api_token=<MARKETAUX_API_KEY>`
- `symbols=CSV`
- `filter_entities=true`
- `language=en`
- `limit=<int>`
- Optional: `published_after=<normalized format>` (see below)

### Critical format rule (proven)
- `published_after` accepted:
  - `YYYY-MM-DDTHH:MM:SS` (no timezone suffix)
  - `YYYY-MM-DD`
- `published_after` rejected:
  - `YYYY-MM-DDTHH:MM:SS+00:00`
  - `YYYY-MM-DDTHH:MM:SSZ`

### Pipeline behavior (proven)
- Ingest records every request in `news_pull_usage` with `response_status`.
- Freshness gates must use successful pull recency (`response_status='success'`), not raw pull attempts.
- Upsert keys:
  - `news_events`: `(provider, provider_news_id)`
  - `news_symbols`: `(provider, provider_news_id, symbol)`

## 3) FRED (Proven)

### Endpoint
- `GET https://api.stlouisfed.org/fred/series/observations`

### Proven query parameters
- `series_id=<id>`
- `api_key=<FRED_API_KEY>`
- `file_type=json`
- `observation_start=YYYY-MM-DD`
- `observation_end=YYYY-MM-DD`
- `sort_order=asc` (pipeline) and `sort_order=desc` + `limit=1` (freshness probe)

### Proven series in pipeline
- `DGS2`, `DGS10`, `DFF`, `VIXCLS`

### Proven storage
- Upsert key: `(series_id, ts_utc)`

## 4) Proven env vars used by pipelines

- `POLYGON_API_KEY`
- `MARKETAUX_API_KEY`
- `FRED_API_KEY`
- `TRADLY_NEWS_PUBLISHED_AFTER_UTC` (normalized before request)
- `TRADLY_MARKET_FROM_DATE`
- `TRADLY_MARKET_TO_DATE`
- `TRADLY_MACRO_FROM_DATE`
- `TRADLY_MACRO_TO_DATE`

## 5) Not marked proven here

These are intentionally excluded from "proven" until explicitly validated in a run:
- Any provider parameter not listed above.
- Alternative Marketaux date formats beyond those listed.
- Intraday Polygon bars for production model path (current model path is daily bars).
- Any undocumented endpoints.

## 6) Operational evidence locations

- Request/run logs:
  - `data/journal/cycle_runs.jsonl`
- News request usage table:
  - `news_pull_usage` in `data/tradly.duckdb`
- Pipeline implementations:
  - `src/tradly/pipeline/ingest_market_bars.py`
  - `src/tradly/pipeline/ingest_news_budgeted.py`
  - `src/tradly/pipeline/seed_macro_fred.py`
