# tradly

Initial scaffold for a long-only, manual-execution trading intelligence platform.

## Current scope
- Trading and data specs in:
  - `TRADING_SPEC_V1.md`
  - `DATA_CONTRACT_V1.md`
- Modeling implementation workflow:
  - `docs/IMPLEMENTATION_AUDIT_WORKFLOW.md`
- Typed schema and service scaffold under `src/tradly/`
- Broker-state freshness gate implemented in `src/tradly/services/freshness_gate.py`
- OpenAI-only model suite config in `src/tradly/config/model_suite.py`
- Basic tests in `tests/`

## Quick start
1. Create a virtual env and install deps:
   - `python -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -e .`
2. Run tests:
   - `PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'`

## OpenAI model suite env vars
- `OPENAI_API_KEY`
- `OPENAI_LLM_MODEL`
- `OPENAI_VLM_MODEL`
- `OPENAI_STT_MODEL`

## Data provider env vars
- `MASSIVE_API_KEY`
- `MARKETAUX_API_KEY`
- `FRED_API_KEY`
- `MACRO_LOOKBACK_DAYS` (optional, default `730`)
- `NEWS_INTERPRET_LOOKBACK_DAYS` (optional, default `3`; set `30` for seed catch-up)

## DuckDB init
1. Install DuckDB Python package:
   - `pip install duckdb`
2. Initialize local database from schema:
   - `python scripts/setup/init_db.py`
3. Load starter universe into `instruments`:
   - `python scripts/setup/load_universe.py`
4. Seed historical news context (one-time bootstrap):
   - `python scripts/pipeline/seed_news.py`
5. Seed macro history from FRED:
   - `python scripts/pipeline/seed_macro.py`
6. Ingest strict daily market bars from Massive:
   - `python scripts/pipeline/ingest_market_bars.py`
   - includes strict context symbols for regime modeling: `SPY`, `QQQ`, `VIXY` (VIX proxy), `TLT`, `IEF`, `SHY`
7. Run budgeted news ingestion (daily cap logic from watchlist config, currently 2500/day):
   - `python scripts/pipeline/ingest_news.py`
8. Run LLM news interpretation (classification + market impact note, interpretation-only):
   - `python scripts/pipeline/interpret_news.py`
9. Run market regime specialist:
   - `python scripts/pipeline/run_market_regime.py`
10. Run sector movement specialist:
   - `python scripts/pipeline/run_sector_movement.py`

## Canonical runners
- One-time bootstrap:
  - `python scripts/pipeline/bootstrap.py`
- Recurring cycle:
  - `python scripts/pipeline/cycle.py`
  - order: market bars -> news ingest -> news interpretation -> market regime -> sector movement
- Seed readiness audit:
  - `python scripts/pipeline/seed_audit.py`
  - exits non-zero on failing baseline checks

## Reliable Automation
- Canonical refresh entrypoint (recommended one command to trust):
  - `python scripts/ops/refresh_all.py`
- Preflight freshness/catch-up (on-demand):
  - `python scripts/ops/preflight_catchup.py`
- Reliable cycle runner (lock + preflight catch-up + retry + runtime freshness audit):
  - `python scripts/ops/run_cycle_reliable.py`
- Runtime freshness audit only:
  - `python scripts/ops/runtime_freshness_audit.py`

Operational meaning of the canonical refresh:
- checks DB freshness by source
- catches up stale sources where needed
- runs the main cycle
- runs postflight freshness audit
- fails non-zero if required data remains stale or the cycle fails

Recommended env knobs:
- `TRADLY_CYCLE_MAX_ATTEMPTS` (default `2`)
- `TRADLY_CYCLE_RETRY_SLEEP_SEC` (default `45`)
- `TRADLY_PREFLIGHT_NEWS_PULL_MAX_AGE_SEC` (default `3600`)
- `TRADLY_PREFLIGHT_MACRO_MAX_AGE_DAYS` (default `2`)
- `TRADLY_PREFLIGHT_INTERPRET_LOOKBACK_DAYS` (default `7`)
- `TRADLY_NEWS_MAX_AGE_MINUTES_MARKET` (default `45`)
- `TRADLY_NEWS_MAX_AGE_MINUTES_OFFHOURS` (default `240`)
- `TRADLY_NEWS_MIN_SUCCESS_PULLS_MARKET` (default `1`)
- `TRADLY_NEWS_MIN_SUCCESS_PULLS_OFFHOURS` (default `1`)
- `TRADLY_NEWS_RUN_MAX_REQUESTS` (default `60`, per-run safety ceiling)
- `TRADLY_NEWS_EXPECTED_DAILY_BUDGET` (optional warning check against watchlist-configured budget)
- `TRADLY_INTERP_MAX_AGE_MINUTES_MARKET` (default `60`)
- `TRADLY_INTERP_MAX_AGE_MINUTES_OFFHOURS` (default `240`)
- `TRADLY_MARKET_FROM_DATE` / `TRADLY_MARKET_TO_DATE` (optional preflight window override for market bars)
- `TRADLY_MACRO_FROM_DATE` / `TRADLY_MACRO_TO_DATE` (optional preflight window override for macro seed)
- `TRADLY_NEWS_PUBLISHED_AFTER_UTC` (optional preflight news catch-up floor)
- `NEWS_SEED_LOOKBACK_DAYS` (optional override for historical Marketaux seed lookback)
- `NEWS_SEED_MAX_PAGES_PER_BUCKET` (optional override for historical Marketaux seed depth)
- `NEWS_SEED_BUCKETS` (optional comma-separated bucket filter for historical Marketaux seeding)
- `NEWS_SEED_REQUEST_CAP` (optional total request cap for historical Marketaux seeding)

Cron example (America/Chicago machine timezone):
- Market hours cadence (every 15 min, Mon-Fri 8:30-15:59 CT):
  - `*/15 8-15 * * 1-5 cd /Users/hashemalsaket/Desktop/workspace/tradly && /Users/hashemalsaket/Desktop/workspace/tradly/.venv/bin/python scripts/ops/run_cycle_reliable.py >> data/journal/cron.log 2>&1`
- Off-hours cadence (hourly):
  - `5 * * * * cd /Users/hashemalsaket/Desktop/workspace/tradly && /Users/hashemalsaket/Desktop/workspace/tradly/.venv/bin/python scripts/ops/run_cycle_reliable.py >> data/journal/cron.log 2>&1`

Depth-first historical news seed example:
- `NEWS_SEED_BUCKETS=core_semis,us_macro NEWS_SEED_LOOKBACK_DAYS=90 NEWS_SEED_MAX_PAGES_PER_BUCKET=40 NEWS_SEED_REQUEST_CAP=80 PYTHONPATH=src .venv/bin/python -m tradly.pipeline.seed_news_marketaux`

## Streamlit dashboard
1. Start dashboard:
   - `streamlit run dashboard/app.py`
2. Dashboard now reads:
   - `data/runs/<date>/market_regime_v1.json`
   - `data/runs/<date>/sector_movement_v1.json`
   - `data/journal/freshness_snapshot.json`
3. The dashboard is now a specialist-model status surface, not a legacy action sheet.
4. Open from your phone/browser on the same network:
   - `http://<your-machine-ip>:8501`
