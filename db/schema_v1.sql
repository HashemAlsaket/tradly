-- tradly DuckDB Schema V1
-- Lean, point-in-time safe, and audit-first.

-- 1) Instruments universe
CREATE TABLE IF NOT EXISTS instruments (
  symbol TEXT PRIMARY KEY,
  asset_type TEXT NOT NULL,              -- stock, etf
  sector TEXT,
  industry TEXT,
  halal_flag TEXT NOT NULL,              -- allowed, review, blocked
  active BOOLEAN NOT NULL DEFAULT TRUE,
  as_of_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL
);

-- 2) Portfolio snapshots (account-level mirror)
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
  snapshot_id TEXT PRIMARY KEY,          -- UUID
  account_id TEXT NOT NULL,
  account_mode TEXT NOT NULL,            -- CASH, MARGIN
  as_of_utc TIMESTAMP NOT NULL,
  equity DOUBLE,
  cash DOUBLE,
  buying_power DOUBLE,
  settled_cash DOUBLE,
  unsettled_cash DOUBLE,
  drawdown_pct DOUBLE,
  source TEXT NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL
);

-- 3) Position snapshots (per symbol at snapshot time)
CREATE TABLE IF NOT EXISTS position_snapshots (
  snapshot_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  as_of_utc TIMESTAMP NOT NULL,
  qty DOUBLE,
  avg_cost DOUBLE,
  market_price DOUBLE,
  market_value DOUBLE,
  weight_pct DOUBLE,
  unrealized_pnl DOUBLE,
  realized_pnl DOUBLE,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (snapshot_id, symbol),
  FOREIGN KEY (snapshot_id) REFERENCES portfolio_snapshots(snapshot_id),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);

-- 4) Market bars (keep lean: 1m + 1d in one table)
-- correction_seq increments when provider corrects same bar.
CREATE TABLE IF NOT EXISTS market_bars (
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,               -- 1m, 1d
  ts_utc TIMESTAMP NOT NULL,             -- bar start
  calendar_date DATE,
  day_of_week INTEGER,
  day_name TEXT,
  is_weekend BOOLEAN,
  is_market_holiday BOOLEAN,
  is_trading_day BOOLEAN,
  market_calendar_state TEXT,
  last_cash_session_date DATE,
  as_of_utc TIMESTAMP NOT NULL,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume DOUBLE,
  vwap DOUBLE,
  data_status TEXT,                      -- DELAYED, REALTIME
  source TEXT NOT NULL,
  correction_seq INTEGER NOT NULL DEFAULT 0,
  ingested_at_utc TIMESTAMP NOT NULL,
  updated_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (symbol, timeframe, ts_utc, correction_seq),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);

-- 5) Trading calendar dimension
CREATE TABLE IF NOT EXISTS market_calendar (
  calendar_date DATE PRIMARY KEY,
  day_of_week INTEGER NOT NULL,          -- 0=Sunday ... 6=Saturday
  day_name TEXT NOT NULL,
  is_weekend BOOLEAN NOT NULL,
  is_market_holiday BOOLEAN NOT NULL,
  is_trading_day BOOLEAN NOT NULL,
  market_calendar_state TEXT NOT NULL,   -- trading_day, weekend, holiday
  last_cash_session_date DATE NOT NULL,
  next_cash_session_date DATE NOT NULL,
  as_of_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL
);

-- 6) Run manifests (one row per decision cycle)
CREATE TABLE IF NOT EXISTS run_manifests (
  run_id TEXT PRIMARY KEY,
  run_type TEXT NOT NULL,                -- intraday, hourly, daily
  started_at_utc TIMESTAMP NOT NULL,
  completed_at_utc TIMESTAMP,
  state TEXT NOT NULL,                   -- research_only, action_safe, blocked
  action_safe BOOLEAN NOT NULL,
  broker_freshness_sec INTEGER,
  market_freshness_sec INTEGER,
  block_reason_code TEXT,
  warning_codes TEXT,                    -- comma-separated for lean v1
  config_version TEXT NOT NULL,
  created_at_utc TIMESTAMP NOT NULL
);

-- 7) Audit results (data/calculation/decision)
CREATE TABLE IF NOT EXISTS audit_results (
  run_id TEXT NOT NULL,
  audit_type TEXT NOT NULL,              -- data, calculation, decision
  decision TEXT NOT NULL,                -- pass, fail
  detail TEXT,
  as_of_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (run_id, audit_type),
  FOREIGN KEY (run_id) REFERENCES run_manifests(run_id)
);

-- 8) Recommendations
CREATE TABLE IF NOT EXISTS recommendations (
  run_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL,                  -- Strong Buy, Buy, Watch, Trim, Exit
  confidence_score DOUBLE,
  horizon_type TEXT,                     -- intraday/hourly/daily/weekly/multi_week
  buy_til TEXT,
  hold_til TEXT,
  sell_by TEXT,
  sell_at DOUBLE,
  invalidation_price DOUBLE,
  invalidation_narrative TEXT,
  why_now TEXT,
  portfolio_impact TEXT,
  computed_by TEXT NOT NULL,
  input_snapshot TEXT NOT NULL,
  method TEXT NOT NULL,
  as_of_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (run_id, symbol),
  FOREIGN KEY (run_id) REFERENCES run_manifests(run_id),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);

-- 9) Manual execution journal (user executed in Robinhood)
CREATE TABLE IF NOT EXISTS execution_journal (
  journal_id TEXT PRIMARY KEY,
  run_id TEXT,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  intended_action TEXT,
  executed_action TEXT,
  executed_price DOUBLE,
  executed_qty DOUBLE,
  executed_at_utc TIMESTAMP,
  notes TEXT,
  as_of_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL,
  FOREIGN KEY (run_id) REFERENCES run_manifests(run_id),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);

-- 10) News events (provider-scoped identity)
CREATE TABLE IF NOT EXISTS news_events (
  provider TEXT NOT NULL,
  provider_news_id TEXT NOT NULL,
  published_at_utc TIMESTAMP NOT NULL,
  calendar_date DATE,
  day_of_week INTEGER,
  day_name TEXT,
  is_weekend BOOLEAN,
  is_market_holiday BOOLEAN,
  is_trading_day BOOLEAN,
  market_calendar_state TEXT,
  last_cash_session_date DATE,
  as_of_utc TIMESTAMP NOT NULL,
  source_name TEXT NOT NULL,
  source_quality TEXT,
  headline TEXT NOT NULL,
  summary TEXT,
  url TEXT,
  sentiment_label TEXT,
  sentiment_score DOUBLE,
  extraction_confidence DOUBLE,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (provider, provider_news_id)
);

-- 11) News to symbol mapping
CREATE TABLE IF NOT EXISTS news_symbols (
  provider TEXT NOT NULL,
  provider_news_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  relevance_score DOUBLE,
  as_of_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (provider, provider_news_id, symbol),
  FOREIGN KEY (provider, provider_news_id) REFERENCES news_events(provider, provider_news_id),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);

-- 12) Macro data points
CREATE TABLE IF NOT EXISTS macro_points (
  series_id TEXT NOT NULL,
  ts_utc TIMESTAMP NOT NULL,
  calendar_date DATE,
  day_of_week INTEGER,
  day_name TEXT,
  is_weekend BOOLEAN,
  is_market_holiday BOOLEAN,
  is_trading_day BOOLEAN,
  market_calendar_state TEXT,
  last_cash_session_date DATE,
  as_of_utc TIMESTAMP NOT NULL,
  value DOUBLE,
  source TEXT NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (series_id, ts_utc)
);

-- 13) Feature store (narrow + versioned)
CREATE TABLE IF NOT EXISTS feature_values (
  run_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  feature_name TEXT NOT NULL,
  feature_version TEXT NOT NULL,
  feature_value DOUBLE NOT NULL,
  computed_by TEXT NOT NULL,
  input_snapshot TEXT NOT NULL,
  as_of_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (run_id, symbol, feature_name, feature_version),
  FOREIGN KEY (run_id) REFERENCES run_manifests(run_id),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);

-- 14) News pull usage log (request budgeting)
CREATE TABLE IF NOT EXISTS news_pull_usage (
  usage_id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  bucket TEXT NOT NULL,
  symbols_csv TEXT NOT NULL,
  request_count INTEGER NOT NULL,
  request_date_utc DATE NOT NULL,
  day_of_week INTEGER,
  day_name TEXT,
  is_weekend BOOLEAN,
  is_market_holiday BOOLEAN,
  is_trading_day BOOLEAN,
  market_calendar_state TEXT,
  last_cash_session_date DATE,
  response_status TEXT NOT NULL,         -- success, http_error, limit_reached
  detail TEXT,
  new_events_upserted INTEGER NOT NULL DEFAULT 0,
  new_symbol_links_upserted INTEGER NOT NULL DEFAULT 0,
  created_at_utc TIMESTAMP NOT NULL
);

-- 15) LLM news interpretations (interpretation only; no calculations)
CREATE TABLE IF NOT EXISTS news_interpretations (
  provider TEXT NOT NULL,
  provider_news_id TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  bucket TEXT NOT NULL,                  -- macro, sector, symbol, asia, ignore
  impact_scope TEXT NOT NULL,            -- macro, rates, energy, semis, usd, risk_sentiment, multiple
  impact_direction TEXT NOT NULL,        -- risk_on, risk_off, bullish_semis, bearish_semis, mixed, unclear
  impact_horizon TEXT NOT NULL,          -- intraday, 1to3d, 1to2w
  calendar_date DATE,
  day_of_week INTEGER,
  day_name TEXT,
  is_weekend BOOLEAN,
  is_market_holiday BOOLEAN,
  is_trading_day BOOLEAN,
  market_calendar_state TEXT,
  last_cash_session_date DATE,
  relevance_symbols_json TEXT NOT NULL,  -- JSON list
  thesis_tags_json TEXT NOT NULL,        -- JSON list
  market_impact_note TEXT NOT NULL,
  confidence_label TEXT NOT NULL,        -- low, medium, high
  based_on_provided_evidence BOOLEAN NOT NULL,
  calculation_performed BOOLEAN NOT NULL,
  interpreted_at_utc TIMESTAMP NOT NULL,
  ingested_at_utc TIMESTAMP NOT NULL,
  PRIMARY KEY (provider, provider_news_id, model, prompt_version),
  FOREIGN KEY (provider, provider_news_id) REFERENCES news_events(provider, provider_news_id)
);
