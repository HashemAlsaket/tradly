# Data Contract V1

## 1) Purpose
This contract defines required data domains, schemas, SLAs, lineage/provenance rules, and audit gates for the long-only manual-execution trading system.

## 2) Core Principles
- Portfolio-aware decisions are mandatory; broker state is a blocking dependency.
- All numeric outputs must be code-computed (deterministic functions/services).
- LLM/VLM/STT are orchestration and interpretation layers, not numeric calculators.
- Every dataset and computed field must be traceable to source + version + timestamp.

## 3) Data Domains
- Broker State (Robinhood mirror)
- Market Data (OHLCV + reference)
- News and Events
- Sector/Benchmark Context
- Derived Features and Scores
- Audit and Run Metadata

## 4) Freshness SLAs (Blocking Rules)
Freshness is measured as `now - as_of_timestamp`.

- Broker State:
  - hard SLA: <= 120 seconds before recommendation cycle
  - action on breach: block recommendations (`not action-safe`)
- Intraday Price Snapshot (when market open):
  - hard SLA: <= 60 seconds for actionable dashboard state
  - action on breach: freeze action queue and flag stale
- Daily OHLCV:
  - hard SLA: updated by 07:00 America/Chicago on trading days
  - action on breach: no pre-market cycle recommendations
- News Feed:
  - hard SLA: <= 5 minutes for event-driven updates
  - action on breach: disable news-impact deltas, degrade confidence
- Sector/Benchmark Series:
  - hard SLA: <= 5 minutes intraday, <= 1 day end-of-day
  - action on breach: disable sector component in scorer

## 5) Canonical Schemas
All records must include:
- `record_id` (string UUID)
- `as_of_timestamp` (ISO-8601 UTC)
- `ingested_at` (ISO-8601 UTC)
- `source` (provider/system name)
- `source_ref` (upstream id/url/event id)
- `schema_version` (semver)

### 5.1 Broker Account Snapshot
- `account_id` (string)
- `equity` (number)
- `cash` (number)
- `buying_power` (number)
- `day_pnl` (number)
- `total_pnl` (number)
- `open_orders_count` (integer)
- `portfolio_drawdown_pct` (number)

### 5.2 Broker Position Snapshot
- `account_id` (string)
- `symbol` (string)
- `quantity` (number)
- `avg_cost` (number)
- `market_price` (number)
- `market_value` (number)
- `weight_pct` (number)
- `unrealized_pnl` (number)
- `realized_pnl` (number)
- `sector` (string)
- `is_borderline_universe` (boolean)

### 5.3 Market Bar (Daily/Intraday)
- `symbol` (string)
- `bar_interval` (enum: `1m`, `5m`, `15m`, `1d`)
- `bar_start` (ISO-8601 UTC)
- `bar_end` (ISO-8601 UTC)
- `open` (number)
- `high` (number)
- `low` (number)
- `close` (number)
- `volume` (number)
- `adjusted_close` (number, nullable intraday)
- `corporate_action_factor` (number)

### 5.4 News Event
- `news_id` (string)
- `published_at` (ISO-8601 UTC)
- `headline` (string)
- `source_name` (string)
- `source_quality_tier` (enum: `high`, `medium`, `low`)
- `symbols` (array[string])
- `sector_tags` (array[string])
- `llm_summary` (string)
- `llm_event_type` (string)
- `llm_sentiment_label` (enum: `positive`, `neutral`, `negative`)
- `llm_sentiment_confidence` (number 0-1)

### 5.5 Sector Context Snapshot
- `sector` (string)
- `benchmark_symbol` (string)
- `relative_strength_20d` (number)
- `relative_strength_60d` (number)
- `volatility_20d` (number)
- `breadth_proxy` (number)

### 5.6 Recommendation Record
- `run_id` (string)
- `symbol` (string)
- `action` (enum: `Strong Buy`, `Buy`, `Watch`, `Trim`, `Exit`)
- `confidence_score` (number 0-100)
- `buy_til` (string condition/date, nullable)
- `hold_til` (string condition/date, nullable)
- `sell_by` (string condition/date, nullable)
- `sell_at` (number price level, nullable)
- `invalidation_price` (number, nullable)
- `invalidation_narrative` (string)
- `why_now` (string)
- `portfolio_impact` (string)

### 5.7 Score Components and Provenance
- `technical_score` (number)
- `news_score` (number)
- `sector_score` (number)
- `vol_regime_score` (number)
- `liquidity_penalty` (number)
- `computed_by` (string module/function + version)
- `input_snapshot` (string hash/version id)
- `calculated_at` (ISO-8601 UTC)
- `method` (string)
- `is_qualitative_non_numeric` (boolean)
- `qualitative_label` (string, nullable)

## 6) Computation Boundaries (Hard)
- Allowed for numeric fields: typed code modules and deterministic functions.
- Not allowed for numeric fields: LLM/VLM free-text estimates.
- Allowed for LLM/VLM/STT:
  - summarizing news/transcripts
  - semantic classification
  - structured extraction into schema-constrained outputs
  - natural-language rationale generation
- Numeric score assembly must execute in code after extraction, with explicit weights.

## 7) AI-First Parsing and Understanding Policy
- Decision-critical parsing must use model-based extraction or schema-aware parsers.
- Regex and hard-coded string matching are disallowed in decision-critical understanding paths.
- Fallback behavior if model extraction confidence is low:
  - mark affected fields uncertain
  - exclude uncertain component from score and re-normalize in code
  - surface warning in dashboard and audit log

## 8) Storage Layout (V1)
- `data/raw/` immutable source payloads (partitioned by domain/date)
- `data/clean/` normalized canonical tables
- `data/features/` derived features and component scores
- `data/runs/` run-level recommendations and audit outputs
- `data/journal/` execution journal entries (manual fills + notes)

## 9) Data Quality Checks
Required checks per ingest cycle:
- schema validity
- null/NaN thresholds
- duplicate key detection
- timestamp monotonicity and market-session consistency
- symbol universe membership
- extreme outlier detection against rolling ranges

On failure:
- tag check as failed with severity
- quarantine affected partition
- block downstream decision run if severity is blocking

## 10) Audit Agent Interfaces
Each run must emit three audit artifacts keyed by `run_id`.

### 10.1 Data Audit Output
- freshness status by domain
- completeness percentages
- failed checks and severities
- decision: `pass` or `fail`

### 10.2 Calculation Audit Output
- verification that all numeric fields map to code computation records
- provenance completeness rate
- any qualitative fields incorrectly used as numeric
- decision: `pass` or `fail`

### 10.3 Decision Audit Output
- risk rule compliance (position cap, sector cap, drawdown gate)
- horizon qualifier completeness
- portfolio-awareness checks (cash impact and overlap)
- decision: `pass` or `fail`

If any output is `fail`, run status must be `not action-safe`.

## 11) Run Contract
Each recommendation cycle creates a `run_manifest`:
- `run_id`
- `run_type` (`pre_market`, `post_close`, `event_driven`)
- `started_at`, `completed_at`
- `broker_state_freshness_seconds`
- `input_snapshots` (array)
- `audit_status` (aggregate + per-agent)
- `action_safe` (boolean)
- `degraded_components` (array)

## 12) Dashboard Data Contract
Mobile dashboard must consume API/views exposing:
- `top_picks_view`
- `portfolio_mirror_view`
- `risk_status_view`
- `action_queue_view`
- `news_impact_view`
- `model_health_view`

Each view must include:
- `as_of_timestamp`
- `freshness_seconds`
- `data_quality_state` (`ok`, `degraded`, `blocked`)

## 13) Security and Access
- Store broker credentials/secrets in environment secret manager only.
- No secrets in repo, logs, or dashboard payloads.
- Access logs for broker-state sync and recommendation access are required.

## 14) Versioning and Change Control
- Any schema change requires `schema_version` bump and migration note.
- Breaking changes require compatibility window or transformer adapters.
- Trading spec and data contract versions must be linked in run manifest.

## 15) Implementation Order (Immediate)
1. Implement canonical schemas and validators.
2. Implement broker-state sync + freshness gate.
3. Implement market/news/sector ingestion to `raw` then `clean`.
4. Implement scorer pipeline with strict provenance fields.
5. Implement three audit agents and run-manifest gate.
6. Expose dashboard views for mobile UI.
