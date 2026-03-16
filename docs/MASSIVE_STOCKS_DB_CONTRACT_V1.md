# Massive Stocks DB Contract V1

This document defines the exact database contract for integrating Massive Stocks Advanced into tradly.

This is the phase-1 contract for:

1. historical `1d` backfill
2. historical `1m` backfill
3. current-state stock snapshots

This document is intentionally limited to database design and provider-to-database field mapping.

It does not define:

1. refresh cadence
2. model logic
3. dashboard behavior
4. websocket streaming
5. options data

Those come later.

## Phase-1 Goals

The phase-1 market-data layer must support:

1. one provider contract for Massive historical bars
2. one provider contract for Massive stock snapshots
3. backfill and live refresh using the same storage model
4. calendar-aware downstream use without a parallel architecture

The phase-1 market-data layer must not require a redesign of:

1. `market_bars`
2. trading-calendar stamping
3. downstream daily-bar models

## Provider Identity

Provider name written to the database:

1. `source = 'massive'`

Provider credentials:

1. primary env var: `MASSIVE_API_KEY`
2. legacy fallback during migration only: `POLYGON_API_KEY`

The application should move toward `MASSIVE_API_KEY` as the canonical setting.

## Provider Semantics Contract

This section defines the exact meaning of:

1. `source`
2. `data_status`
3. `correction_seq`

These semantics must be shared across:

1. `market_bars` `1d`
2. `market_bars` `1m`
3. `market_snapshots`

No pipeline may invent a different meaning locally.

### Source

`source` identifies the provider that supplied the persisted market-data row.

Phase-1 allowed values for new stock-market rows:

1. `massive`

Legacy values may still exist during migration:

1. `polygon`

Phase-1 rules:

1. all newly written Massive-backed bars use `source = 'massive'`
2. all newly written Massive-backed snapshots use `source = 'massive'`
3. `source` must describe the upstream provider, not the pipeline step name
4. `source` must not be overloaded with latency state

### Data Status

`data_status` expresses the effective recency/evidence quality of the stored market-data row.

It must not encode:

1. provider plan names
2. pipeline phase names
3. inferred model confidence

Phase-1 allowed values for rows that may be read by the current live runtime:

1. `DELAYED`
2. `REALTIME`

Reserved future value for non-live-compatible archival/storage work:

1. `HISTORICAL`

#### Data Status Meaning

`DELAYED`

1. written by a refresh path using delayed market data
2. represents current or recent market state but not real-time evidence quality
3. valid for slower horizons and latency-aware modeling only

`REALTIME`

1. written by a refresh path using real-time market data
2. represents current-session evidence suitable for real-time-aware freshness logic
3. does not automatically imply the row is perfect, only that its provider status is real-time

#### Data Status By Table And Mode

For `market_bars` `1d`:

1. historical backfill -> `DELAYED` in phase 1 so existing live readers remain compatible
2. same-day or latest-session refresh from real-time-capable provider path -> `REALTIME` only if that row truly reflects the real-time endpoint semantics we are using
3. same-day or latest-session refresh from delayed path -> `DELAYED`

For `market_bars` `1m`:

1. historical backfill -> `DELAYED` in phase 1 so existing live readers remain compatible
2. live refresh from Stocks Advanced real-time endpoint -> `REALTIME`
3. live refresh from delayed fallback path -> `DELAYED`

For `market_snapshots`:

1. live snapshot refresh from Stocks Advanced -> `REALTIME`
2. delayed snapshot fallback path -> `DELAYED`
3. synthetic historical snapshot generation is not part of phase 1

#### Data Status Non-Rules

The following are explicitly forbidden:

1. marking historical bars `REALTIME`
2. inferring `REALTIME` from purchase plan alone without checking the actual endpoint path being used

### Correction Sequence

`correction_seq` identifies provider-level corrections for the same logical market-data observation.

Phase-1 allowed value:

1. `0`

Phase-1 rules:

1. all Massive-backed `1d` rows write `correction_seq = 0`
2. all Massive-backed `1m` rows write `correction_seq = 0`
3. `market_snapshots` does not use `correction_seq`
4. phase 1 does not synthesize correction numbers locally

Rationale:

1. the current schema already supports future correction-aware ingestion
2. phase 1 should not invent correction identity that the provider did not explicitly supply in the ingestion contract we are implementing

### Provider Semantics Verification

The market-data integration is not valid unless all of the following are true:

1. all new stock-market rows written by the new ingestion path use `source = 'massive'`
2. all phase-1 bar and snapshot rows written for live-runtime compatibility use `REALTIME` or `DELAYED`
3. live refresh rows use `REALTIME` or `DELAYED` according to the actual endpoint path
4. all Massive-backed bar rows in phase 1 use `correction_seq = 0`
5. no write path overloads `source` or `data_status` with pipeline-specific meanings

## Runtime Scope

Phase-1 Massive market-data ingestion will target the existing runtime market-data scope manifest:

1. `data/manual/universe_runtime_scopes.json`
2. `scopes.market_data_symbols`

Current live scope size at audit time:

1. `70` symbols

Phase-1 backfill must not ingest every row in `instruments`.

It must ingest only the active runtime market-data scope.

### Live Scope Audit At Planning Time

Current live scope facts:

1. `market_data_symbols = 70`
2. all `70` scope symbols exist in `instruments`
3. scope composition:
   1. active stocks = `39`
   2. inactive stocks = `10`
   3. active ETFs = `5`
   4. inactive ETFs = `16`

This means the market-data scope is broader than the active model universe by design.

### Phase-1 Backfill Scope Decision

Phase-1 backfill will preserve the current runtime market-data scope exactly.

Canonical backfill scope:

1. `data/manual/universe_runtime_scopes.json`
2. `scopes.market_data_symbols`

Phase-1 backfill does not narrow this to:

1. only `active_symbols`
2. only `model_symbols`
3. only direct-news symbols

### Why Phase-1 Preserves The Existing Scope

The current `market_data_symbols` scope already represents the intended market-observation layer for:

1. regime context
2. sector and ETF context
3. direct symbol modeling
4. broader market confirmation

If we narrow it prematurely during the provider migration, we introduce two risks:

1. changing provider and changing scope at the same time
2. breaking regime/sector context coverage without realizing it

Phase-1 therefore changes provider and frequency, not scope meaning.

### Asset-Type Scope Rules

Phase-1 backfill eligibility by asset type:

1. `stock` symbols in `market_data_symbols` are eligible
2. `etf` symbols in `market_data_symbols` are eligible
3. `index` symbols are not part of the phase-1 Stocks Advanced backfill scope unless they are separately supported through a future Indices integration

Practical implication:

1. ETF context rows such as `SPY`, `QQQ`, `IWM`, `DIA`, `IEF`, and similar market/sector proxies remain in scope
2. stock symbols remain in scope for direct modeling
3. the legacy index-style symbol `I:VIX` is not part of `market_data_symbols` and is therefore out of scope for this phase

### Active vs Inactive Instrument Rules

Phase-1 backfill preserves the manifest scope even when a scoped instrument is currently marked inactive in `instruments`.

Reason:

1. the current scope manifest is already the stronger runtime decision layer
2. inactive status in `instruments` does not necessarily mean “remove all historical market context”

Phase-1 therefore does not auto-filter `market_data_symbols` by `active = true`.

### Implementation Guardrails For Scope

Backfill must fail fast if any of the following are true:

1. scope manifest file is missing
2. `scopes.market_data_symbols` is missing
3. `scopes.market_data_symbols` is empty
4. any scoped symbol is missing from `instruments`

Backfill must log:

1. scoped symbol count
2. stock count
3. ETF count
4. symbols with no provider rows
5. symbols that fail ingest

### Phase-1 Non-Goals For Scope

Phase-1 does not:

1. redesign the universe manifest
2. prune the market-data scope
3. expand to every instrument in the DB
4. add index-data scope via the stock provider path

### Scope Verification Checklist

The scope decision is only correctly implemented if:

1. the number of eligible backfill symbols equals the current `market_data_symbols` scope size unless explicit exclusions are logged
2. ETF proxies remain present in the backfill set
3. direct modeled stocks remain present in the backfill set
4. no symbol outside `market_data_symbols` is backfilled by default

## Existing Table Reuse

Phase-1 reuses:

1. `market_bars` for `1d` and `1m`

Phase-1 adds:

1. `market_snapshots`

Phase-1 does not add:

1. raw `market_quotes`
2. raw `market_trades`
3. raw websocket event tables

## Market Bars Contract

The existing `market_bars` table remains the canonical storage layer for provider bars.

Relevant columns:

1. `symbol`
2. `timeframe`
3. `ts_utc`
4. `as_of_utc`
5. `open`
6. `high`
7. `low`
8. `close`
9. `volume`
10. `vwap`
11. `data_status`
12. `source`
13. `correction_seq`
14. `ingested_at_utc`
15. `updated_at_utc`
16. calendar fields already stamped later

### Supported Timeframes

Phase-1 Massive bars written to `market_bars`:

1. `1d`
2. `1m`

No other bar timeframes will be written in phase 1.

### Massive Aggregate Mapping

Massive aggregate fields map into `market_bars` as follows:

1. provider ticker -> `symbol`
2. requested bar granularity -> `timeframe`
3. aggregate timestamp -> `ts_utc`
4. aggregate timestamp -> `as_of_utc`
5. `o` -> `open`
6. `h` -> `high`
7. `l` -> `low`
8. `c` -> `close`
9. `v` -> `volume`
10. `vw` -> `vwap`
11. provider recency classification -> `data_status`
12. constant -> `source = 'massive'`
13. default -> `correction_seq = 0`

### Timestamp Rules

For `market_bars`:

1. `ts_utc` is the provider bar start timestamp normalized to UTC
2. `as_of_utc` equals the same bar timestamp in phase 1
3. `ingested_at_utc` is the system write time
4. `updated_at_utc` is the last upsert time

This matches the existing daily-bar contract and avoids a second bar semantics layer.

### Daily Bars

For `1d` bars:

1. `timeframe = '1d'`
2. `ts_utc` is the provider daily bar timestamp normalized to UTC
3. backfill uses adjusted bars
4. sort order must be ascending before write

#### Daily Backfill Endpoint Contract

Phase-1 daily backfill uses Massive stock aggregates with this exact request shape:

1. ticker-scoped aggregate endpoint
2. multiplier = `1`
3. timespan = `day`
4. `adjusted = true`
5. `sort = asc`
6. high enough `limit` to cover the requested window without partial fetches when possible

Required request inputs:

1. `symbol`
2. `from` date
3. `to` date
4. Massive API key

Required request behavior:

1. one request series per scoped symbol
2. ascending-order materialization before database write
3. deterministic date window
4. explicit failure if the provider returns malformed or missing result arrays

Phase-1 daily backfill does not use:

1. snapshots
2. trades
3. quotes
4. websocket streams

#### Daily Backfill Window Rules

Phase-1 daily backfill window is defined by explicit environment/config inputs:

1. `TRADLY_MARKET_FROM_DATE`
2. `TRADLY_MARKET_TO_DATE`

Fallback behavior:

1. if unset, use the existing rolling lookback default already used by the current daily ingest path

Validation rules:

1. `from <= to`
2. both dates must be parseable
3. empty windows are invalid

#### Daily Aggregate Response Mapping

Each Massive daily aggregate result row must map as follows:

1. result timestamp -> `ts_utc`
2. result timestamp -> `as_of_utc`
3. `o` -> `open`
4. `h` -> `high`
5. `l` -> `low`
6. `c` -> `close`
7. `v` -> `volume`
8. `vw` -> `vwap`
9. constant -> `timeframe = '1d'`
10. constant -> `source = 'massive'`
11. constant during phase-1 historical backfill -> `data_status = 'DELAYED'`
12. constant during historical backfill -> `correction_seq = 0`

#### Daily Timestamp Semantics

Daily timestamps must preserve the current tradly meaning:

1. bar timestamp is stored as the provider daily-bar timestamp normalized to UTC
2. no synthetic market-close timestamp translation layer is introduced in phase 1

This is important because the current live daily rows already use the provider timestamp convention and downstream code assumes that contract.

#### Daily Write Semantics

Daily backfill writes must be idempotent at the row level.

Required behavior:

1. upsert into `market_bars`
2. key by `(symbol, timeframe, ts_utc, correction_seq)`
3. update mutable fields on conflict:
   1. `as_of_utc`
   2. OHLCV
   3. `vwap`
   4. `data_status`
   5. `source`
   6. `updated_at_utc`

Phase-1 daily backfill must not create duplicate daily bars for the same:

1. symbol
2. timeframe
3. timestamp
4. correction sequence

#### Daily Migration Policy

Because `market_bars` is keyed by `(symbol, timeframe, ts_utc, correction_seq)` and does not include `source`, overlapping Massive and Polygon daily bars cannot coexist in the same table for the same logical row.

Phase-1 migration mode is therefore:

1. replacement mode only for overlapping daily history in `market_bars`

Required migration behavior:

1. stage and validate Massive daily rows outside the canonical overlapping write path before cutover, or validate on a controlled subset/window first
2. once validated, replace overlapping Polygon-backed daily rows in `market_bars` with Massive-backed daily rows
3. do not claim same-table coexistence for overlapping daily rows

This means the implementation must support a staged cutover, but not same-key coexistence.

#### Daily Verification Checklist

Daily backfill is not complete until all of the following are checked:

1. row counts by symbol are plausible
2. distinct scoped symbol coverage is complete
3. min/max daily timestamps match the requested window
4. sample OHLCV rows match the provider response for chosen symbols
5. `source = 'massive'` rows exist as expected
6. `data_status = 'DELAYED'` for phase-1 historical daily backfill rows
7. calendar stamping succeeds after backfill
8. downstream daily-model readers can still read `timeframe = '1d'` without code changes

Daily bars remain the canonical source for:

1. swing and position modeling
2. scorecard entry/exit logic
3. seed coverage audits

### Minute Bars

For `1m` bars:

1. `timeframe = '1m'`
2. `ts_utc` is the provider minute-bar start timestamp normalized to UTC
3. backfill uses adjusted bars if provider supports that consistently for intraday aggregates
4. sort order must be ascending before write

#### Minute Backfill Endpoint Contract

Phase-1 minute backfill uses Massive stock aggregates with this exact request shape:

1. ticker-scoped aggregate endpoint
2. multiplier = `1`
3. timespan = `minute`
4. `adjusted = true` if supported consistently for intraday aggregates under the chosen endpoint contract
5. `sort = asc`
6. explicit pagination handling if the requested window exceeds one response page

Required request inputs:

1. `symbol`
2. `from` datetime or market-date boundary
3. `to` datetime or market-date boundary
4. Massive API key

Required request behavior:

1. one request series per scoped symbol
2. ascending-order materialization before database write
3. deterministic time window
4. explicit failure if the provider returns malformed or missing result arrays

#### Minute Backfill Window Strategy

Phase-1 minute backfill must be bounded and practical.

It must not attempt “all available minute history.”

Phase-1 target:

1. backfill a recent tactical window only
2. size that window to what the current tactical system can actually use

Default policy:

1. backfill the last `30` calendar days of `1m` bars for the scoped symbol set

Rationale:

1. enough to support tactical feature development and validation
2. enough to cover several recent market regimes and sessions
3. small enough to backfill and refresh without turning phase 1 into a storage project

Configuration contract:

1. `TRADLY_MARKET_1M_FROM_DATE` may override the default lower bound
2. `TRADLY_MARKET_1M_TO_DATE` may override the default upper bound
3. if unset, use:
   1. `to = current local market date`
   2. `from = to - 30 calendar days`

Validation rules:

1. `from <= to`
2. both bounds must be parseable
3. empty windows are invalid

#### Minute Aggregate Response Mapping

Each Massive minute aggregate result row must map as follows:

1. result timestamp -> `ts_utc`
2. result timestamp -> `as_of_utc`
3. `o` -> `open`
4. `h` -> `high`
5. `l` -> `low`
6. `c` -> `close`
7. `v` -> `volume`
8. `vw` -> `vwap`
9. constant -> `timeframe = '1m'`
10. constant during historical minute backfill -> `source = 'massive'`
11. constant during phase-1 historical minute backfill -> `data_status = 'DELAYED'`
12. constant during historical minute backfill -> `correction_seq = 0`

#### Minute Timestamp Semantics

Minute timestamps must preserve provider bar-start semantics.

Phase-1 rules:

1. `ts_utc` is the provider minute-bar start normalized to UTC
2. no synthetic “session label” timestamp is introduced
3. `as_of_utc` equals the same minute-bar timestamp in phase 1

This keeps minute and daily bar contracts parallel.

#### Minute Write Semantics

Minute backfill writes must be idempotent at the row level.

Required behavior:

1. upsert into `market_bars`
2. key by `(symbol, timeframe, ts_utc, correction_seq)`
3. update mutable fields on conflict:
   1. `as_of_utc`
   2. OHLCV
   3. `vwap`
   4. `data_status`
   5. `source`
   6. `updated_at_utc`

Phase-1 minute backfill must not create duplicate minute bars for the same:

1. symbol
2. timeframe
3. timestamp
4. correction sequence

#### Minute Validation Rules

Minute bars must be rejected before write if any of the following are true:

1. missing timestamp
2. missing close
3. negative volume
4. non-positive close
5. malformed OHLC relationship
6. symbol not in runtime market-data scope

Additional minute-specific rule:

1. phase-1 historical minute backfill may accept zero-volume bars only if the provider emits a structurally valid minute row and we confirm the endpoint uses zero-volume minutes legitimately

This rule must be made explicit in implementation rather than assumed.

#### Minute Practical Limits

Phase-1 minute backfill must remain bounded operationally.

Required practical limits:

1. only scoped symbols are eligible
2. only the configured recent tactical window is eligible
3. pagination must be explicit and auditable
4. implementation must log:
   1. rows written
   2. symbols written
   3. window start/end
   4. symbols with no rows
   5. symbols with partial failures

Phase-1 minute backfill does not yet require:

1. raw quote replay
2. raw trade replay
3. websocket replay
4. full multi-year minute history

#### Minute Verification Checklist

Minute backfill is not complete until all of the following are checked:

1. row counts by symbol are plausible for the requested window
2. distinct scoped symbol coverage is complete or explicitly explained
3. min/max minute timestamps match the requested window
4. sample minute OHLCV rows match the provider response for chosen symbols
5. `source = 'massive'` rows exist as expected
6. `data_status = 'DELAYED'` for phase-1 historical minute backfill rows
7. calendar stamping succeeds after backfill
8. no duplicate-key inflation appears in `market_bars`
9. the minute backfill volume is operationally acceptable for refresh follow-on work

Minute bars become the canonical source for:

1. intraday freshness
2. open/premarket state
3. short-horizon confirmation logic

Minute bars do not replace daily bars.

### Bar Write Semantics

Upsert key remains:

1. `(symbol, timeframe, ts_utc, correction_seq)`

Phase-1 correction handling:

1. default `correction_seq = 0`
2. if Massive later exposes corrected aggregate sequencing we can extend this
3. phase 1 does not invent synthetic correction numbers

### Data Status Semantics

Phase-1 `data_status` must express evidence quality, not plan names.

Allowed values for Massive-backed bars:

1. `REALTIME`
2. `DELAYED`

Phase-1 policy:

1. historical backfill writes `DELAYED` in phase 1 for live-runtime compatibility
2. same-session or latest-session refresh writes:
   1. `REALTIME` when Massive data is real-time for that endpoint and plan
   2. `DELAYED` only if an endpoint or fallback path is delayed

This is stricter than the current Polygon-only contract and is the right long-term shape.

### Required Validation Rules For Bars

Bars must be rejected before write if any of the following are true:

1. missing timestamp
2. missing close
3. missing volume when provider should supply it
4. non-positive close
5. negative volume
6. malformed OHLC relationship
7. symbol not in runtime market-data scope

Bars may be accepted with nullable values only for:

1. `open`
2. `high`
3. `low`
4. `vwap`

if the provider response genuinely omits them and we explicitly decide to tolerate that endpoint shape.

## Market Snapshots Contract

Phase-1 adds a new table:

1. `market_snapshots`

Purpose:

1. store current-state market evidence that is not a historical bar
2. support premarket/open readiness
3. avoid abusing `market_bars` to represent point-in-time quotes

### Proposed Table Shape

Required fields:

1. `symbol TEXT NOT NULL`
2. `as_of_utc TIMESTAMP NOT NULL`
3. `last_trade_price DOUBLE`
4. `last_trade_size DOUBLE`
5. `last_trade_ts_utc TIMESTAMP`
6. `bid_price DOUBLE`
7. `bid_size DOUBLE`
8. `ask_price DOUBLE`
9. `ask_size DOUBLE`
10. `last_quote_ts_utc TIMESTAMP`
11. `session_open DOUBLE`
12. `session_high DOUBLE`
13. `session_low DOUBLE`
14. `session_close DOUBLE`
15. `session_volume DOUBLE`
16. `prev_close DOUBLE`
17. `change DOUBLE`
18. `change_pct DOUBLE`
19. `day_vwap DOUBLE`
20. `market_status TEXT`
21. `data_status TEXT`
22. `source TEXT NOT NULL`
23. `ingested_at_utc TIMESTAMP NOT NULL`
24. `updated_at_utc TIMESTAMP NOT NULL`

Recommended primary key:

1. `(symbol, as_of_utc)`

This preserves an auditable snapshot trail without pretending snapshots are bars.

### Snapshot Table DDL Contract

Phase-1 snapshot table should be created with this logical shape:

1. `symbol TEXT NOT NULL`
2. `as_of_utc TIMESTAMP NOT NULL`
3. `calendar_date DATE`
4. `day_of_week INTEGER`
5. `day_name TEXT`
6. `is_weekend BOOLEAN`
7. `is_market_holiday BOOLEAN`
8. `is_trading_day BOOLEAN`
9. `market_calendar_state TEXT`
10. `last_cash_session_date DATE`
11. `last_trade_price DOUBLE`
12. `last_trade_size DOUBLE`
13. `last_trade_ts_utc TIMESTAMP`
14. `bid_price DOUBLE`
15. `bid_size DOUBLE`
16. `ask_price DOUBLE`
17. `ask_size DOUBLE`
18. `last_quote_ts_utc TIMESTAMP`
19. `session_open DOUBLE`
20. `session_high DOUBLE`
21. `session_low DOUBLE`
22. `session_close DOUBLE`
23. `session_volume DOUBLE`
24. `prev_close DOUBLE`
25. `change DOUBLE`
26. `change_pct DOUBLE`
27. `day_vwap DOUBLE`
28. `market_status TEXT`
29. `data_status TEXT`
30. `source TEXT NOT NULL`
31. `ingested_at_utc TIMESTAMP NOT NULL`
32. `updated_at_utc TIMESTAMP NOT NULL`

Primary key:

1. `(symbol, as_of_utc)`

Foreign key:

1. `symbol -> instruments(symbol)`

### Snapshot Endpoint Contract

Phase-1 snapshot ingestion uses Massive stock snapshot responses as the canonical current-state object.

Preferred input order:

1. stock snapshot endpoint as the primary source
2. last trade endpoint only if the snapshot payload does not provide the required last-trade fields cleanly
3. last quote endpoint only if the snapshot payload does not provide the required quote fields cleanly

Phase-1 default:

1. do not fetch separate last trade or last quote data if the snapshot response already provides those fields

This keeps phase 1 simple and avoids multiplying API calls unnecessarily.

### Snapshot Mapping

Phase-1 snapshot rows should be built from Massive stock snapshot responses and, if needed, supplemented by:

1. last trade endpoint
2. last quote endpoint

Mapping rules:

1. snapshot timestamp -> `as_of_utc`
2. session/day open -> `session_open`
3. session/day high -> `session_high`
4. session/day low -> `session_low`
5. session/day close or last price -> `session_close`
6. session/day volume -> `session_volume`
7. previous close -> `prev_close`
8. change -> `change`
9. percent change -> `change_pct`
10. snapshot VWAP if present -> `day_vwap`
11. last trade price -> `last_trade_price`
12. last trade size -> `last_trade_size`
13. last trade timestamp -> `last_trade_ts_utc`
14. last quote bid -> `bid_price`
15. last quote bid size -> `bid_size`
16. last quote ask -> `ask_price`
17. last quote ask size -> `ask_size`
18. last quote timestamp -> `last_quote_ts_utc`
19. provider/session state -> `market_status`
20. provider recency -> `data_status`
21. constant -> `source = 'massive'`

### Snapshot Field Semantics

Snapshot rows represent point-in-time market state, not a bar series.

Phase-1 semantics:

1. `as_of_utc` is the provider snapshot timestamp or the best available current-state timestamp supplied by Massive
2. `session_*` fields describe the current trading session/day state known at snapshot time
3. `prev_close` is the previous regular-session close used for change calculations
4. `change` and `change_pct` preserve provider semantics rather than being recomputed locally in phase 1
5. `market_status` stores provider/session context in normalized tradly form when possible

### Snapshot Data Status Semantics

Allowed values for Massive-backed snapshots:

1. `REALTIME`
2. `DELAYED`

Phase-1 policy:

1. snapshot rows written during active refresh use:
   1. `REALTIME` when the endpoint is real-time under Stocks Advanced
   2. `DELAYED` only if the endpoint or fallback path is delayed
2. historical backfill does not create synthetic snapshot history in phase 1

### Snapshot Validation Rules

Snapshot rows must be rejected before write if any of the following are true:

1. missing symbol
2. missing `as_of_utc`
3. symbol not in runtime market-data scope
4. malformed numeric fields that cannot be parsed
5. negative sizes or volumes where the provider semantics require non-negative values
6. bid price greater than ask price when both are present

Snapshot rows may be accepted with nullable values for:

1. quote fields
2. trade fields
3. session fields

only when the provider legitimately omits them for that symbol or session state.

### Snapshot Write Semantics

Snapshot writes must be append-safe and auditable.

Required behavior:

1. upsert into `market_snapshots`
2. key by `(symbol, as_of_utc)`
3. update mutable fields on conflict:
   1. trade fields
   2. quote fields
   3. session fields
   4. `market_status`
   5. `data_status`
   6. `source`
   7. `updated_at_utc`

Phase-1 does not require:

1. deduping snapshots into arbitrary fixed intervals
2. converting snapshots into bars
3. retaining every quote/trade event between snapshots

### Snapshot Calendar Stamping

`market_snapshots` must use the same trading-calendar enrichment pattern as the rest of the system.

Stamping rules:

1. derive calendar context from `as_of_utc`
2. populate:
   1. `calendar_date`
   2. `day_of_week`
   3. `day_name`
   4. `is_weekend`
   5. `is_market_holiday`
   6. `is_trading_day`
   7. `market_calendar_state`
   8. `last_cash_session_date`

### Snapshot Non-Goals In Phase 1

Phase-1 `market_snapshots` does not attempt to be:

1. a best-bid-offer history table
2. a full last-trade history table
3. a market microstructure archive
4. a websocket event sink

It is a compact current-state layer for:

1. tactical freshness
2. dashboard truth
3. open/premarket state
4. intraday model confirmation features

### Snapshot Verification Checklist

Snapshot ingestion is not complete until all of the following are checked:

1. rows exist for the scoped symbol set
2. distinct symbol coverage is complete or explicitly explained
3. `as_of_utc` values are plausible and recent for the run
4. sample rows match provider payloads for chosen symbols
5. `source = 'massive'` rows exist as expected
6. `data_status` matches endpoint reality
7. calendar stamping succeeds
8. quote/trade fields are populated when the provider supplies them

### Snapshot Non-Goals

Phase-1 snapshots are not:

1. a replacement for minute bars
2. a raw quote tape
3. a raw trade tape

They are a point-in-time state layer for tactical freshness and dashboard truth.

## Calendar Stamping Contract

Existing calendar fields remain the standard:

1. `calendar_date`
2. `day_of_week`
3. `day_name`
4. `is_weekend`
5. `is_market_holiday`
6. `is_trading_day`
7. `market_calendar_state`
8. `last_cash_session_date`

Phase-1 rule:

1. `market_bars` continues to be stamped after write
2. `market_snapshots` must receive the same calendar context

The stamping logic should use `as_of_utc` for snapshots and `ts_utc` for bars.

## Backfill Windows

Phase-1 historical windows:

1. `1d` backfill:
   1. fill the scoped symbol set across the target historical window
   2. preserve existing backfill behavior unless explicitly changed
2. `1m` backfill:
   1. do not attempt unlimited history in phase 1
   2. choose a bounded window sized to tactical model needs and provider practicality

The exact `1m` default backfill window for phase 1 is `30` calendar days unless explicitly overridden by config.

## Non-Destructive Migration Rules

Phase-1 migration must not:

1. delete existing `1d` bars before validation
2. rewrite daily timestamps into a different semantic model
3. mix provider names in ambiguous form

Phase-1 migration may:

1. replace the current overlapping `polygon` `1d` path with `massive` `1d` after verification

The preferred operational target is:

1. one canonical provider for stock bars going forward: `massive`

## Required Verification After Backfill

The backfill phase must verify:

1. row counts by timeframe
2. distinct symbol coverage by timeframe
3. min/max timestamps by timeframe
4. sample-symbol bar correctness
5. `source` distribution
6. `data_status` distribution
7. duplicate-key integrity
8. calendar stamping completeness

## Phase-1 Decision

The exact DB contract for Massive phase 1 is:

1. reuse `market_bars` for `1d` and `1m`
2. add `market_snapshots` for point-in-time market state
3. use `source = 'massive'`
4. use `DELAYED` and `REALTIME` in phase-1 live-compatible writes
5. keep correction handling simple with `correction_seq = 0` unless the provider gives explicit correction identity
6. keep calendar stamping as the shared post-write enrichment layer

This is the contract the backfill implementation must follow.
