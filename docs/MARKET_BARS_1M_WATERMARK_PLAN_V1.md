# Market Bars 1M Watermark Plan V1

This document defines the exact implementation plan for converting `market_bars` `1m` refresh from freshness-threshold behavior to watermark-driven incremental catch-up.

This step is intentionally limited to:

1. `market_bars` `1m`
2. `pipeline_watermarks` minimum viable support for `1m`
3. preflight and runner verification needed to support `1m`

It does not implement:

1. news watermarks
2. macro completeness redesign
3. snapshot history

## Goal

When a refresh run starts, `1m` ingestion must:

1. determine the last verified persisted `1m` progress point
2. fetch from that point forward
3. upsert missing bars
4. advance the verified watermark
5. prove that post-run `1m` state reached the required session target

The system must stop relying on:

1. broad recent date-window refetch as the primary correctness mechanism
2. max-age-only freshness checks as a substitute for progress

## Current Behavior

Current `1m` ingest:

1. [ingest_market_bars_1m.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/pipeline/ingest_market_bars_1m.py)
2. fetches by `TRADLY_MARKET_1M_FROM_DATE` / `TRADLY_MARKET_1M_TO_DATE`
3. defaults to a broad lookback window
4. upserts rows into `market_bars`

Current preflight:

1. [preflight_catchup.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/ops/preflight_catchup.py)
2. checks `MAX(ts_utc)` age against session policy
3. derives a rough backfill date window
4. does not track explicit verified progress

## Required DB Change

Add a new table:

1. `pipeline_watermarks`

Minimum columns for phase 1:

1. `source_name TEXT NOT NULL`
2. `scope_key TEXT NOT NULL`
3. `watermark_ts_utc TIMESTAMP`
4. `watermark_meta_json TEXT`
5. `updated_at_utc TIMESTAMP NOT NULL`

Primary key:

1. `(source_name, scope_key)`

Phase-1 `1m` usage:

1. `source_name = 'market_bars_1m'`
2. `scope_key = <symbol>`
3. `watermark_ts_utc = latest fully ingested minute for that symbol`

## Watermark Semantics

The `1m` watermark means:

1. all provider minute bars for that symbol up to `watermark_ts_utc` have been ingested for the current contract
2. the watermark only advances after successful DB upsert for that symbol
3. a missing watermark means the symbol needs bootstrap catch-up

The watermark does not mean:

1. every minute beyond the provider’s available range exists
2. the market is currently open

## Catch-Up Target

The per-run `1m` catch-up target is session-aware.

### During `market_hours`

Target:

1. latest completed minute at or just before run wall clock

### During `pre_market`

Target:

1. latest completed extended-hours minute at or just before run wall clock

### During `after_hours`

Target:

1. latest completed after-hours minute at or just before run wall clock

### During `weekend` / `holiday`

Target:

1. no required intraday catch-up

## Fetch Window Rule

For each symbol:

1. load the persisted watermark
2. if watermark exists:
   1. fetch from watermark market date minus one guard day
   2. fetch through current run local date
3. if watermark is missing:
   1. bootstrap from configured lookback window

Rationale:

1. Massive aggregates are date-bounded
2. we still need a date request window
3. but the watermark defines correctness
4. the guard day protects session-boundary overlap and provider timestamp quirks

## Post-Fetch Filtering Rule

After fetching bars for a symbol:

1. normalize all rows
2. discard rows with `ts_utc <= watermark_ts_utc` for that symbol
3. keep only rows strictly newer than the watermark
4. upsert remaining rows

This makes the date-window request safe while preserving cursor semantics.

## Watermark Advancement Rule

For each symbol:

1. if newer rows were successfully upserted:
   1. set watermark to `MAX(ts_utc)` of rows upserted for that symbol
2. if no newer rows were returned:
   1. keep existing watermark unchanged
3. if symbol ingestion fails:
   1. do not advance the watermark

## Verification Rule

`1m` refresh is valid only if all required scoped symbols satisfy one of:

1. watermark advanced during the run
2. existing watermark was already sufficiently current for the session target

Verification outputs must include:

1. symbols with advanced watermark count
2. symbols already current count
3. symbols still stale count
4. minimum and maximum watermark across scoped symbols
5. required target timestamp bound for the run

## Preflight Changes

[preflight_catchup.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/ops/preflight_catchup.py) must change from:

1. `MAX(ts_utc)` age-only logic

To:

1. scoped watermark inspection
2. scoped watermark lag classification
3. invocation of `ingest_market_bars_1m` when required symbols are behind target

Phase-1 simplified lag policy:

1. if any required symbol watermark is behind the session target beyond policy, `market_bars_1m` is stale

## Ingest Changes

[ingest_market_bars_1m.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/pipeline/ingest_market_bars_1m.py) must gain:

1. watermark load helper
2. watermark update helper
3. per-symbol fetch window builder from watermark
4. post-fetch filter using watermark timestamp
5. artifact fields for watermark progress

Artifact additions:

1. `watermark_mode = true`
2. per-symbol:
   1. previous watermark
   2. fetched row count
   3. filtered new row count
   4. advanced watermark
   5. advancement status

## Runner Impact

The hardened runner in [run_cycle_reliable.py](/Users/hashemalsaket/Desktop/workspace/tradly/scripts/ops/run_cycle_reliable.py) remains unchanged for this step.

But this step must make runner success more meaningful because:

1. preflight will no longer rely only on broad age thresholds
2. `1m` advancement will be explicitly verifiable

## Failure Modes

New explicit failure reasons should be possible:

1. `market_bars_1m_watermark_missing`
2. `market_bars_1m_watermark_not_advanced`
3. `market_bars_1m_post_upsert_stale`
4. `market_bars_1m_symbol_ingest_failed`

## Tests Required

### Unit tests

1. watermark bootstrap path when no watermark exists
2. filtering out rows at or before watermark
3. advancing watermark after successful upsert
4. not advancing watermark on symbol failure
5. preflight classification using watermark state

### Integration-style tests

1. symbol already current does not falsely advance watermark
2. stale symbol advances to a newer minute
3. mixed symbol states still classify correctly

## Audit Requirements After Implementation

1. verify `pipeline_watermarks` rows exist for all scoped `1m` symbols
2. verify `MAX(ts_utc)` and watermarks are coherent
3. verify a live market-hours run advances current watermarks correctly
4. verify preflight no longer relies only on broad date-window semantics for `1m`

## Exact Next Files To Edit

1. [db/schema_v1.sql](/Users/hashemalsaket/Desktop/workspace/tradly/db/schema_v1.sql)
2. [ingest_market_bars_1m.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/pipeline/ingest_market_bars_1m.py)
3. [preflight_catchup.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/ops/preflight_catchup.py)
4. [test_ingest_market_bars_1m.py](/Users/hashemalsaket/Desktop/workspace/tradly/tests/test_ingest_market_bars_1m.py)
5. [test_preflight_catchup.py](/Users/hashemalsaket/Desktop/workspace/tradly/tests/test_preflight_catchup.py)

## Non-Goals

This step does not:

1. redesign snapshot semantics
2. redesign macro freshness
3. redesign news progression
4. remove `TRADLY_SKIP_PREFLIGHT_CATCHUP`

## Success Condition

This step succeeds only when `1m` refresh means:

1. the system knows where each symbol last left off
2. the system fetches from that point forward
3. the system proves whether each symbol advanced
4. the system can fail when required `1m` progress did not occur
