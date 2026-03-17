# Refresh Reliability Contract V1

This document defines the exact reliability contract for tradly refresh.

The intended outcome is:

1. every required source advances from the state already persisted in the database
2. every required source reaches the correct catch-up target for the run
3. the runner proves that advancement happened
4. the run fails if required advancement did not happen

This document is intentionally about refresh correctness and operational truth.

It does not define:

1. recommendation ranking logic
2. dashboard layout
3. model feature design

## Core Principle

Refresh must not be treated as:

1. "data looks fresh enough"
2. "the subprocess returned zero"
3. "the latest row is recent-ish"

Refresh must be treated as:

1. source-by-source progress from persisted state
2. source-by-source verification against the run timestamp
3. fail-hard behavior when required progress is missing

## Source Classes

Every source must be assigned one refresh mode.

Allowed refresh modes:

1. `incremental_cursor`
2. `date_completeness`
3. `current_state_only`
4. `derived_artifact`

Meaning:

`incremental_cursor`

1. the source must advance from the last stored cursor/watermark to the run target

`date_completeness`

1. the source must contain all expected dates through the run target

`current_state_only`

1. the source does not preserve every intermediate state
2. refresh proves only that current state was updated recently enough

`derived_artifact`

1. the source is generated from upstream inputs
2. refresh proves it was regenerated after upstream dependencies were refreshed

## Canonical Refresh Outcome

A refresh run is valid only if all of the following are true:

1. the runner start time and end time are recorded
2. each required source has a declared refresh mode
3. each required source has a declared catch-up target
4. each required source either:
   1. reaches its target
   2. is explicitly allowed to remain warning-grade
5. the persisted postflight freshness snapshot is newly written by the run
6. the persisted postflight snapshot timestamp is close to wall clock
7. the persisted cycle log entry is newly written by the run

Subprocess success alone is not sufficient.

## Source Contracts

### 1. `market_bars` `1d`

Refresh mode:

1. `date_completeness`

Unit of progress:

1. trading date

Catch-up target:

1. expected latest completed trading date for the run
2. this uses market-calendar semantics, not naive local date

Proof of advancement:

1. `MAX(calendar_date)` or equivalent market-date projection reaches the expected latest completed trading date
2. daily bar rows for that date exist for the required scope

Allowed stale mode:

1. weekend or holiday closed-calendar state

Block condition:

1. required trading date is missing on a trading day

Notes:

1. daily does not need minute-level cursor semantics
2. daily correctness is date completeness

### 2. `market_bars` `1m`

Refresh mode:

1. `incremental_cursor`

Unit of progress:

1. minute timestamp per symbol

Catch-up target:

1. latest required minute for the run, based on market session state
2. during active session or premarket, this target must be close to run wall clock
3. off-hours may permit a non-required state

Proof of advancement:

1. per-symbol or scoped watermark advances
2. DB `MAX(ts_utc)` advances to the required minute bound
3. post-run age against wall clock is within policy

Allowed stale mode:

1. closed-calendar or non-required session states only

Block condition:

1. active-session or premarket run where `1m` remains behind the required bound after refresh

Current gap:

1. the current system uses date-window refetch plus upsert
2. it does not yet use a durable minute watermark

Required implementation direction:

1. fetch from last stored minute or watermark to the run-time minute bound
2. verify advancement after write

### 3. `market_snapshots`

Refresh mode:

1. `current_state_only`

Unit of progress:

1. latest `as_of_utc` per symbol

Catch-up target:

1. current snapshot state refreshed within allowed age

Proof of advancement:

1. required scope symbols have fresh `as_of_utc`
2. post-run snapshot age is within session policy

Allowed stale mode:

1. non-required session states

Block condition:

1. active-session or premarket run where snapshot age exceeds allowed bound

Important non-rule:

1. snapshots are not historical backfill in phase 1
2. the system must not pretend snapshot refresh closes every missed intermediate state
3. a fresh snapshot row proves only current-state recency at refresh time
4. a fresh snapshot row must never be interpreted as "all snapshot states between last run and this run were captured"

Future option:

1. if true snapshot history is required later, it needs a different storage and cursor contract

### 4. `news_events`

Refresh mode:

1. `incremental_cursor`

Unit of progress:

1. provider publication timestamp or provider event cursor

Catch-up target:

1. no required gap between the last stored news point and the run timestamp inside the supported news horizon

Proof of advancement:

1. ingestion watermark advances
2. latest ingested news publication timestamp advances when new news exists
3. request usage proves the pull succeeded

Allowed stale mode:

1. low-news periods with no new provider events

Block condition:

1. required news pull path fails
2. or provider watermark does not advance when fresh provider data exists

Current gap:

1. current news refresh uses bounded recent-window pulls
2. it is not yet a true durable provider cursor

Canonical Marketaux progression rule for this repo:

1. until provider event-id cursoring is intentionally added, the canonical progress unit is provider publication timestamp
2. watermark advancement is based on the highest successfully ingested `published_at_utc`
3. refresh must close the required gap between the stored publication-timestamp watermark and the run target inside the supported news horizon

### 5. `news_interpretations`

Refresh mode:

1. `derived_artifact`

Unit of progress:

1. interpreted news event coverage for the required recent horizon

Catch-up target:

1. all required recent news events are interpreted within policy

Proof of advancement:

1. pending uninterpreted count in the required window reaches acceptable level
2. latest interpretation timestamp advances after new ingest

Allowed stale mode:

1. none for the required interpretation horizon

Block condition:

1. required recent news remains uninterpreted after refresh

### 6. `macro_points`

Refresh mode:

1. `date_completeness`

Unit of progress:

1. observation date per series

Catch-up target:

1. all expected observation dates are present through the run date
2. publication-lag-aware series may have a justified expected latest date earlier than wall-clock date

Proof of advancement:

1. per-series completeness map
2. latest observation date by series
3. expected-date comparison by series

Allowed stale mode:

1. warning-grade lag only when publication schedule justifies it

Block condition:

1. per-series expected date missing beyond policy

Current gap:

1. current macro handling is age-in-days based
2. it is not yet expressed as per-series expected-date completeness

### 7. Model Artifacts

Applies to:

1. `market_regime_v1`
2. `sector_movement_v1`
3. `symbol_movement_v1`
4. `ensemble_v1`
5. `recommendation_v1`
6. `recommendation_review_v1`
7. scorecard artifacts

Refresh mode:

1. `derived_artifact`

Unit of progress:

1. artifact run timestamp plus dependency alignment

Catch-up target:

1. artifact must be regenerated after all required upstream source refreshes for the run

Proof of advancement:

1. artifact timestamp is newer than required upstream inputs
2. dependency chain is ordered correctly
3. artifact alignment audit passes

Allowed stale mode:

1. none for canonical live outputs

Block condition:

1. downstream artifact is older than required upstream source refresh

### 8. Ops / Freshness Snapshot

Refresh mode:

1. `derived_artifact`

Unit of progress:

1. persisted snapshot timestamp for the run

Catch-up target:

1. persisted ops snapshot must reflect the current run, not an earlier run

Proof of advancement:

1. freshness snapshot write time advances during the run
2. cycle log entry advances during the run
3. freshness snapshot timestamp is close to wall clock end time

Allowed stale mode:

1. none for canonical ops reporting after a successful run

Block condition:

1. a run claims success but does not produce a newly current freshness snapshot

## Required Watermark Infrastructure

The refresh system should move toward a canonical table:

1. `pipeline_watermarks`

Proposed fields:

1. `source_name`
2. `scope_key`
3. `watermark_ts_utc`
4. `watermark_meta_json`
5. `updated_at_utc`

Purpose:

1. separate "latest row observed in DB" from "last verified catch-up cursor"
2. make incremental refresh explicit and auditable

Canonical `scope_key` rules:

1. `market_bars_1m` -> symbol, for example `AAPL`
2. `market_bars_1d` -> singleton scope key `__market_bars_1d__`
3. `market_snapshots` current-state refresh -> symbol, for example `AAPL`
4. `news_events` -> provider name, for example `marketaux`
5. `news_interpretations` -> singleton scope key `__news_interpretations__`
6. `macro_points` -> `series_id`, for example `DGS10`
7. model artifacts -> artifact name, for example `market_regime_v1`
8. ops freshness snapshot -> singleton scope key `__freshness_snapshot__`

### Where Explicit Watermarks Are Required

Must use explicit watermark semantics:

1. `market_bars_1m`
2. `news_events`
3. any future historical snapshot capture

### Where DB Max May Be Sufficient Initially

May rely on DB max plus completeness checks:

1. `market_bars_1d`
2. `macro_points`
3. derived model artifacts

## Runner Contract

`run_cycle_reliable` must become fail-hard on stale success.

It must verify after refresh:

1. persisted freshness snapshot advanced during the run
2. persisted cycle log advanced during the run
3. each required source met its contract for the current session state
4. required derived artifacts were regenerated after upstream refresh

Canonical postflight wall-clock tolerance thresholds:

1. persisted cycle log end timestamp must be within `120` seconds of wall clock at verification time
2. persisted freshness snapshot audit timestamp must be within `120` seconds of wall clock at verification time
3. during required intraday sessions, `market_bars_1m` and `market_snapshots` freshness must still satisfy their stricter source-level age policies in addition to the `120` second postflight write tolerance

It must fail with explicit reasons such as:

1. `postflight_snapshot_not_advanced`
2. `postflight_cycle_log_not_advanced`
3. `market_bars_1m_not_caught_up`
4. `news_events_not_caught_up`
5. `macro_points_expected_date_missing`
6. `artifact_dependency_stale`

## Session Policy

### Weekend / Holiday

Required:

1. daily bars current for calendar
2. news and interpretations within policy

Warning-allowed:

1. `1m`
2. snapshots

### Premarket / Market Hours / After Hours

Required:

1. `1m` within intraday policy
2. snapshots within intraday policy
3. daily bars current for calendar
4. news refresh within policy
5. required interpretations within policy

Warning-allowed only if contract says so:

1. macro series with justified publication lag

## Immediate Implementation Order

1. create this contract as the canonical rule set
2. add fail-hard postflight verification to `run_cycle_reliable`
3. add `pipeline_watermarks`
4. convert `market_bars_1m` to true incremental catch-up
5. convert news to cursor or durable watermark semantics
6. convert macro to expected-date completeness by series
7. keep snapshots explicitly current-state-only until historical capture is intentionally designed

## Non-Rules

The following are explicitly not acceptable end states:

1. subprocess zero exit code without verified source advancement
2. "fresh enough" as a substitute for source-by-source catch-up
3. treating snapshots as historically backfilled when only current state was refreshed
4. passing a run while the persisted ops snapshot still represents an earlier wall-clock state

## Success Condition

This contract is satisfied only when a successful refresh run means:

1. the database advanced where it was required to advance
2. the derived artifacts advanced where they were required to advance
3. the persisted ops surface advanced where it was required to advance
4. the system can prove all three facts after the run
