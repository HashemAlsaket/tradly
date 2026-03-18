# Ops Runbook V1

## Purpose

This runbook is the canonical operating guide for running the refresh pipeline, checking health, and responding to failures.

It reflects the current production contracts:
- `market_bars` `1d`: date-complete
- `market_bars` `1m`: watermark-driven
- `market_snapshots`: latest-state only
- `macro_points`: refresh-time plus required-series coverage
- `news_events`: Marketaux bucket high-water marks

It also reflects the current session freshness policy:
- `premarket_strict`
- `market_hours_strict`
- `after_hours_relaxed`
- `closed_calendar_relaxed`

## Canonical Command

Run:

```bash
PYTHONPATH=src .venv/bin/python scripts/ops/run_cycle_reliable.py
```

This is the only canonical refresh entrypoint.

## What The Reliable Cycle Does

1. runs preflight catch-up
2. refreshes stale required sources
3. runs the full model chain
4. writes the persisted freshness snapshot
5. fails if postflight freshness output did not advance during the run

## Healthy Run Criteria

A healthy run means all of the following are true:

1. [cycle_runs.jsonl](/Users/hashemalsaket/Desktop/workspace/tradly/data/journal/cycle_runs.jsonl) latest entry has:
   - `status = PASS`
   - `cycle_rc = 0`
   - `freshness_rc = 0`

2. [freshness_snapshot.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/journal/freshness_snapshot.json) has:
   - `overall_status = PASS`
   - current `written_at_utc`
   - current `freshness.as_of_utc`

3. source freshness is acceptable for the current session:
   - `market_bars_1d` current for calendar
   - `market_bars_1m` fresh if intraday is strict, or warning-grade if after-hours relaxed
   - snapshots fresh if intraday is strict, or warning-grade if after-hours relaxed
   - news pull fresh
   - macro fresh under current contract

## Source Contracts

### `market_bars` `1d`

- expectation: latest completed market date is present
- check: `market_bar_status = current_for_calendar`
- acceptable when market is open: previous completed market date

### `market_bars` `1m`

- expectation: watermark coverage exists for the scoped symbol universe
- expectation: oldest symbol watermark is within freshness threshold
- check:
  - `intraday_bar_status = fresh`
  - `watermark_coverage = scoped universe`
- policy nuance:
  - strict failure in `premarket_strict` and `market_hours_strict`
  - warning-grade in `after_hours_relaxed`

### `market_snapshots`

- expectation: latest snapshot state is fresh
- this is not a historical replay source
- check: `snapshot_status = fresh`
- policy nuance:
  - strict failure in `premarket_strict` and `market_hours_strict`
  - warning-grade in `after_hours_relaxed`

### `news`

- expectation: news pulls are fresh
- expectation: configured Marketaux buckets advance by high-water mark
- check:
  - `news_pull_recency = PASS`
  - `success_news_pulls_today >= 1`
  - Marketaux bucket watermarks exist in `pipeline_watermarks`

### `macro`

- expectation: required series coverage is complete
- expectation: macro refresh `as_of_utc` is within freshness threshold
- observation dates may lag publication reality and are not treated as refresh failure by themselves
- check:
  - `macro_points = fresh` in preflight
  - `latest_macro_as_of_utc` present in freshness snapshot

## Primary Files To Check

1. [cycle_runs.jsonl](/Users/hashemalsaket/Desktop/workspace/tradly/data/journal/cycle_runs.jsonl)
2. [freshness_snapshot.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/journal/freshness_snapshot.json)
3. [tradly.duckdb](/Users/hashemalsaket/Desktop/workspace/tradly/data/tradly.duckdb)
4. [portfolio_snapshot_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/manual/portfolio_snapshot_v1.json)
5. `data/runs/<date>/portfolio_policy_v1.json`

## Fast Operator Checklist

After a run:

1. confirm latest cycle entry is `PASS`
2. confirm freshness snapshot `overall_status = PASS`
3. confirm:
   - `market_session_state`
   - `intraday_bar_status`
   - `snapshot_status`
   - `latest_news_pull_utc`
   - `latest_macro_as_of_utc`
4. if intraday session is active, confirm short-horizon readiness is true when expected
5. confirm `freshness_policy` matches the current session expectation
6. if portfolio policy is enabled, confirm:
   - `portfolio_policy_v1.json` exists for the latest run
   - `input_audit.status = ready`
   - `portfolio_mode` is plausible for the current market state
   - top `buy` / `add` / `trim` / `exit` rows are sensible

## Portfolio Policy Input

### Manual holdings snapshot

The portfolio engine uses a manual holdings input at:

- [portfolio_snapshot_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/manual/portfolio_snapshot_v1.json)

Maintain it manually with:
- current `as_of_utc`
- current `cash_available`
- current `net_liquidation_value`
- current `positions`
- current `open_orders`

Do not let the pipeline mutate this file. It is an operator-maintained input.

### Portfolio policy artifact

The portfolio engine writes:

- `data/runs/<date>/portfolio_policy_v1.json`

This artifact is decision support only. It does not place orders.

Expected checks:
- `portfolio_mode`
- `target_gross_long_exposure`
- `current_gross_long_exposure`
- `policy_violation_counts`
- per-symbol `action_recommendation`
- per-symbol `action_execution_state`

Off-hours nuance:
- after hours, non-hold actions may be intentionally marked `deferred_to_next_cash_session`
- urgent exits may still appear as `exit`

## Failure Handling

### Preflight failure

Symptoms:
- cycle exits before main pipeline
- latest cycle log `reason = preflight_catchup_failed`

Action:
1. inspect `preflight_stdout_tail`
2. inspect `preflight_stderr_tail`
3. determine which source failed

### `1m` stale or missing

Symptoms:
- `market_bars_1m` stale/missing in preflight
- `intraday_bar_status != fresh` during active session

Action:
1. rerun canonical cycle once
2. inspect `pipeline_watermarks` coverage for `market_bars_1m`
3. inspect provider/network errors if refresh does not advance

### snapshot stale

Symptoms:
- `market_snapshots` stale in preflight
- `snapshot_status != fresh` during active session

Action:
1. rerun canonical cycle once
2. inspect snapshot provider/network path

### news stale

Symptoms:
- `news_pull_recency = FAIL`
- no fresh `news_pull_usage`

Action:
1. rerun canonical cycle once
2. inspect `news_pull_usage`
3. inspect Marketaux bucket watermarks in `pipeline_watermarks`
4. inspect request budget / provider response failures

### macro stale

Symptoms:
- `macro_points != fresh`
- missing or old `latest_macro_as_of_utc`

Action:
1. rerun canonical cycle once
2. inspect preflight `macro_points` detail
3. inspect per-series macro coverage in `macro_points`

### lock held

Symptoms:
- cycle returns `cycle_skipped=lock_held`
- journal entry `status = SKIPPED_LOCK_HELD`

Action:
1. confirm another cycle is actually running
2. wait for the active run to finish
3. rerun only if no active run is progressing

## Warnings vs Blockers

### Acceptable warning

- snapshot source is latest-state only, not historical replay
- symbol-level `1m` watermarks are not all on the exact same minute, as long as oldest watermark is still fresh

### Blockers

- cycle log does not advance
- freshness snapshot does not advance
- postflight snapshot stale
- `market_bars_1m` stale during active session
- snapshots stale during active session
- news pull stale during active session
- macro coverage missing required series

## When To Run

Safe to run:
- on demand
- before market open
- during market hours
- after market close

The process is now reliable enough to be manually triggered whenever needed.

## Recommended Operating Cadence

Use this default cadence unless there is a specific reason to override it.

### Premarket

- run one full refresh before the cash open
- goal: have the board and artifacts ready for the session

### Market Hours

- run the canonical cycle every `15` minutes
- manual on-demand runs are still allowed between scheduled runs when needed

### After Close

- run one full refresh after the close
- goal: settle end-of-day state and prepare overnight context

### Manual Override

- manual refresh is always allowed
- use it for:
  - major headlines
  - suspicious stale state
  - pre-decision checks

## Alert Policy

### Critical

- latest cycle run fails during market hours
- `market_bars_1m` is stale during market hours
- `market_snapshots` are stale during market hours
- repeated `SKIPPED_LOCK_HELD` across multiple scheduled runs

### Warning

- `news_pull_usage` goes stale during market hours
- `macro_points` goes stale
- one `SKIPPED_LOCK_HELD`
- `market_bars_1m` or `market_snapshots` are stale during `after_hours_relaxed`

### Info

- preflight took corrective action but final state is still `PASS`
- no-op/healthy run with `preflight_actions = []`

## Rollout Plan

1. run this cadence manually for one live session
2. observe:
   - cycle duration
   - lock-held skips
   - provider request pressure
   - stale-source incidents
3. if stable, automate the same cadence without changing the policy
4. keep manual refresh available at all times

## Current Known Non-Blockers

1. backfill validation artifacts are still overwrite-prone, not immutable per run
2. cycle path still uses the `TRADLY_SKIP_PREFLIGHT_CATCHUP` seam internally
3. snapshots remain latest-state only by design

## Escalation Rule

If a source stays stale after one clean rerun of the canonical cycle:

1. treat it as a real incident
2. inspect provider/network path
3. do not assume the next blind rerun will fix it
