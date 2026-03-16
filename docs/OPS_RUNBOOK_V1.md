# Ops Runbook V1

## Purpose

This runbook is the canonical operating guide for running the refresh pipeline, checking health, and responding to failures.

It reflects the current production contracts:
- `market_bars` `1d`: date-complete
- `market_bars` `1m`: watermark-driven
- `market_snapshots`: latest-state only
- `macro_points`: refresh-time plus required-series coverage
- `news_events`: Marketaux bucket high-water marks

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
   - `market_bars_1m` fresh if intraday required
   - snapshots fresh if intraday required
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

### `market_snapshots`

- expectation: latest snapshot state is fresh
- this is not a historical replay source
- check: `snapshot_status = fresh`

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

## Current Known Non-Blockers

1. backfill validation artifacts are still overwrite-prone, not immutable per run
2. cycle path still uses the `TRADLY_SKIP_PREFLIGHT_CATCHUP` seam internally
3. snapshots remain latest-state only by design

## Escalation Rule

If a source stays stale after one clean rerun of the canonical cycle:

1. treat it as a real incident
2. inspect provider/network path
3. do not assume the next blind rerun will fix it
