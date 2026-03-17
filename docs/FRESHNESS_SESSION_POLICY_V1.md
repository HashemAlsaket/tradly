# Freshness Session Policy V1

## Purpose

This policy defines how refresh strictness changes by market session so preflight, runtime freshness, alerts, and ops cadence all use the same rules.

The goal is to eliminate ambiguous behavior where the system is technically outside core cash hours but still treated with market-hours strictness.

## Canonical Policy States

The system should derive one explicit policy state for every run:

- `premarket_strict`
- `market_hours_strict`
- `after_hours_relaxed`
- `closed_calendar_relaxed`

## Policy Mapping

### `premarket_strict`

Use when:
- session state is `pre_market`

Meaning:
- `1m` bars are required
- snapshots are required
- stale intraday should fail preflight
- stale snapshots should fail preflight

### `market_hours_strict`

Use when:
- session state is `market_hours`

Meaning:
- `1m` bars are required
- snapshots are required
- stale intraday should fail preflight
- stale snapshots should fail preflight

### `after_hours_relaxed`

Use when:
- session state is `after_hours`

Meaning:
- `1m` bars are useful, but not run-failing if they exceed the strict market-hours threshold
- snapshots are useful, but not run-failing under the same strict threshold logic
- stale intraday becomes warning-grade, not blocker-grade
- stale snapshots become warning-grade, not blocker-grade
- news and macro continue to refresh normally

### `closed_calendar_relaxed`

Use when:
- session state is `weekend`
- session state is `holiday`

Meaning:
- `1m` not required
- snapshots not required
- no intraday-source failure due to missing active-session data

## Required Behavior Changes

### Preflight

Preflight should derive the policy state first, then classify source freshness according to policy.

For intraday sources:

- `premarket_strict`: stale => fail-grade
- `market_hours_strict`: stale => fail-grade
- `after_hours_relaxed`: stale => warning-grade
- `closed_calendar_relaxed`: not required

### Runtime Freshness Audit

Runtime freshness should use the same policy state as preflight.

This means:
- no split behavior between journaled preflight and persisted freshness snapshot
- operator-facing freshness is consistent with preflight failure rules

### Alerts

Alert severity should follow the policy state:

- in strict states: stale `1m` / snapshots => critical
- in relaxed after-hours: stale `1m` / snapshots => warning
- in closed-calendar: intraday not required

## Runbook Implications

The ops runbook should describe:
- strict intraday requirements in premarket and market hours
- relaxed intraday expectations after close
- no intraday requirement on closed calendar

## Non-Goals

This policy does not change:
- source contracts
- watermark semantics
- snapshot latest-state contract
- macro contract
- news bucket contract

It only changes how freshness strictness is interpreted by session.
