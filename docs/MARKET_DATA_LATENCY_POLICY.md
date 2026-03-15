# Market Data Latency Policy

This document defines how market-data latency should affect modeling, freshness, coverage, and confidence.

The goal is to make delayed-data handling explicit and consistent across all deterministic models.

## Purpose

The system currently uses Polygon stock market data on a plan that provides approximately `15-minute delayed` stock data.

That means:

1. delayed data must not be treated as equivalent to real-time data
2. latency must be modeled explicitly
3. the effect of latency must depend on model horizon

## Core Concepts

Every model that uses market bars should distinguish between:

1. `recency`
   - whether the latest bar is from the expected market date or expected bar interval
2. `latency`
   - how far behind the current market state the bar is
3. `freshness`
   - the model-level judgment that combines recency and latency for the model's horizon

Recency and latency are not the same thing.

Example:

1. a daily bar can be same-day or current-market-date and still be delayed
2. a bar can be real-time but stale if it is from the wrong market date

## Canonical Data Statuses

Current supported market-data statuses:

1. `REALTIME`
2. `DELAYED`

If a source introduces more specific status values later, they must be mapped into this policy before model scoring.

## Canonical Latency Assumption

For Polygon Stocks Starter:

1. assume `market_data_latency_minutes = 15` for stock market data marked `DELAYED`

For `REALTIME` data:

1. assume `market_data_latency_minutes = 0`

## Required Evidence Fields

Any deterministic model using market bars should emit these evidence fields:

1. `latest_bar_utc`
2. `latest_market_date`
3. `expected_min_market_date`
4. `data_status`
5. `market_data_latency_minutes`
6. `recency_ok`
7. `latency_class`

## Latency Classes

Models should classify latency into one of:

1. `realtime`
2. `delayed_tolerable`
3. `delayed_material`
4. `stale`

Classification rules:

1. if `data_status = REALTIME`, `latency_class = realtime`
2. if `data_status = DELAYED` and the model horizon is `1to2w` or longer, `latency_class = delayed_tolerable`
3. if `data_status = DELAYED` and the model horizon is `1to3d`, `latency_class = delayed_material`
4. if the latest market date is older than the expected minimum market date, `latency_class = stale`

## Horizon-Aware Policy

### 1. `intraday`

Policy:

1. delayed stock bars are not acceptable as full-strength evidence
2. if `data_status = DELAYED`, the model must not emit `sufficient_evidence`
3. if the model depends primarily on current market state, delayed data should usually become `insufficient_evidence`

Required handling:

1. cap `confidence_score` at `25`
2. emit `coverage_state = insufficient_evidence` unless the model is explicitly designed for lagged contextual intraday use
3. emit `why_code` including `market_data_delayed_intraday`

### 2. `1to3d`

Policy:

1. delayed stock bars are usable, but materially weaker than real-time
2. delayed data may still support a valid directional model
3. delayed data must not receive a perfect freshness score

Required handling:

1. if `data_status = DELAYED`, cap `freshness_score` at `70`
2. if the model is strongly sensitive to same-session moves, cap `freshness_score` at `60`
3. do not automatically downgrade coverage to `thin_evidence` solely because data is delayed
4. emit `why_code` including `market_data_delayed_15m`

### 3. `1to2w`

Policy:

1. delayed stock bars are generally acceptable
2. delayed data still deserves a small freshness penalty, but not a severe one

Required handling:

1. if `data_status = DELAYED`, cap `freshness_score` at `85`
2. keep `coverage_state = sufficient_evidence` if recency is otherwise good
3. emit `why_code` including `market_data_delayed_15m`

### 4. `2to6w`

Policy:

1. a `15-minute` delay is operationally minor
2. delayed data still should not be represented as identical to real-time

Required handling:

1. if `data_status = DELAYED`, cap `freshness_score` at `90`
2. keep `coverage_state = sufficient_evidence` if recency is otherwise good
3. emit `why_code` including `market_data_delayed_15m`

## Recency Rules

Latency policy does not override recency requirements.

If the latest market date is older than the expected minimum market date:

1. `recency_ok = false`
2. `latency_class = stale`
3. `coverage_state` must be at most `thin_evidence`
4. if the stale gap is severe for the model horizon, use `insufficient_evidence`

## Data-Status Impact On Coverage

Coverage should follow these rules:

1. `REALTIME` + recency OK:
   - coverage may remain `sufficient_evidence`
2. `DELAYED` + recency OK:
   - `intraday`: at most `thin_evidence`, usually `insufficient_evidence`
   - `1to3d`: may remain `sufficient_evidence`
   - `1to2w`: may remain `sufficient_evidence`
   - `2to6w`: may remain `sufficient_evidence`
3. stale market date:
   - at most `thin_evidence`

## Data-Status Impact On Confidence

Models must not let delayed data flow through as if it were full-strength fresh data.

Minimum required policy:

1. `REALTIME`:
   - no latency penalty
2. `DELAYED`:
   - apply horizon-aware freshness cap
   - include delay in `why_code`
3. stale recency:
   - apply stronger confidence cap according to missing-data rules

## Model-Level Implementation Rule

Every deterministic market-data model should:

1. compute `market_data_latency_minutes`
2. compute `recency_ok`
3. derive `latency_class`
4. apply horizon-aware freshness caps
5. surface delay-related `why_code`

No model may silently treat `DELAYED` as equivalent to `REALTIME`.

## Phase 1 Implementation Guidance

The first implementation pass should apply this policy to:

1. `market_regime_v1`
2. `sector_movement_v1`
3. `symbol_movement_v1`

## Future Upgrade Impact

If the Polygon plan is upgraded to real-time:

1. `market_data_latency_minutes` for supported real-time bars becomes `0`
2. the delayed-data penalties in this policy no longer apply for those bars
3. intraday-capable models may then move from blocked/thin evidence to sufficient evidence when recency is also good

This policy should remain in place even after upgrading, because:

1. not all sources may be real-time
2. some instruments may still arrive delayed
3. explicit latency handling remains safer than implicit assumptions
