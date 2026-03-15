# Data Gaps And Upgrade Path

This document tracks current data limitations, their effect on modeling, and the conditions under which upgrading data access becomes justified.

The goal is to keep data-source decisions deliberate and tied to actual system needs.

## Current Market Data Source

Primary market data source:

1. Polygon Stocks Starter

Current known characteristics:

1. all US stocks tickers
2. unlimited API calls
3. 5 years historical data
4. `15-minute delayed` market data
5. daily and minute aggregate support available under the current plan, but still subject to the delayed-data policy

This means the current system should assume:

1. `market_data_latency_minutes = 15`
2. stock bars are usable for swing and short multi-day modeling
3. stock bars are not reliable enough for true intraday decision quality

## Current Modeling-Relevant Data Gaps

### 1. Market data is delayed, not real-time

Current gap:

1. Polygon stock data is delayed by approximately `15` minutes on the current plan

Why it matters:

1. delayed data should not be treated as equivalent to real-time
2. intraday timing models become unreliable
3. near-open and near-close decision quality is weaker
4. confidence should be penalized when model horizon is sensitive to live state

Current operational rule:

1. delayed data is acceptable for:
   - `1to3d`
   - `1to2w`
2. delayed data is not acceptable as full-strength evidence for:
   - `intraday`
   - execution timing
   - live decision quality near the open or close

### 2. Delayed-data policy is not fully implemented in code yet

Current gap:

1. the system now knows the delay assumption, but model freshness and confidence handling still need to be standardized across models

Why it matters:

1. models can still become overconfident by treating delayed bars as fully valid
2. different models may handle delayed data inconsistently

Required fix:

1. create a shared market-data latency policy
2. apply horizon-aware freshness penalties for `DELAYED`

### 3. Score normalization is not yet shared across models

Current gap:

1. deterministic models currently use different raw-score mappings and clipping behavior

Why it matters:

1. model outputs are not yet reliably comparable
2. ensemble quality will degrade if inputs are not calibrated on a shared scale

Required fix:

1. add a shared normalization utility before building more specialist models

### 4. Confidence is still inflated by data presence

Current gap:

1. confidence currently rises too easily when data is present and fresh, even if signal richness is limited

Why it matters:

1. system outputs can appear more trustworthy than they are
2. downstream ensemble weighting would become distorted

Required fix:

1. redesign confidence to reflect:
   - signal richness
   - signal separation
   - stability
   - latency-sensitive freshness

### 5. Output-quality audits are still incomplete

Current gap:

1. the system can now audit missing inputs, but it does not yet fail outputs for calibration problems like score saturation or confidence clustering

Why it matters:

1. a model can be structurally valid but operationally unusable

Required fix:

1. add model-quality audit checks for:
   - score clipping frequency
   - confidence clustering
   - score-confidence mismatch

## Current Data Coverage Status

### Market bars

Current state:

1. broad market proxies are seeded
2. canonical sector ETF proxies are now included in `market_data_symbols`
3. canonical sector ETF daily bars have now been seeded into `market_bars`

Implication:

1. `sector_movement_v1` is no longer blocked by missing sector proxy bars

### News

Current state:

1. direct-news coverage is improved but still incomplete
2. the known residual gap remains concentrated in a small set of symbols
3. direct-news coverage and mapping limitations still affect symbol and sector news models

Implication:

1. data upgrade decisions should not be based only on Polygon
2. some future modeling limitations may still come from news coverage rather than market data

## When We Do Not Need To Upgrade

Stay on the current Polygon plan if the active modeling work is focused on:

1. daily-bar modeling
2. `1to3d` and `1to2w` horizons
3. sector movement modeling
4. symbol movement modeling
5. expected-move / range models using daily volatility
6. deterministic ensemble logic built from delayed daily data

In this phase, the bigger need is policy and calibration, not more expensive data.

## When We Probably Do Need To Upgrade

Upgrade to real-time Polygon stock data when any of the following become true:

1. we begin relying on `intraday` horizons for actual decision-making
2. we implement intraday execution logic or VWAP-style timing
3. we want confidence to reflect near-live market state rather than delayed state
4. we want actionable open/close-sensitive models
5. we begin treating minute aggregates as a live execution input rather than a lagged contextual input

## Recommended Upgrade Trigger

The clean upgrade trigger is:

1. after delayed-data policy is implemented
2. after shared normalization and confidence calibration are implemented
3. after the current daily-bar specialist models are behaving credibly
4. before intraday or execution-aware models become part of the real decision stack

This avoids paying for real-time data before the modeling foundation is ready to use it well.

## Future Upgrade Checklist

Before upgrading Polygon, confirm:

1. which endpoints and workflows need real-time:
   - daily aggregates
   - minute aggregates
   - trades
   - quotes
2. which models will actually consume the upgraded data
3. whether the upgrade is needed for:
   - better modeling
   - better execution timing
   - both
4. whether the current system has already fixed:
   - delayed-data handling
   - shared normalization
   - confidence calibration
   - output-quality audits

## Current Recommendation

Current recommendation:

1. do not upgrade yet
2. explicitly model Polygon stock data as `15-minute delayed`
3. fix latency policy, normalization, confidence, and output-quality audits first
4. revisit upgrade when intraday or execution-aware models become the next active workstream
