# Delayed To Realtime Compatibility Plan

This document defines how the system should behave while market data is `T-15` and how that same architecture should behave after a future upgrade to real-time data.

The goal is to keep building safely now without creating a second architecture that would need to be replaced later.

## Core Principle

There should be one modeling system, not:

1. a delayed-data system
2. a separate real-time system

Instead, the system should always be:

1. latency-aware
2. horizon-aware
3. explicit about evidence quality

Real-time data should improve evidence quality and unlock additional model classes, but it should not require rewriting the core modeling stack.

## Current Assumption

Current stock market data assumption:

1. source: Polygon Stocks Starter
2. stock market data status: commonly `DELAYED`
3. assumed stock-market latency when delayed: `15 minutes`

This means the active system should treat current market-bar evidence as:

1. same-session or same-market-date data in many cases
2. not real-time
3. acceptable for swing and short multi-day modeling
4. unacceptable as full-strength evidence for true intraday decision quality

## What Is Allowed On T-15 Data

The following model classes are valid to build and operate on delayed market data now:

1. `market_regime_v1`
2. `sector_movement_v1`
3. `symbol_movement_v1`
4. `sector_news_v1`
5. `symbol_news_v1`
6. `range_expectation_v1`
7. `ensemble_v1`

Conditions:

1. they must apply the shared latency policy
2. they must apply the shared calibration and confidence rules
3. they must not claim real-time evidence quality
4. they must emit explicit latency metadata

## What Is Not Allowed As Full-Strength On T-15 Data

The following should remain blocked, thin-evidence, or heavily penalized until real-time data is available:

1. true `intraday` directional models
2. execution-timing models based on current session microstructure
3. open-sensitive or close-sensitive tactical decision models
4. VWAP and session execution models used for live action timing

These may still exist in placeholder or deferred form, but they should not become trusted live-decision inputs while stock bars remain delayed.

## Compatibility Contract

To keep the later upgrade seamless, every market-data-driven model must use the same latency-aware contract now.

Required fields:

1. `data_status`
2. `market_data_latency_minutes`
3. `latency_class`
4. `recency_ok`
5. `freshness_score`

Required behavior:

1. determine evidence quality from horizon plus latency, not from provider-plan assumptions hardcoded inside each model
2. keep model logic separate from provider-specific plan names
3. treat `REALTIME` and `DELAYED` as data states, not separate architectures
4. allow the same model to run under either state with different evidence quality outcomes

## What Changes After A Realtime Upgrade

If stock market data becomes real-time:

1. `data_status` for supported bars becomes `REALTIME`
2. `market_data_latency_minutes` becomes `0`
3. latency penalties no longer apply for those bars
4. freshness caps relax automatically under the same policy
5. confidence may improve if the rest of the evidence remains strong
6. `intraday` and execution-aware models may move from blocked to eligible

## What Must Not Change After A Realtime Upgrade

These system behaviors should remain the same:

1. model registry structure
2. model output contract
3. ensemble contract
4. missing-data rules
5. quality-audit rules
6. requirement to emit latency metadata
7. requirement to keep delayed-data handling explicit

The upgrade should improve input quality, not redefine model meaning.

## Build Order Under Current Data

The safe build order while still on delayed stock data is:

1. shared calibration and quality-audit utility
2. retrofit existing movement models and the active specialist runtime
3. dashboard safety tightening
4. `symbol_movement_v1`
5. `sector_news_v1`
6. `symbol_news_v1`
7. `range_expectation_v1`
8. `ensemble_v1`

Deferred until real-time is in place:

1. true intraday directional modeling
2. live execution-timing models
3. VWAP/session execution models as trusted action inputs

## Upgrade Trigger

The system should remain on delayed data until all of the following are true:

1. shared latency and calibration logic is implemented
2. current daily-bar specialist models are calibrated and passing quality audits
3. live-path operator safety is tightened
4. the next active workstream genuinely requires intraday or execution-aware modeling

At that point, upgrading to real-time should be treated as an evidence-quality upgrade, not a modeling redesign.

## Decision Rule

Current decision:

1. continue building on `T-15` data
2. keep all models latency-aware
3. avoid treating delayed data as live
4. defer intraday and execution-sensitive models until real-time data is available

This is the intended compatibility path unless a future source or provider change forces a new data contract.
