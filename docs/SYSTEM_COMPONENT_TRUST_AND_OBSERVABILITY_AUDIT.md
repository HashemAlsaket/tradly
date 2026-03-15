# System Component Trust And Observability Audit

This document maps the major system components, their intended behavior, current trust level, and current observability level.

The goal is to make it easy to answer:

1. what each component is supposed to do
2. what inputs and outputs it owns
3. whether we currently trust it
4. whether we can diagnose bad behavior quickly
5. what needs instrumentation before we continue scaling the system

## Trust Levels

Use these trust labels:

1. `trusted`
   - behavior is understandable and outputs are credible enough for current use
2. `provisional`
   - structure is acceptable, but behavior still needs calibration or validation
3. `failing`
   - behavior is currently not trustworthy for live decision support

## Observability Levels

Use these observability labels:

1. `strong`
   - inputs, outputs, and failure reasons are quick to inspect
2. `partial`
   - some evidence exists, but diagnosis still requires inference
3. `weak`
   - behavior can fail without fast causal explanation

## Component Map

### 1. Universe Registry

Purpose:

1. define canonical symbols, roles, sectors, and source-of-truth membership

Primary files:

1. [universe_registry.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/manual/universe_registry.json)
2. [universe_runtime_scopes.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/manual/universe_runtime_scopes.json)

Inputs:

1. manual universe definitions
2. registry sync process

Outputs:

1. active runtime scopes
2. market-data symbol set
3. model symbol set
4. direct-news symbol set
5. sector groupings

Trust:

1. `trusted`

Observability:

1. `strong`

Why:

1. the scopes are explicit, inspectable, and currently behaving as intended

Main remaining risk:

1. scope changes must continue to be synchronized cleanly into runtime files

### 2. Market Data Ingest

Purpose:

1. ingest market bars into `market_bars`

Primary files:

1. [ingest_market_bars.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/pipeline/ingest_market_bars.py)

Inputs:

1. Polygon stock data
2. runtime market-data symbols

Outputs:

1. daily bar rows in the database
2. `data_status` mapped into `REALTIME` or `DELAYED`

Trust:

1. `provisional`

Observability:

1. `partial`

Why:

1. ingest itself works, and sector ETF coverage is now in place
2. latency status exists, but provider latency effects still need clearer model-level traceability

Main remaining risks:

1. delayed data is still easy to misinterpret as strong evidence downstream
2. provider/network issues still surface mostly at pipeline-run time

### 3. News Ingest And Linking

Purpose:

1. pull news
2. link it to symbols
3. maintain symbol-news coverage

Primary sources:

1. `news_events`
2. `news_symbols`
3. freshness audit outputs

Trust:

1. `provisional`

Observability:

1. `partial`

Why:

1. recency and usage are visible in freshness snapshots
2. coverage is still uneven and not yet surfaced as a clean per-symbol trust map in live modeling

Main remaining risks:

1. residual zero-coverage direct-news symbols
2. incomplete visibility into how coverage gaps affect downstream symbol/sector news models

### 4. News Interpretation

Purpose:

1. convert linked news into structured interpreted signals

Primary file:

1. [interpret_news_llm.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/pipeline/interpret_news_llm.py)

Trust:

1. `provisional`

Observability:

1. `weak`

Why:

1. interpreted outputs exist, but the schema is still too narrow for the intended sector/symbol stack
2. we do not yet have a strong trust map for interpretation quality by scope

Main remaining risks:

1. sector coverage is schema-limited
2. downstream modeling can inherit narrow interpretation semantics without obvious warning

### 5. Macro Data Ingest

Purpose:

1. provide macro context through `macro_points`

Trust:

1. `provisional`

Observability:

1. `partial`

Why:

1. freshness is visible and enforced in `market_regime_v1`
2. macro freshness and directional impact still need richer tracing

Main remaining risk:

1. macro recency and macro meaning are still easier to observe than to interpret behaviorally

### 6. Runtime Freshness Audit

Purpose:

1. assess whether runtime data is fresh enough for the system to operate

Primary files:

1. [freshness_snapshot.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/journal/freshness_snapshot.json)
2. [runtime_freshness_audit.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/ops/runtime_freshness_audit.py)

Trust:

1. `trusted`

Observability:

1. `strong`

Why:

1. current status, checks, and recency details are explicit and easy to inspect

Main remaining risk:

1. dashboard blocking is not yet fully tied to freshness status

### 7. Shared Calibration Utility

Purpose:

1. standardize normalization, latency handling, confidence scoring, and artifact quality audits

Primary file:

1. [calibration.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/models/calibration.py)

Trust:

1. `provisional`

Observability:

1. `partial`

Why:

1. the utility now has focused tests and central policy handling
2. it still needs more runtime trace outputs to explain exactly which caps and rules fired for live rows

Main remaining risks:

1. cap reasons are still implicit rather than explicitly emitted
2. artifact audit summaries do not yet surface row-level examples of failure triggers

### 8. Market Regime Model

Purpose:

1. produce one broad-market directional context row

Primary files:

1. [market_regime.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/models/market_regime.py)
2. [run_market_regime.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/pipeline/run_market_regime.py)
3. [market_regime_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-14/market_regime_v1.json)

Trust:

1. `provisional`

Observability:

1. `partial`

Why:

1. it now uses the shared calibration utility
2. it emits useful evidence and payload-level quality audit
3. we still lack a fuller diagnostic trace for how confidence and score were constructed

Main remaining risks:

1. current score magnitude may still be too strong
2. quality audit passes structurally, but behavioral realism still needs more evaluation

### 9. Sector Movement Model

Purpose:

1. produce one directional row per canonical stock sector

Primary files:

1. [sector_movement.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/models/sector_movement.py)
2. [run_sector_movement.py](/Users/hashemalsaket/Desktop/workspace/tradly/src/tradly/pipeline/run_sector_movement.py)
3. [sector_movement_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-14/sector_movement_v1.json)

Trust:

1. `failing`

Observability:

1. `partial`

Why:

1. input audit and row audit are good
2. payload-level quality audit is good
3. the model still fails its own quality audit for score saturation and confidence clustering

Main remaining risks:

1. normalization scale is still too aggressive
2. confidence is still clustering too tightly
3. we do not yet emit enough row-level diagnostics explaining exactly why those failures occur

### 10. Dashboard And Operator Surface

Purpose:

1. expose current specialist outputs and system status to the operator

Primary file:

1. [app.py](/Users/hashemalsaket/Desktop/workspace/tradly/dashboard/app.py)

Trust:

1. `failing`

Observability:

1. `partial`

Why:

1. the dashboard itself is inspectable
2. it now reads specialist artifacts only
3. it is usable as a diagnostics and health surface, not yet a final decision surface

Main remaining risks:

1. there is no ensemble or final decision layer yet
2. specialist diagnostics can still be richer

## Biggest System-Level Gaps

These are the main reasons bad outputs are still hard to diagnose:

1. no standard per-row `diagnostics` or `debug_trace` block for deterministic models
2. no explicit cap-reason tracing for confidence and freshness adjustments
3. no row-example output in `quality_audit` failures
4. there is still no specialist ensemble or final decision layer

## Observability Improvements Needed Before More Model Expansion

### 1. Per-row diagnostics block

Every deterministic model row should eventually emit a compact diagnostics section with:

1. raw feature values used for scoring
2. normalization inputs
3. latency assessment result
4. coverage decision inputs
5. confidence input breakdown
6. cap reasons applied

### 2. Better quality-audit diagnostics

Artifact-level quality audits should eventually include:

1. failing row count
2. representative failing row ids or scope ids
3. which audit rules each example violated

### 3. Runner summaries

Pipeline runners should print compact summaries such as:

1. score range
2. confidence range
3. audit status
4. top failure reasons

### 4. Specialist convergence

The active operator surface should continue to converge on specialist artifacts only.

## Current Recommended Priority

The next observability-oriented priorities are:

1. iterate `sector_movement_v1` until it passes quality audit
2. add row-level diagnostics for specialist models
3. tighten dashboard freshness/system-state blocking
4. build the specialist ensemble and final decision layer

## Current Trust Summary

Most trusted today:

1. universe and runtime scopes
2. freshness audit
3. step-by-step implementation workflow

Most concerning today:

1. `sector_movement_v1` behavior
2. lack of specialist ensemble/final decision layer
3. dashboard still being primarily a status surface
4. weak behavioral observability across modeling outputs
