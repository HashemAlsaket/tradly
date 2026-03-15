# Horizon Lanes V1

## Purpose

This document defines how Tradly should represent directional judgment across multiple time horizons without forcing one single score and confidence number to stand in for all timeframes.

The goal is to support statements like:

1. short-term bearish, medium-term constructive
2. weak next few days, stronger next few weeks
3. usable on delayed data for swing horizons, but not for intraday timing

This design must remain valid after a future Polygon upgrade to real-time data.

## Core Principle

Tradly should operate as one latency-aware system with explicit horizon lanes.

That means:

1. the system does not split into a delayed-data architecture and a real-time architecture
2. the system does not collapse all directional judgment into one confidence number
3. each lane expresses its own:
   - direction
   - strength
   - confidence
   - coverage
   - freshness adequacy

## Canonical Horizon Lanes

Operator-facing labels should use explicit horizons:

1. `1to3d`
2. `1to2w`
3. `2to6w`

Internal lane ids may continue temporarily as implementation aliases.

Phase 1 lanes:

1. `near_term`
   - canonical horizon: `1to3d`
2. `swing_term`
   - canonical horizon: `1to2w`

Deferred lane:

1. `position_term`
   - canonical horizon: `2to6w`
   - add after the current specialist stack is stable

Blocked for now:

1. `intraday`
   - keep as a supported taxonomy value
   - do not treat as an active lane until real-time market data is in place

## Lane Contract

Each deterministic model may continue emitting:

1. `horizon_primary`
2. `horizon_secondary`

But the system should begin standardizing a lane-aware view shaped like:

```json
{
  "lane_id": "near_term",
  "canonical_horizon": "1to3d",
  "signal_direction": "bullish|bearish|neutral",
  "signal_strength": 0.0,
  "confidence_score": 0,
  "confidence_label": "low|medium|high",
  "coverage_state": "sufficient_evidence|thin_evidence|insufficient_evidence",
  "freshness_score": 0,
  "lane_data_freshness_ok": true,
  "why_code": ["machine_reason"],
  "evidence": {
    "supporting_inputs": "value"
  }
}
```

Rules:

1. every lane is evaluated independently
2. a model may be strong in one lane and weak in another
3. lane confidence must mean confidence for that lane only
4. lane freshness must be judged relative to that lane, not globally

## Lane Semantics

### `near_term`

Use for:

1. next-session to next-few-session direction
2. reactions to recent price structure
3. regime-sensitive directional pressure

Sensitivity:

1. highest sensitivity to delayed data
2. highest sensitivity to stale macro/news relative to market state
3. should be the first lane to lose confidence when current-state inputs degrade

### `swing_term`

Use for:

1. next one to two weeks
2. broader momentum, sector leadership, and medium-horizon drift
3. slower-moving macro and news backdrop interpretation

Sensitivity:

1. more tolerant of delayed data than `near_term`
2. more tolerant of slower macro publication cadence
3. should not collapse simply because a context input is a few days old if that input still supports the lane

### `position_term`

Use for:

1. two to six week positioning
2. broader trend persistence and regime backdrop
3. slower-moving macro or valuation-sensitive context

Sensitivity:

1. least sensitive to delayed market data among active directional lanes
2. should tolerate slower macro cadence better than shorter lanes

## Freshness And Confidence By Lane

Freshness and confidence must be lane-specific.

Examples:

1. `macro_points` that are `3` days old may be:
   - too weak for high-confidence `near_term`
   - acceptable for `swing_term`
   - acceptable for `position_term`
2. `DELAYED` market bars may be:
   - materially weaker for `near_term`
   - acceptable with modest penalty for `swing_term`
   - minor penalty for `position_term`

Rules:

1. no input should be treated as simply `fresh` or `stale` without lane context unless it is severely broken or missing
2. warning-grade lag should degrade the affected lane, not automatically collapse all lanes
3. hard blocking should be reserved for:
   - missing required input
   - extreme staleness
   - lane/input mismatch that makes the lane invalid

## Lane-Aware Upstream Propagation

Upstream uncertainty must propagate into downstream lanes.

Examples:

1. if `market_regime_v1` has weak `near_term` confidence, a downstream symbol model must not emit very high `near_term` confidence as if the market overlay were certain
2. if sector movement is strong in `swing_term` but weak in `near_term`, symbol confidence should reflect that split instead of flattening to one value

Rules:

1. downstream models should inherit upstream lane quality, not just upstream freshness
2. downstream lane confidence should be capped or penalized when required upstream lane confidence is weak
3. downstream lane confidence should not exceed what the upstream lane support plausibly allows unless the downstream model has strong independent evidence

## Transitional Representation

Phase 1 implementation does not need to redesign every artifact at once.

Transitional rules:

1. models may keep emitting `horizon_primary` and `horizon_secondary`
2. the first retrofit may add:
   - `lane_primary`
   - `lane_secondary`
   - optional `lane_diagnostics`
3. full per-lane row output can follow after the first lane-aware retrofit proves stable

Recommended Phase 1 mapping:

1. `horizon_primary = 1to3d` -> `lane_primary = near_term`
2. `horizon_primary = 1to2w` -> `lane_primary = swing_term`
3. `horizon_primary = 2to6w` -> `lane_primary = position_term`

Operator rule:

1. dashboards and actionability summaries should present the canonical horizons directly
2. lane ids should remain a model/debug concept unless explicitly needed

## Delayed-To-Realtime Compatibility

This design should survive the future real-time upgrade unchanged.

What changes after real-time upgrade:

1. lane freshness penalties from delayed bars become smaller or disappear for supported feeds
2. `intraday` can later become an active lane
3. `near_term` becomes more trustworthy for current-session-sensitive models

What does not change:

1. lane separation
2. lane-specific confidence
3. lane-specific freshness reasoning
4. upstream uncertainty propagation

## Phase 1 Adoption Order

1. update the modeling framework to recognize horizon lanes formally
2. retrofit `market_regime_v1` first
   - let macro warning degrade `near_term` more than `swing_term`
3. retrofit `symbol_movement_v1`
   - inherit upstream lane confidence and coverage
4. expand the dashboard later to show lane-aware trust instead of one flattened conclusion

## Non-Goals For Phase 1

1. do not introduce real-time-only logic
2. do not add active intraday trading lanes yet
3. do not rebuild every specialist model in one pass
4. do not treat lane outputs as a substitute for deterministic quality audits
