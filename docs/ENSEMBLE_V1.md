# Ensemble V1

## Purpose

This document defines the first deterministic ensemble layer for Tradly.

The goal is to combine the current specialist stack into one lane-aware symbol-level decision surface without collapsing uncertainty, thin evidence, or stale upstream support into fake precision.

This is a spec only. It does not imply that `ensemble_v1` is already implemented.

## Phase 1 Inputs

`ensemble_v1` should consume only currently active specialist models:

1. [market_regime_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-15/market_regime_v1.json)
2. [sector_movement_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-15/sector_movement_v1.json)
3. [symbol_movement_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-15/symbol_movement_v1.json)
4. [symbol_news_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-15/symbol_news_v1.json)
5. [sector_news_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-15/sector_news_v1.json)
6. [range_expectation_v1.json](/Users/hashemalsaket/Desktop/workspace/tradly/data/runs/2026-03-15/range_expectation_v1.json)

Rules:

1. `market_regime_v1`, `sector_movement_v1`, `symbol_movement_v1`, `symbol_news_v1`, and `sector_news_v1` are directional inputs
2. `range_expectation_v1` is supporting-only and must not create direction on its own
3. phase 1 ensemble scope is `symbol`
4. phase 1 ensemble operates only on active horizon lanes:
   - `near_term`
   - `swing_term`

## Core Principle

`ensemble_v1` should combine directional agreement and disagreement by lane while using range as a conviction modifier, not a directional vote.

That means:

1. movement and news models vote on direction
2. market and sector context constrain symbol conviction
3. range expectation modulates how assertive the ensemble should be
4. thin upstream evidence must propagate downward

## Ensemble Row Contract

Each ensemble row should remain compatible with the specialist output contract:

```json
{
  "model_id": "ensemble_v1",
  "model_scope": "symbol",
  "scope_id": "NVDA",
  "horizon_primary": "1to2w",
  "horizon_secondary": ["1to3d"],
  "lane_primary": "swing_term",
  "lane_secondary": ["near_term"],
  "signal_direction": "bullish|bearish|neutral",
  "signal_strength": 0.0,
  "confidence_score": 0,
  "confidence_label": "low|medium|high",
  "coverage_state": "sufficient_evidence|thin_evidence|insufficient_evidence",
  "score_raw": 0.0,
  "score_normalized": 0.0,
  "why_code": ["machine_reason"],
  "lane_diagnostics": {
    "near_term": {},
    "swing_term": {}
  },
  "evidence": {
    "component_inputs": {}
  },
  "as_of_utc": "2026-03-15T00:00:00+00:00",
  "data_freshness_ok": true
}
```

## Horizon Output Contract

`ensemble_v1` should become the primary operator-facing source for explicit horizon actionability.

That means each row should expose a horizon-level view keyed by:

1. `1to3d`
2. `1to2w`
3. `2to6w`

Recommended shape:

```json
{
  "horizon_summary": {
    "1to3d": {
      "state": "actionable|research_only|blocked",
      "signal_direction": "bullish|bearish|neutral",
      "signal_strength": 0.0,
      "confidence_score": 0,
      "confidence_label": "low|medium|high",
      "coverage_state": "sufficient_evidence|thin_evidence|insufficient_evidence",
      "score_normalized": 0.0,
      "why_code": ["machine_reason"],
      "data_freshness_ok": true
    },
    "1to2w": {
      "state": "actionable|research_only|blocked",
      "signal_direction": "bullish|bearish|neutral",
      "signal_strength": 0.0,
      "confidence_score": 0,
      "confidence_label": "low|medium|high",
      "coverage_state": "sufficient_evidence|thin_evidence|insufficient_evidence",
      "score_normalized": 0.0,
      "why_code": ["machine_reason"],
      "data_freshness_ok": true
    },
    "2to6w": {
      "state": "actionable|research_only|blocked",
      "signal_direction": "bullish|bearish|neutral",
      "signal_strength": 0.0,
      "confidence_score": 0,
      "confidence_label": "low|medium|high",
      "coverage_state": "sufficient_evidence|thin_evidence|insufficient_evidence",
      "score_normalized": 0.0,
      "why_code": ["..."],
      "data_freshness_ok": false
    }
  }
}
```

Rules:

1. `horizon_summary` is operator-facing and should use explicit horizon labels, not internal lane ids
2. `1to3d` should map from the ensemble `near_term` lane
3. `1to2w` should map from the ensemble `swing_term` lane
4. `2to6w` should map from the ensemble `position_term` lane
5. existing `lane_diagnostics` may remain for debugging, but `horizon_summary` should become the dashboard contract

## Horizon State Rules

For `ensemble_v1`, horizon state should be derived deterministically from existing ensemble lane outputs.

### `1to3d`

Use the `near_term` lane and map:

1. `blocked`
   - required lane data missing
   - or lane freshness/alignment contract broken
2. `research_only`
   - lane `coverage_state != sufficient_evidence`
   - or lane carries materially thin upstream context
3. `actionable`
   - lane `coverage_state = sufficient_evidence`
   - and no major lane blocker is present

### `1to2w`

Use the `swing_term` lane and map:

1. `blocked`
   - required lane data missing
   - or lane freshness/alignment contract broken
2. `research_only`
   - lane `coverage_state != sufficient_evidence`
   - or lane carries material unresolved thinness or conflict
3. `actionable`
   - lane `coverage_state = sufficient_evidence`
   - and no major lane blocker is present

### `2to6w`

Use the `position_term` lane and map:

1. `blocked`
   - required lane data missing
   - or lane freshness/alignment contract broken
2. `research_only`
   - lane `coverage_state != sufficient_evidence`
   - or lane carries material unresolved thinness or conflict
3. `actionable`
   - lane `coverage_state = sufficient_evidence`
   - and no major lane blocker is present

## LLM Relationship

The LLM interpreter should not emit separate interpretations per horizon in phase 1.

Rules:

1. one article still produces one interpretation record
2. that record may carry one primary `impact_horizon`
3. deterministic news models are responsible for translating article populations into horizon-specific symbol/sector outputs
4. ensemble horizon actionability should therefore remain deterministic, not LLM-authored

Rationale:

1. this keeps horizon aggregation stable and auditable
2. it avoids making the LLM the source of multi-horizon portfolio judgment
3. it remains compatible with future richer schemas if needed

## Lane-Aware Aggregation

The ensemble should aggregate each lane independently.

### `near_term`

Primary contributors:

1. `market_regime_v1.near_term`
2. `sector_movement_v1.near_term`
3. `symbol_movement_v1.near_term`
4. `symbol_news_v1.near_term`
5. `sector_news_v1.near_term`

### `swing_term`

Primary contributors:

1. `market_regime_v1.swing_term`
2. `sector_movement_v1.swing_term`
3. `symbol_movement_v1.swing_term`
4. `symbol_news_v1.swing_term`
5. `sector_news_v1.swing_term`

## Phase 1 Weighting

Initial directional weighting should follow the current model registry closely while remaining lane-local:

1. symbol movement: `0.30`
2. symbol news: `0.20`
3. sector movement: `0.20`
4. market regime: `0.15`
5. sector news: `0.15`

Rules:

1. symbol-level models should generally have the highest local weight
2. market and sector models should constrain and contextualize, not dominate, unless symbol evidence is thin
3. weights are starting priors, not guarantees of full contribution

## Contribution Eligibility

A component lane may contribute only if:

1. the required upstream artifact exists
2. the artifact is temporally aligned for downstream use
3. the component lane exists
4. the component lane `coverage_state` meets at least the model registry minimum

Contribution scaling rules:

1. `sufficient_evidence` contributes at full weight
2. `thin_evidence` contributes at reduced weight
   - default multiplier: `0.50`
3. `insufficient_evidence` contributes zero directional weight

Additional propagation rule:

1. `thin_evidence` affects both:
   - directional contribution weight
   - final ensemble confidence cap
2. apply these in order:
   - contribution weighting during directional aggregation
   - then final confidence caps after ensemble confidence is computed

## Direction Aggregation

For each contributing component lane:

1. convert `signal_direction` into signed polarity:
   - `bullish = +1`
   - `bearish = -1`
   - `neutral = 0`
2. multiply polarity by:
   - component base weight
   - coverage multiplier
   - lane confidence factor
   - lane signal strength

Suggested lane confidence factor:

```text
lane_confidence_factor = confidence_score / 100
```

Suggested directional contribution:

```text
directional_contribution
= polarity
 * base_weight
 * coverage_multiplier
 * lane_confidence_factor
 * signal_strength
```

The ensemble lane raw score is:

```text
ensemble_lane_raw
= sum(directional_contribution for all eligible component lanes)
```

Normalize that lane score with the shared calibration utility into `score_normalized`.

## Agreement And Conflict

The ensemble should explicitly account for component conflict.

Rules:

1. if strong components disagree materially, ensemble confidence must fall
2. if only one component is directional while others are absent or neutral, confidence must remain capped
3. if symbol and sector agree while market disagrees, the ensemble may still be directional but should reflect tension in `why_code`

Phase 1 conflict signals should include:

1. directional agreement ratio across contributing components
2. opposing weighted contribution share
3. count of non-neutral contributing components

Phase 1 conflict penalty:

```text
conflict_penalty_points
= round(25 * opposing_weighted_share)
```

Rules:

1. `opposing_weighted_share` is the smaller of bullish-weight share and bearish-weight share among non-neutral contributors
2. if only one component is non-neutral, apply an additional `single_component_penalty = 10`

## Range Modulation

`range_expectation_v1` must not vote on direction.

Instead it should modify conviction and interpretation:

1. if expected range is expanding sharply:
   - reduce directional confidence modestly
   - append a reason like `range_expanding_conviction_reduced`
2. if expected range is stable or contracting:
   - allow confidence to remain closer to the directional aggregate
3. if the expected move is extremely wide for the lane:
   - avoid high-confidence directional outputs even when movement/news agree

Phase 1 rule:

1. derive a `range_pressure_score` from the primary lane in `range_expectation_v1`
2. apply a confidence haircut, not a directional sign change
3. use the range lane matching the ensemble lane:
   - `near_term` ensemble uses `range_expectation_v1.near_term`
   - `swing_term` ensemble uses `range_expectation_v1.swing_term`

Phase 1 `range_pressure_score`:

```text
range_pressure_score
= clamp(
     (range_confidence_factor * expected_move_pct) / lane_move_reference_pct
   , 0.0
   , 2.0
   )
```

Where:

1. `range_confidence_factor = range_confidence_score / 100`
2. `expected_move_pct` comes from the matching lane in `range_expectation_v1`
3. `lane_move_reference_pct` defaults to:
   - `6.0` for `near_term`
   - `10.0` for `swing_term`

Phase 1 range haircut:

```text
range_confidence_haircut_points
= round(max(0.0, range_pressure_score - 1.0) * 20)
```

Rules:

1. if `range_pressure_score <= 1.0`, no haircut is applied
2. if `range_pressure_score > 1.0`, reduce final confidence by `range_confidence_haircut_points`
3. range may reduce confidence only; it may not change directional sign

## Confidence Construction

The ensemble should use shared confidence logic, but with additional ensemble-specific inputs:

1. component coverage breadth
2. directional agreement score
3. upstream freshness adequacy
4. conflict penalty
5. range modulation penalty

Rules:

1. ensemble confidence must not exceed the strongest plausible upstream lane support
2. if the key upstream market or sector lane is `thin_evidence`, ensemble confidence must be capped accordingly
3. if all symbol-local inputs are missing and only context remains, ensemble confidence must remain low
4. range expansion can reduce confidence, but should not force a neutral direction by itself

Phase 1 exact final confidence cap:

```text
final_confidence_cap
= min(
     strongest_symbol_support_cap,
     context_support_cap,
     thin_context_cap,
     single_component_cap
   )
```

Where:

1. `strongest_symbol_support_cap`
   - if one or more symbol-local directional contributors are non-neutral:
     - max confidence among non-neutral `symbol_movement_v1` and `symbol_news_v1` lane contributors
   - otherwise `55`
2. `context_support_cap`
   - max confidence among non-neutral market/sector contributors
   - default `70` if all context contributors are neutral or absent
3. `thin_context_cap`
   - `70` if any required market/sector contributor is `thin_evidence`
   - otherwise `100`
4. `single_component_cap`
   - `65` if only one non-neutral contributor exists
   - otherwise `100`

Phase 1 final confidence order:

1. compute ensemble base confidence from shared confidence logic
2. subtract `conflict_penalty_points`
3. subtract `range_confidence_haircut_points`
4. apply `final_confidence_cap`
5. clamp to `[0, 100]`

## Coverage State

The ensemble lane coverage state should be determined from contributing lane availability and quality.

Suggested rules:

1. `sufficient_evidence`
   - symbol movement available
   - at least one of symbol news or sector movement available
   - upstream market lane not insufficient
2. `thin_evidence`
   - symbol movement available but supporting layers are sparse
   - or market/sector context is only thin
3. `insufficient_evidence`
   - symbol movement unavailable
   - or required upstream lane missing/misaligned

## Primary Lane Selection

The ensemble should compute both active lanes and then choose a row-level primary lane.

Phase 1 rule:

1. choose the lane with:
   - higher coverage quality first
   - then higher confidence
   - then higher absolute normalized score
2. keep the other active lane in `lane_secondary`

## Why Codes

The ensemble should emit compact machine-readable reasons such as:

1. `symbol_movement_supports_bullish`
2. `symbol_news_supports_bullish`
3. `sector_context_supportive`
4. `market_context_headwind`
5. `component_conflict_high`
6. `range_expanding_conviction_reduced`
7. `upstream_lane_thin`

## Temporal Alignment

The ensemble runner must validate upstream artifact alignment before aggregation.

Rules:

1. all required specialist artifacts must exist
2. all required specialist artifacts must be recent enough for downstream use
3. if a required upstream artifact is stale, fail early
4. do not silently aggregate across mismatched cycle windows

This should reuse the shared artifact-alignment service rather than custom ad hoc checks.

## Real-Time Compatibility

This design must remain valid after the future Polygon real-time upgrade.

What changes later:

1. latency penalties shrink
2. `near_term` becomes more trustworthy
3. `intraday` may become a future active ensemble lane

What does not change:

1. lane-local aggregation
2. range as conviction modifier, not directional vote
3. temporal alignment requirements
4. deterministic ensemble rules

## Phase 1 Non-Goals

1. do not implement portfolio allocation logic here
2. do not mix execution timing into the ensemble
3. do not let range expectation generate bullish/bearish direction
4. do not let the LLM override ensemble math

## Recommended Next Implementation Order

1. add `ensemble_v1` to the registry
2. implement a symbol-scope runner that loads aligned specialist artifacts
3. aggregate `near_term` and `swing_term` independently
4. apply range modulation to confidence
5. emit payload-level quality audit and diagnostics
6. then audit before wiring the dashboard to it
