# Modeling Framework V1

## Purpose

This document defines the next-step modeling architecture for Tradly.

The goal is to replace the current single broad scoring path with a specialist-model stack:

1. deterministic specialist models
2. deterministic ensemble aggregation
3. LLM final decision review

This document is a design/spec only. It does not imply that all models below are already implemented.

## Design Principles

1. Deterministic models do calculations.
2. The LLM does not calculate prices, probabilities, or allocations.
3. Every model must declare its horizon and confidence.
4. Every model must be auditable by symbol, sector, and input set.
5. The ensemble must remain deterministic.
6. The LLM is the final judgment/explanation layer, not the math layer.

Reference:

1. symbol-scope deterministic aggregation is specified in [ENSEMBLE_V1.md](/Users/hashemalsaket/Desktop/workspace/tradly/docs/ENSEMBLE_V1.md)

## Scope Layers

The framework operates on three scope levels:

1. Market scope
2. Sector scope
3. Symbol scope

Each model must declare exactly one primary scope.

## Canonical Taxonomy

The framework must use one explicit taxonomy shared by the registry, data layer, and model layer.

### Canonical sectors

1. `Technology`
2. `Healthcare`
3. `Financial Services`
4. `Industrials`
5. `Consumer Defensive`
6. `Communication Services`
7. `Energy`
8. `Consumer Cyclical`
9. `Basic Materials`
10. `Real Estate`
11. `Utilities`
12. `Macro`
13. `ETF`

### Canonical sector ETF proxies

These ETF proxies are the official inputs for sector-level movement models.

| Canonical sector | Sector ETF proxy |
| --- | --- |
| `Technology` | `XLK` |
| `Financial Services` | `XLF` |
| `Energy` | `XLE` |
| `Healthcare` | `XLV` |
| `Industrials` | `XLI` |
| `Basic Materials` | `XLB` |
| `Utilities` | `XLU` |
| `Real Estate` | `XLRE` |
| `Consumer Defensive` | `XLP` |
| `Consumer Cyclical` | `XLY` |
| `Communication Services` | `XLC` |

### Canonical market and context proxies

These are official non-sector context inputs:

1. broad market: `SPY`, `QQQ`, `IWM`, `DIA`, `VTI`
2. rates / duration: `TLT`, `IEF`, `SHY`
3. vol / risk: `VIXY`, `I:VIX`
4. semis context: `SOXX`, `SMH`, `XSD`
5. Asia / international context: `EWJ`, `EWT`, `FXI`, `KWEB`
6. macro / commodity / FX context: `GLD`, `USO`, `UUP`, `KRE`, `ARKK`

## Horizon Taxonomy

All models must emit one or more supported horizons.

Supported horizons:

1. `intraday`
2. `1to3d`
3. `1to2w`
4. `2to6w`

Notes:

1. Current daily-bar system is strongest at `1to3d` and `1to2w`.
2. `intraday` models should remain limited until real intraday data is in place.
3. Models may emit a best horizon plus optional secondary horizons.

## Horizon Lanes

The supported horizon taxonomy above is necessary but not sufficient.

The system must also recognize explicit horizon lanes so it does not force one directional score and one confidence number to stand in for all timeframes.

Phase 1 active lanes:

1. `near_term`
   - canonical horizon: `1to3d`
2. `swing_term`
   - canonical horizon: `1to2w`

Deferred lane:

1. `position_term`
   - canonical horizon: `2to6w`

Rules:

1. confidence must be interpreted in lane context, not as a timeless global certainty claim
2. freshness adequacy must be evaluated per lane
3. slower-moving inputs like macro may be weak for `near_term` while still acceptable for `swing_term`
4. downstream models must inherit upstream lane quality, not just upstream freshness timestamps
5. current implementations may continue emitting `horizon_primary` and `horizon_secondary` during transition, but future directional outputs should become lane-aware

Reference:

1. see [HORIZON_LANES_V1.md](/Users/hashemalsaket/Desktop/workspace/tradly/docs/HORIZON_LANES_V1.md)
2. operator-facing actionability is specified in [HORIZON_ACTIONABILITY_V1.md](/Users/hashemalsaket/Desktop/workspace/tradly/docs/HORIZON_ACTIONABILITY_V1.md)

## Output Contract

Every deterministic model should produce rows shaped like:

```json
{
  "model_id": "symbol_price_v1",
  "model_scope": "symbol",
  "scope_id": "NVDA",
  "horizon_primary": "1to3d",
  "horizon_secondary": ["1to2w"],
  "signal_direction": "bullish|bearish|neutral",
  "signal_strength": 0.0,
  "confidence_score": 0,
  "confidence_label": "low|medium|high",
  "coverage_state": "sufficient_evidence|thin_evidence|insufficient_evidence",
  "score_raw": 0.0,
  "score_normalized": 0.0,
  "why_code": ["trend_up", "vol_ok"],
  "evidence": {
    "feature_name": "value"
  },
  "as_of_utc": "2026-03-14T00:00:00+00:00",
  "data_freshness_ok": true
}
```

Rules:

1. `confidence_score` is deterministic and `0-100`.
2. `coverage_state` is required for every deterministic model row.
3. `score_normalized` should be on a shared scale usable by the ensemble.
4. `why_code` should contain short machine-readable reasons, not prose.
5. `evidence` should contain compact feature values that support debugging and UI inspection.
6. `evidence` must include explicit latency metadata for market-data-driven models:
   - `data_status`
   - `market_data_latency_minutes`
   - `latency_class`
   - `freshness_score`
7. No model may silently treat `DELAYED` market data as equivalent to `REALTIME`.

## Shared Calibration Rules

All deterministic models must use one shared calibration contract before their outputs are eligible for the ensemble.

### Score normalization

Rules:

1. every model may compute a model-specific `score_raw`
2. every model must then map `score_raw` into `score_normalized` on `[-100, 100]` using a shared normalization utility
3. direct multiplication-based scaling embedded inside individual models must be treated as transitional only and should be removed as models are retrofitted
4. clipping to `-100` or `100` should be rare; repeated clipping is an audit failure
5. `score_normalized = 0` must mean either:
   - truly neutral signal
   - or insufficient evidence, with the distinction made explicit by `coverage_state`

Phase 1 normalization guidance:

1. use bounded transforms that reduce saturation risk
2. preserve sign and ordinal ranking of `score_raw`
3. reserve `|score_normalized| >= 80` for unusually strong signals
4. reserve `|score_normalized| >= 95` for extreme and rare signals only

Phase 1 normalization formula:

```text
score_normalized
= round(
    100 * tanh(score_raw / raw_scale)
  , 4
  )
```

Rules:

1. `raw_scale` is a positive model-specific calibration constant declared in code for the model
2. `raw_scale` must be chosen so that typical non-extreme rows for the model family land well inside `[-80, 80]`
3. the same `raw_scale` must be reused across runs for the same model version unless the model version changes
4. do not apply a second model-specific multiplier after the shared transform
5. after the transform, clamp to `[-100, 100]` only as a final safety guard

### Latency-aware freshness

All market-data-driven models must separate:

1. recency: whether the bar belongs to the expected market date or session
2. latency: whether the feed is `REALTIME` or `DELAYED`
3. freshness: the model-level result after applying horizon-aware recency and latency rules

Phase 1 caps:

1. `REALTIME`:
   - no latency cap from feed status alone
2. `DELAYED` with `intraday` horizon:
   - cap `freshness_score` at `25`
   - cap `confidence_score` at `25`
   - emit `coverage_state = insufficient_evidence`
3. `DELAYED` with `1to3d` horizon:
   - cap `freshness_score` at `70`
4. `DELAYED` with `1to2w` horizon:
   - cap `freshness_score` at `85`
5. `DELAYED` with `2to6w` horizon:
   - cap `freshness_score` at `90`

The exact assumed delay in minutes must be emitted in `evidence.market_data_latency_minutes`.

## Model Registry

Every model should be registered in code with:

1. `model_id`
2. `scope`
3. `required_inputs`
4. `supported_horizons`
5. `output_schema_version`
6. `directional_role`
7. `ensemble_inclusion`
8. `confidence_inclusion`
9. `base_weight_default`
10. `minimum_coverage_state`

Suggested registry shape:

```json
{
  "model_id": "sector_news_v1",
  "scope": "sector",
  "required_inputs": ["news_interpretations", "news_events", "news_symbols"],
  "supported_horizons": ["intraday", "1to3d", "1to2w", "2to6w"],
  "output_schema_version": 1,
  "directional_role": "directional",
  "ensemble_inclusion": true,
  "confidence_inclusion": true,
  "base_weight_default": 0.15,
  "minimum_coverage_state": "thin_evidence"
}
```

## Instantiation Rules

Model families are not enough. Runtime must instantiate models from explicit universe data.

### Market-scope instantiation

Instantiate exactly one `market_regime_v1` row per run timestamp.

Primary scope id:

1. `US_BROAD_MARKET`

### Sector-scope instantiation

Instantiate one `sector_movement_v1` and one `sector_news_v1` row per canonical sector below:

1. `Technology`
2. `Healthcare`
3. `Financial Services`
4. `Industrials`
5. `Consumer Defensive`
6. `Communication Services`
7. `Energy`
8. `Consumer Cyclical`
9. `Basic Materials`
10. `Real Estate`
11. `Utilities`

Rules:

1. sector models must not be instantiated for `ETF` or `Macro`
2. each sector row must declare its ETF proxy in `evidence.sector_proxy`
3. each sector row must declare the symbols contributing to the sector rollup in `evidence.member_symbols`

### Symbol-scope instantiation

Instantiate one `symbol_movement_v1` and one `symbol_news_v1` row for every symbol in `model_symbols`.

Rules:

1. every symbol model row must include `evidence.symbol_sector`
2. every symbol model row must include the canonical sector ETF proxy used for overlay
3. symbols in `Macro` or `ETF` may still receive symbol models if they are in `model_symbols`, but must use the market proxy path rather than a stock-sector rollup

### Source-of-truth rule

Instantiation must derive from:

1. `/Users/hashemalsaket/Desktop/workspace/tradly/data/manual/universe_registry.json`
2. `/Users/hashemalsaket/Desktop/workspace/tradly/data/manual/universe_runtime_scopes.json`

No model family may invent its own sector map or symbol list.

## Phase 1 Core Models

These are the first models to implement.

### 1. Market Regime Model

`model_id`: `market_regime_v1`

Purpose:

1. Detect broad risk-on / risk-off posture.
2. Score market pressure and regime stability.

Inputs:

1. `SPY`, `QQQ`, `VIXY`, `TLT`, `IEF`, `SHY`
2. macro points
3. latest interpreted macro news summary

Outputs:

1. regime direction
2. regime penalty / support
3. confidence score
4. recommended horizons
5. `score_raw`
6. `score_normalized`
7. `coverage_state`
8. `why_code`
9. `evidence`

Primary horizons:

1. `1to3d`
2. `1to2w`

### 2. Sector Movement Model

`model_id`: `sector_movement_v1`

Purpose:

1. Measure sector-relative price strength/weakness.
2. Compare sector ETFs against broad market.

Inputs:

1. sector ETFs:
   - `XLK`, `XLF`, `XLE`, `XLV`, `XLI`, `XLB`, `XLU`, `XLRE`, `XLP`, `XLY`, `XLC`
2. broad market ETFs:
   - `SPY`, `QQQ`, `IWM`, `DIA`, `VTI`

Outputs:

1. sector direction
2. relative-strength score
3. confidence score
4. sector proxy used
5. horizon vote
6. `score_raw`
7. `score_normalized`
8. `coverage_state`
9. `why_code`
10. `evidence`

Primary horizons:

1. `1to3d`
2. `1to2w`

### 3. Sector News Model

`model_id`: `sector_news_v1`

Purpose:

1. Convert sector-specific interpreted news into sector pressure signals.
2. Detect positive/negative sector attention and trend reinforcement.

Inputs:

1. `news_events`
2. `news_symbols`
3. `news_interpretations`
4. sector/context ETF mappings
5. registry-derived member symbols for the sector

Outputs:

1. sector news direction
2. evidence density
3. confidence score
4. symbol coverage ratio
5. `score_raw`
6. `score_normalized`
7. `coverage_state`
8. `why_code`
9. `evidence`

Primary horizons:

1. `intraday`
2. `1to3d`

### 4. Symbol Movement Model

`model_id`: `symbol_movement_v1`

Purpose:

1. Score symbol-specific price action quality.
2. Detect strong setups, weak setups, and breakdowns.

Inputs:

1. daily bars for the symbol
2. market regime overlay
3. sector movement overlay

Core features:

1. trend quality
2. pullback quality
3. volatility penalty
4. liquidity / ADV check
5. relative strength vs sector and market

Outputs:

1. direction
2. normalized score
3. confidence score
4. preferred horizon
5. sector-relative score
6. market-relative score
7. `coverage_state`
8. `why_code`
9. `evidence`

Primary horizons:

1. `1to3d`
2. `1to2w`

### 5. Symbol News Model

`model_id`: `symbol_news_v1`

Purpose:

1. Score symbol-specific interpreted news.
2. Capture bullish/bearish headline pressure and catalyst quality.

Inputs:

1. symbol-linked `news_symbols`
2. `news_events`
3. `news_interpretations`

Core features:

1. recent article count
2. interpreted impact direction
3. confidence mix
4. freshness
5. catalyst concentration

Outputs:

1. symbol news direction
2. news confidence score
3. evidence density score
4. coverage state
5. `score_raw`
6. `score_normalized`
7. `why_code`
8. `evidence`

Primary horizons:

1. `intraday`
2. `1to3d`

## Phase 2 Models

These come after the Phase 1 core stack is working.

### 6. Event Risk Model

`model_id`: `event_risk_v1`

Purpose:

1. Adjust signals for earnings and known event risk.

Inputs:

1. earnings watchlist
2. days-to-earnings
3. recent volatility
4. news event intensity

Outputs:

1. event penalty/support
2. best horizon around event
3. confidence score

### 7. Macro News Model

`model_id`: `macro_news_v1`

Purpose:

1. Convert macro news flow into broad market pressure signals.

Inputs:

1. macro bucket interpreted news
2. rates ETFs
3. vol proxy

Outputs:

1. macro risk-on / risk-off direction
2. confidence score

### 8. Range / Expected Move Model

`model_id`: `range_expectation_v1`

Purpose:

1. Estimate expected move and reasonable bands.
2. Support trim/re-entry/invalidation planning.

Inputs:

1. daily volatility
2. trailing return variance
3. horizon length

Candidate methods:

1. rolling volatility bands
2. simple Brownian-motion expected move approximation
3. confidence interval banding

Outputs:

1. expected move band
2. upper/lower confidence band
3. horizon-aware uncertainty score
4. `coverage_state`
5. `why_code`
6. `evidence`

Phase 1 contract:

1. Phase 1 method is `rolling_volatility_plus_brownian_v1`
2. use daily close-to-close realized volatility over the trailing `20` sessions as the primary sigma input
3. use Brownian-motion approximation for horizon scaling:

```text
expected_move_pct = sigma_daily * sqrt(horizon_days)
```

4. where:
   - `sigma_daily = stdev(daily_returns_last_20)`
   - `horizon_days = 1` for `intraday`
   - `horizon_days = 2` for `1to3d`
   - `horizon_days = 7` for `1to2w`
   - `horizon_days = 20` for `2to6w`
5. emit:
   - `score_normalized = 0`
   - `signal_direction = neutral`
   - `coverage_state` based on volatility-history sufficiency
6. `range_expectation_v1` is a supporting model, not a directional vote
7. required evidence fields:
   - `latest_close`
   - `sigma_daily`
   - `expected_move_pct`
   - `upper_band_1sigma`
   - `lower_band_1sigma`
   - `upper_band_2sigma`
   - `lower_band_2sigma`
   - `horizon_days`
8. confidence for `range_expectation_v1` should reflect history sufficiency, volatility stability, and freshness of the underlying bar set

### 9. Intraday Execution / VWAP Model

`model_id`: `intraday_execution_vwap_v1`

Status:

1. deferred until true intraday market data exists

Purpose:

1. improve entry/trim/re-entry execution
2. support anchored VWAP and session execution logic

Inputs:

1. intraday bars
2. intraday volume
3. anchored/session VWAP features

Outputs:

1. execution quality score
2. intraday entry/trim timing guidance

## Confidence Rules

Deterministic confidence should reflect:

1. evidence density
2. feature agreement
3. data freshness
4. signal stability
5. scope quality

Suggested deterministic confidence labels:

1. `low`: `0-39`
2. `medium`: `40-69`
3. `high`: `70-100`

Confidence must not be a free-form narrative field in deterministic models.

Confidence is not a proxy for simple data presence. A model with fresh inputs but weak or sparse signal structure must not produce near-max confidence.

Deterministic confidence score formula:

```text
confidence_score
= round(
    0.25 * evidence_density_score
  + 0.25 * feature_agreement_score
  + 0.20 * freshness_score
  + 0.20 * stability_score
  + 0.10 * coverage_score
  )
```

Where all component scores are normalized to `0-100`.

Interpretation:

1. `evidence_density_score`: amount of usable evidence for this model
2. `feature_agreement_score`: whether the model's component features align
3. `freshness_score`: how current the underlying inputs are for the model horizon
4. `stability_score`: whether the signal is stable vs noisy
5. `coverage_score`: whether expected scope data is sufficiently present

Additional confidence rules:

1. fresh data alone must not produce `confidence_score >= 70`
2. models with fewer than three independent informative features must cap `confidence_score` at `85`
3. models with only one informative directional feature must cap `confidence_score` at `65`
4. `thin_evidence` rows must cap `confidence_score` at `49`
5. `insufficient_evidence` rows must cap `confidence_score` at `25`
6. if `signal_strength < 0.20`, cap `confidence_score` at `60`
7. if delayed-data policy applies, latency caps are enforced after the base formula
8. `confidence_score >= 90` should be rare and should require:
   - `coverage_state = sufficient_evidence`
   - no delayed-material freshness violation for the active horizon
   - multiple informative features aligned
   - no quality-audit failure

Definitions:

1. an `informative feature` is a model input-derived feature that contributes directly to directional or uncertainty judgment for the row
2. a feature is not informative if it only reports metadata or gating state, such as:
   - freshness only
   - coverage only
   - symbol identity
   - timestamp presence
3. `independent informative features` are informative features that are not simple restatements of the same underlying quantity at nearly identical construction
4. examples:
   - `relative_r20` and `relative_r60` count as two informative features but are only moderately independent
   - `r20` and `relative_r20` do not count as independent
   - `raw_news_count` alone is not directional; interpreted positive/negative pressure counts as informative
5. when uncertainty exists, models should under-count independence rather than over-count it

## Missing-Data Rules

Models must degrade explicitly when evidence is weak. Missing inputs must not silently become neutral.

### Standard coverage states

Every model row must emit one of:

1. `sufficient_evidence`
2. `thin_evidence`
3. `insufficient_evidence`

### Required behavior

1. If a required input source is absent, emit `signal_direction = neutral`, `coverage_state = insufficient_evidence`, and cap `confidence_score` at `25`.
2. If a model has partial but thin evidence, emit `coverage_state = thin_evidence` and cap `confidence_score` at `49`.
3. If a model emits `insufficient_evidence`, the ensemble must downweight it heavily rather than treating it as a full neutral vote.
4. If sector ETF data is stale for the selected horizon, only the affected sector model confidence is penalized.
5. If symbol news coverage is zero in the lookback window, `symbol_news_v1` must emit `insufficient_evidence`, not implied neutrality.
6. If a symbol lacks sufficient bar history, `symbol_movement_v1` must emit `insufficient_evidence`.
7. Models must surface missing-data reasons in `why_code`, for example:
   - `news_missing_30d`
   - `bars_insufficient_history`
   - `sector_proxy_stale`
   - `macro_input_stale`

## Model Output Quality Audit

Passing schema validation is not enough. Every deterministic model artifact must also pass output-quality audit checks.

### Required artifact-level checks

1. saturation check
2. confidence clustering check
3. score-confidence consistency check
4. delayed-data honesty check
5. required-input honesty check

### Failure rules

An artifact fails quality audit if any of the following are true:

1. more than `15%` of rows clip to `|score_normalized| >= 95` without explicit extreme-event reasons in `why_code`
2. more than `40%` of rows have `confidence_score >= 90`
3. for artifacts with `5` or more rows, more than `50%` of rows share the exact same `confidence_score`
4. any row with `data_status = DELAYED` omits `market_data_latency_minutes` or `latency_class`
5. any row with `latency_class = delayed_material` and active horizon `intraday` emits `coverage_state = sufficient_evidence`
6. any row with `coverage_state = insufficient_evidence` emits a non-neutral direction without explicit exception rules
7. any row with very weak signal magnitude and very high confidence violates the consistency rule:
   - if `|score_normalized| < 10`, `confidence_score` must be `< 70`
   - if `|score_normalized| < 5`, `confidence_score` must be `< 55`

Applicability rules:

1. artifact-level ratio and clustering checks apply only to artifacts with `5` or more rows
2. single-row artifacts, such as `market_regime_v1`, must still pass:
   - delayed-data honesty check
   - required-input honesty check
   - score-confidence consistency check

### Audit outputs

Each model artifact should preserve compact quality-audit outputs when feasible:

1. `quality_audit.status = pass|fail`
2. `quality_audit.failure_reasons`
3. `quality_audit.summary`

## Ensemble Layer

The ensemble should aggregate specialist outputs into one deterministic symbol-level view.

Suggested ensemble inputs per symbol:

1. market regime output
2. sector movement output when the symbol belongs to a canonical stock sector
3. sector news output when the symbol belongs to a canonical stock sector
4. symbol movement output
5. symbol news output
6. event risk output
7. range/expected move output

ETF and Macro symbol rule:

1. symbols whose canonical sector is `ETF` or `Macro` do not require `sector_movement_v1` or `sector_news_v1`
2. for those symbols, the ensemble uses:
   - `market_regime_v1`
   - `symbol_movement_v1`
   - `symbol_news_v1`
   - optional later ETF/context-specific specialist models
3. implementation must not invent synthetic sector rows for `ETF` or `Macro`
4. ETF/Macro symbols must renormalize active directional base weights before confidence adjustment
5. renormalization rule:

```text
renormalized_base_weight_i
= base_weight_i / sum(base_weight_j for all active directional models present for the symbol)
```

6. use `renormalized_base_weight_i` in place of `base_weight_i` for ETF/Macro symbols in all later ensemble steps

Suggested ensemble outputs:

```json
{
  "symbol": "NVDA",
  "ensemble_score": 61.5,
  "ensemble_confidence_score": 74,
  "ensemble_confidence_label": "high",
  "horizon_primary": "1to3d",
  "horizon_secondary": ["1to2w"],
  "model_votes": {
    "bullish": 4,
    "bearish": 2,
    "neutral": 1
  },
  "model_conflicts": [
    "sector_news_bearish_vs_symbol_movement_bullish"
  ],
  "component_scores": {
    "market_regime_v1": -4.0,
    "sector_movement_v1": 6.5,
    "sector_news_v1": -1.5,
    "symbol_movement_v1": 8.0,
    "symbol_news_v1": 3.0
  },
  "why_code": [
    "trend_strong",
    "sector_supportive",
    "macro_penalty_active"
  ]
}
```

Deterministic ensemble must be explicit and reproducible.

### Step 1: normalize component scores

Every specialist model must emit:

1. `score_normalized` on `[-100, 100]`
2. `confidence_score` on `[0, 100]`

### Step 2: apply base weights

Phase 1 base weights per symbol:

1. `market_regime_v1`: `0.15`
2. `sector_movement_v1`: `0.20`
3. `sector_news_v1`: `0.15`
4. `symbol_movement_v1`: `0.30`
5. `symbol_news_v1`: `0.20`

Phase 2 models add:

1. `event_risk_v1`: `0.10` penalty/boost channel
2. `range_expectation_v1`: no direct directional vote; used for action mapping and confidence penalty
3. `macro_news_v1`: absorbed into market/regime weight unless explicitly split later

For canonical stock-sector symbols in Phase 1:

```text
base_weight_sum = 1.00
```

For ETF/Macro symbols in Phase 1:

```text
active_directional_models = {market_regime_v1, symbol_movement_v1, symbol_news_v1}
```

and the renormalized base weights become:

1. `market_regime_v1`: `0.15 / 0.65 = 0.230769`
2. `symbol_movement_v1`: `0.30 / 0.65 = 0.461538`
3. `symbol_news_v1`: `0.20 / 0.65 = 0.307692`

### Step 3: apply confidence-adjusted weights

For each directional model:

```text
effective_weight = base_weight * (confidence_score / 100)
```

If `coverage_state = insufficient_evidence`:

```text
effective_weight = effective_weight * 0.25
```

If `coverage_state = thin_evidence`:

```text
effective_weight = effective_weight * 0.60
```

### Step 4: compute ensemble score

```text
ensemble_score
= sum(score_normalized * effective_weight)
  / sum(effective_weight)
```

If the denominator is zero, emit:

1. `ensemble_score = 0`
2. `ensemble_confidence_score = 0`
3. `action_candidate = Abstain`

### Step 5: compute conflict penalty

Conflict exists when at least two high-confidence directional models disagree on sign.

High-confidence conflict rule:

1. model A confidence `>= 60`
2. model B confidence `>= 60`
3. one score is `>= +20`
4. the other is `<= -20`

Penalty:

```text
conflict_penalty = min(20, 5 * conflict_pair_count)
```

Then:

```text
ensemble_confidence_score
= round(weighted_mean_component_confidence - conflict_penalty)
```

Where:

```text
weighted_mean_component_confidence
= sum(confidence_score * effective_weight)
  / sum(effective_weight)
```

Rules:

1. include only directional models that are active for the symbol
2. exclude supporting models with no direct directional vote, including `range_expectation_v1`
3. if the denominator is zero, set `weighted_mean_component_confidence = 0`

Then clamp:

```text
ensemble_confidence_score = min(100, max(0, ensemble_confidence_score))
```

### Step 6: choose horizon

Each directional model gets one horizon vote weighted by its `effective_weight`.

Horizon selection rules:

1. primary horizon = highest weighted vote total
2. secondary horizon = second-highest weighted vote total if at least `25%` of the primary total
3. if symbol and sector horizons disagree, symbol scope wins
4. if market regime conflicts with symbol horizon, lower ensemble confidence by `5`
5. if two horizons tie exactly, prefer the shorter horizon
6. if all horizon vote totals are zero, emit:
   - `horizon_primary = 1to3d`
   - `horizon_secondary = []`
7. if only one directional model has `coverage_state = sufficient_evidence`, use that model's horizon directly

### Step 7: preserve audit trail

The ensemble row must preserve:

1. component scores
2. component effective weights
3. conflict pairs
4. conflict penalty
5. horizon vote totals

## Action Mapping Layer

The ensemble output should not directly become prose.

First it maps to deterministic action candidates:

1. `Buy`
2. `Watch`
3. `Trim`
4. `Exit`
5. `Abstain`

Action mapping should consider:

1. ensemble score
2. ensemble confidence
3. event risk
4. regime penalty
5. range model outputs
6. horizon alignment

This deterministic layer should also emit:

1. reference price
2. invalidation level
3. buy/hold/sell timing windows
4. execution regime

Phase 1 deterministic action thresholds:

1. `Buy` if:
   - `ensemble_score >= 25`
   - `ensemble_confidence_score >= 60`
   - no hard event-risk block
2. `Watch` if:
   - `ensemble_score` is between `5` and `24.999`
   - or confidence is below the `Buy` threshold while score remains positive
3. `Trim` if:
   - `ensemble_score` is between `-24.999` and `-5`
   - and the symbol is not in explicit `Abstain`
4. `Exit` if:
   - `ensemble_score <= -25`
   - and `ensemble_confidence_score >= 55`
5. `Abstain` if:
   - denominator of active directional evidence is zero
   - or `ensemble_confidence_score < 35`
   - or all directional models are `insufficient_evidence`

Phase 1 adjustment rules:

1. if `event_risk_v1` later emits a hard penalty, cap `Buy` at `Watch`
2. if market regime is strongly risk-off and symbol horizon is shorter than market horizon, lower action by one step toward caution
3. `range_expectation_v1` does not change direction directly, but may:
   - lower confidence when uncertainty is extreme
   - widen invalidation distance
   - shorten timing windows
4. action mapping must remain deterministic and threshold-based

### Portfolio boundary

This layer is allowed to emit only:

1. action candidate
2. reference / invalidation / timing
3. execution regime

It must not emit target allocation or size decisions. Those belong to the later portfolio layer.

## LLM Final Decision Layer

The LLM sits above the deterministic stack.

Inputs to the LLM should include:

1. specialist model outputs
2. ensemble output
3. deterministic action candidate
4. horizon outputs
5. key evidence
6. conflict summary

LLM outputs:

1. `llm_action`
2. `llm_decision_confidence_score`
3. `llm_confidence_label`
4. `llm_rationale`
5. `llm_based_on_provided_evidence`
6. `llm_calculation_performed`

Rules:

1. `llm_calculation_performed` must remain `false`
2. the LLM confidence score is judgment, not deterministic probability
3. the LLM must not invent inputs not present in the payload

## Portfolio Layer

Portfolio-aware sizing is explicitly out of scope for the current model file, but should be designed next.

Future outputs should include:

1. current weight
2. target weight
3. delta weight
4. execution tranche guidance

This should sit after deterministic action mapping and before final dashboard rendering.

Future portfolio contract boundary:

1. deterministic models and ensemble produce conviction, risk, and horizon
2. action mapping produces candidate action plus execution levels
3. portfolio layer converts that into:
   - target weight
   - current weight
   - delta weight
   - tranche plan
4. LLM final review may explain the sizing decision but must not calculate it

## Implementation Order

Recommended order:

1. Define model registry in code
2. Implement `market_regime_v1`
3. Implement `sector_movement_v1`
4. Implement `sector_news_v1`
5. Implement `symbol_movement_v1`
6. Implement `symbol_news_v1`
7. Build deterministic ensemble layer
8. Update LLM review payload to consume specialist + ensemble outputs
9. Add portfolio sizing layer
10. Add deferred intraday/VWAP execution model when intraday data exists

## Current Gaps Relative to This Spec

Current repo status relative to this target:

1. explicit runtime scopes: partially done
2. broad registry coverage: done
3. specialist deterministic model stack: not yet done
4. ensemble layer: not yet done
5. portfolio-aware sizing: not yet done
6. intraday / VWAP modeling: not yet done

## Success Criteria

This framework is successful when:

1. every live symbol has auditable specialist model outputs
2. every final action can be traced back to deterministic component evidence
3. the LLM final review is evidence-grounded and easy to inspect
4. horizons and confidence are explicit
5. dashboard can show fast action plus detailed model decomposition
