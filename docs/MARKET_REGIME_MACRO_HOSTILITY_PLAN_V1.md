# Market Regime Macro Hostility Plan V1

## Goal

Strengthen `market_regime_v1` so macro hostility is represented as a first-class deterministic input to the decision stack, not just a broad bearish label or UI warning.

This phase changes **only** `market_regime_v1`.

It does **not** yet change:
- `ensemble_v1`
- `recommendation_v1`
- `recommendation_review_v1`
- dashboard wording

## Current Gap

The model already captures broad regime direction and an intraday overlay, but it still under-expresses *why* the environment is hostile for longs.

The current live issue is:
- the system can say `Macro unstable`
- while the long book still reads too friendly for an oil / inflation / geopolitics-driven tape

That means macro is still not strong or explicit enough as a structured downstream input.

## Design Principle

Do not make macro “smarter” through vague prose.

Instead:
1. derive explicit macro sub-states from deterministic ETF / rate / volatility proxies already in the DB
2. expose those sub-states in `market_regime_v1`
3. derive a stronger top-level macro state from them
4. let downstream layers later consume that structured output

## New Macro Sub-Signals

`market_regime_v1` should add the following deterministic sub-signals:

1. `risk_appetite_state`
   Purpose:
   - detect whether broad equity risk appetite is healthy, unstable, or risk-off

   Primary inputs:
   - `SPY`
   - `QQQ`
   - `IWM`
   - existing daily regime score
   - current intraday overlay state

   Suggested states:
   - `risk_on`
   - `unstable`
   - `risk_off`

2. `rates_pressure_state`
   Purpose:
   - detect whether duration / rates proxies imply macro pressure on growth

   Primary inputs:
   - `TLT`
   - `IEF`
   - `SHY`

   Suggested states:
   - `supportive`
   - `mixed`
   - `pressuring`

3. `energy_stress_state`
   Purpose:
   - detect whether energy leadership is acting like macro stress rather than healthy cyclicality

   Primary inputs:
   - `XLE`
   - broad equity proxies
   - defensive sectors

   Suggested states:
   - `contained`
   - `elevated`
   - `stress`

   Important note:
   - we do **not** have direct Brent / WTI futures here
   - this signal must be framed honestly as an equity-proxy energy-stress measure

4. `defensive_rotation_state`
   Purpose:
   - detect whether defensive sectors are outperforming in a way that implies caution rather than healthy breadth

   Primary inputs:
   - `XLP`
   - `XLU`
   - `XLV`
   - `SPY`
   - `QQQ`
   - `IWM`

   Suggested states:
   - `cyclical_leadership`
   - `mixed`
   - `defensive_leadership`

## Exact Threshold Logic

All threshold logic should be deterministic and based on already-available daily and intraday/snapshot proxy metrics.

### 1. `risk_appetite_state`

Primary daily inputs:
- `SPY`
- `QQQ`
- `IWM`

Primary intraday inputs:
- existing intraday overlay state/freshness

Daily score inputs:
- average of 20-day returns for `SPY`, `QQQ`, `IWM`
- average of 60-day returns for `SPY`, `QQQ`, `IWM`

Suggested daily buckets:
- `risk_on`
  - average `r20 >= +0.02`
  - and average `r60 >= +0.04`
- `risk_off`
  - average `r20 <= -0.02`
  - or average `r60 <= -0.04`
- `unstable`
  - anything in between

Intraday refinement:
- if intraday overlay is `risk_off`, downgrade one step toward `risk_off`
- if intraday overlay is `supportive`, upgrade one step toward `risk_on`
- if intraday overlay is `snapshot_only`, refinement may occur but should carry lower confidence in evidence metadata

### 2. `rates_pressure_state`

Primary daily inputs:
- `TLT`
- `IEF`
- `SHY`

Interpretation:
- weaker duration proxies imply more rates pressure on growth/risk assets

Suggested buckets:
- `supportive`
  - `TLT r20 >= +0.01`
  - and `IEF r20 >= 0.0`
- `pressuring`
  - `TLT r20 <= -0.02`
  - or (`TLT r20 <= -0.01` and `IEF r20 <= -0.005`)
- `mixed`
  - otherwise

### 3. `energy_stress_state`

Primary inputs:
- `XLE`
- broad equity proxy average (`SPY`, `QQQ`, `IWM`)
- defensive rotation state inputs

Important framing:
- this is an **equity-proxy energy stress** signal
- it is **not** direct oil/futures truth

Suggested buckets:
- `stress`
  - `XLE relative r20 vs broad >= +0.03`
  - and broad risk appetite is `unstable` or `risk_off`
- `elevated`
  - `XLE relative r20 vs broad >= +0.015`
  - but not enough for `stress`
- `contained`
  - otherwise

### 4. `defensive_rotation_state`

Primary inputs:
- `XLP`
- `XLU`
- `XLV`
- broad equity proxy average

Suggested buckets:
- `defensive_leadership`
  - average defensive `r20 - broad_r20_avg >= +0.015`
- `cyclical_leadership`
  - average defensive `r20 - broad_r20_avg <= -0.015`
- `mixed`
  - otherwise

## Macro Hostility Score

Add a bounded `macro_hostility_score` that is separate from the existing base regime score.

Suggested bounded range:
- `[-30, +30]`

Suggested contributions:
- `risk_appetite_state`
  - `risk_on = +10`
  - `unstable = 0`
  - `risk_off = -10`
- `rates_pressure_state`
  - `supportive = +6`
  - `mixed = 0`
  - `pressuring = -6`
- `energy_stress_state`
  - `contained = 0`
  - `elevated = -4`
  - `stress = -8`
- `defensive_rotation_state`
  - `cyclical_leadership = +6`
  - `mixed = 0`
  - `defensive_leadership = -6`

Clamp the final score into `[-30, +30]`.

Important rule:
- this score is an overlay on macro interpretation
- it does **not** replace the existing primary regime score
- it should influence interpretation and later downstream logic, not overwhelm the regime base

## Conflict Resolution

When sub-signals conflict, use deterministic precedence:

1. If `risk_appetite_state = risk_off` and at least one of:
   - `rates_pressure_state = pressuring`
   - `energy_stress_state = stress`
   - `defensive_rotation_state = defensive_leadership`
   then `macro_state = risk_off`

2. Else if `risk_appetite_state = risk_on`
   and `rates_pressure_state != pressuring`
   and `defensive_rotation_state != defensive_leadership`
   and `energy_stress_state != stress`
   then `macro_state = risk_on_confirmed`

3. Else `macro_state = macro_unstable`

This keeps the model conservative in mixed environments.

## Fallback And Availability Rules

Availability should be explicit in evidence.

For each sub-signal:
- if required daily proxies are missing, mark that sub-signal as `unavailable`
- if daily inputs are available but intraday inputs are not, use daily logic only
- if only snapshot inputs are available, allow intraday refinement but record that refinement as `snapshot_only`

For top-level `macro_state`:
- if two or more sub-signals are `unavailable`, default to `macro_unstable`
- do not emit `risk_on_confirmed` when proxy coverage is materially incomplete

Add evidence fields:
- `macro_signal_availability`
- `macro_intraday_freshness`
- `macro_conflict_flags`

## New Top-Level Macro State

Add a new explicit macro state in `market_regime_v1`:

- `risk_on_confirmed`
- `macro_unstable`
- `risk_off`

This should be derived from the four sub-signals above plus the existing primary regime direction.

### Suggested derivation logic

`risk_on_confirmed`
- `risk_appetite_state = risk_on`
- `rates_pressure_state != pressuring`
- `defensive_rotation_state != defensive_leadership`
- no strong energy stress

`risk_off`
- `risk_appetite_state = risk_off`
- and at least one of:
  - `rates_pressure_state = pressuring`
  - `energy_stress_state = stress`
  - `defensive_rotation_state = defensive_leadership`

`macro_unstable`
- everything in between
- or conflicting macro sub-signals

This derivation should use the exact conflict-resolution rules above rather than ad hoc interpretation during implementation.

## New Evidence Contract

Add a dedicated block to `market_regime_v1`:

`evidence.macro_hostility`

Fields:
- `macro_state`
- `risk_appetite_state`
- `rates_pressure_state`
- `energy_stress_state`
- `defensive_rotation_state`
- `macro_hostility_score`
- `latest_macro_proxy_ts_utc`
- `daily_proxy_metrics`
- `intraday_proxy_metrics`

The score should be bounded and interpretable, for example:
- negative = more hostile
- positive = more supportive

It should not replace the existing primary regime score.

## Why-Codes

Add explicit regime why-codes such as:
- `macro_risk_appetite_risk_off`
- `macro_risk_appetite_unstable`
- `macro_rates_pressure`
- `macro_energy_stress`
- `macro_defensive_rotation`
- `macro_risk_on_confirmed`
- `macro_hostility_mixed`

These codes should be added in a way that downstream models can consume them later without string gymnastics.

## Data Inputs

Use only data already available in the system:

Daily:
- `SPY`
- `QQQ`
- `IWM`
- `TLT`
- `IEF`
- `SHY`
- `XLE`
- `XLP`
- `XLU`
- `XLV`

Intraday / snapshot:
- same symbols when available

No new provider integrations are part of this phase.

## Scoring Approach

Do **not** rewrite the existing regime score.

Instead:
1. keep the current daily regime base
2. keep the current intraday overlay
3. add a bounded macro-hostility layer
4. derive `macro_state` from the combined macro-hostility view

This macro layer should have its own score contribution, but it must remain auditable and not swamp the whole model.

## Old Assumptions To Delete

Delete the effective assumption that:
- broad market bearishness alone is enough to express macro hostility

Delete the implicit assumption that:
- `Macro unstable` can live primarily as a dashboard phrase rather than a model output

Do **not** delete:
- existing daily regime scoring
- existing intraday overlay
- existing horizon and lane logic

## Tests Required

Add focused tests for:

1. `risk_on_confirmed`
   - broad proxies supportive
   - rates not pressuring
   - defensive rotation absent

2. `macro_unstable`
   - conflicting signals
   - e.g. risk appetite weak but rates mixed and defensives not fully leading

3. `risk_off`
   - weak broad proxies
   - plus rates pressure or defensive leadership

4. `energy_stress_state = stress`
   - strong energy relative behavior with weak broad risk appetite

5. `snapshot_only` macro classification
   - no minute bars
   - snapshot signals still produce honest sub-states without pretending minute confirmation

6. `unavailable`
   - insufficient proxy coverage falls back safely

## Audit Requirements After Implementation

After implementation, audit:

1. `market_regime_v1.json`
   Confirm:
   - new `macro_hostility` block exists
   - new `macro_state` is present
   - why-codes are coherent

2. live classification quality
   Confirm:
   - the current Monday pre-market state classifies as something reasonable
   - likely `macro_unstable` or `risk_off`, not `risk_on_confirmed`

3. no regression in existing regime outputs
   Confirm:
   - runner still passes
   - quality audit still passes
   - existing intraday overlay stays coherent

## Non-Goals

This phase does **not**:
- change recommendation thresholds
- change review promotion policy
- change dashboard wording
- add commodities / futures providers
- add LLM judgment to macro classification

## Next Step After This Phase

Once this regime expansion is implemented and audited, the next rollout step should be:

1. update `recommendation_v1` to consume the stronger macro state
2. then update `recommendation_review_v1` to enforce it on long-side promotion
