# Modeling V0

## Scope
Modeling V0 is intentionally lean and deterministic:
- `M_price`: trend + pullback score from daily bars only.
- `M_risk`: volatility + liquidity + delayed-feed penalty.
- `M_regime`: macro regime penalty from `SPY`, `QQQ`, `VIXY` (VIX proxy), `TLT`, `IEF`, `SHY`.
- `investability_gate`: final action cannot be `Buy/Strong Buy` for non-investable statuses.
- Stateless execution regime tagging (no portfolio sizing, no account allocation math).

No LLM-generated numeric scoring is used.

## Strictness (Hard Rule)
- No silent defaults for missing critical inputs.
- No symbol-level skipping during scoring.
- If any active symbol has missing/invalid critical data, the run fails fast and produces no action sheet.
- Critical fields include:
  - at least 61 daily bars per active symbol
  - positive close values
  - non-null positive volume for last 20 bars
  - valid `data_status` in `{REALTIME, DELAYED}`
  - valid investability status in universe table

## Inputs
- `market_bars` with `timeframe='1d'`
- `instruments` (`symbol`, `halal_flag`, `sector`, `active`)
- Context symbols present in `instruments` for regime calculation:
  - `SPY`, `QQQ`, `VIXY`, `TLT`, `IEF`, `SHY`

## M_price
For each symbol with at least 60 daily bars:
- `r20 = close_t / close_t-20 - 1`
- `r60 = close_t / close_t-60 - 1`
- `pullback_pct = max(0, (high20 - close_t) / high20)`
- `bounce_3d = close_t / close_t-3 - 1`

Subscores:
- `trend_score = clamp(50 + 900*r20 + 400*r60, 0, 100)`
- `pullback_score = clamp(55 + 350*pullback_pct + 500*bounce_3d, 0, 100)`

Combine:
- `M_price = 0.60 * trend_score + 0.40 * pullback_score`

## M_risk (Penalty)
- Daily return volatility over 20 bars: `vol20_ann = std(ret_1d_20) * sqrt(252)`
- Avg dollar volume over 20 bars: `adv20 = mean(close * volume)`
- Feed status penalty if latest bar is not `REALTIME`: `+10`

Penalty terms:
- `vol_penalty = clamp((vol20_ann - 0.25) / 0.35 * 40, 0, 40)`
- `liq_penalty = 0` if `adv20 >= 1e9`, else `clamp((1e9 - adv20) / 1e9 * 25, 0, 25)`
- `feed_penalty = 10` when status is delayed/unknown, else `0`

Combine:
- `M_risk = vol_penalty + liq_penalty + feed_penalty`

## Final Score
- `score = clamp(M_price - M_risk - M_regime, 0, 100)`

## M_regime (Penalty)
Macro context is computed once per run and applied to all scored symbols.
- Weak market breadth:
  - `SPY 20d return < -3%` -> `+8`
  - `QQQ 20d return < -4%` -> `+6`
- Volatility stress (`VIXY` proxy):
  - `VIXY > 25` -> `+6`
  - `VIXY > 35` -> `+10`
  - `5d VIXY change > +15%` -> `+4`
- Flight-to-safety:
  - `TLT 20d return > +4%` while `SPY 20d return < 0` -> `+4`

`M_regime` is clamped to `[0, 20]`.

## Action Mapping (pre-investability)
- `score >= 75` -> `Strong Buy`
- `60 <= score < 75` -> `Buy`
- `45 <= score < 60` -> `Watch`
- `30 <= score < 45` -> `Trim`
- `score < 30` -> `Exit`

## Horizon Mapping
- `score >= 75 and vol20_ann < 0.30` -> `weekly`
- `score >= 60` -> `daily`
- `45 <= score < 60` -> `hourly`
- else -> `daily`

## Investability Gate
Apply `apply_investability_gate(proposed_action, halal_flag)` as final action override.
- If blocked for investability, final action is forced to `Watch`.

## Output
`data/runs/<YYYY-MM-DD>/model_v0_actions.json` containing:
- run metadata
- sorted action rows (highest score first)
- reasons/metrics per symbol
- deterministic tranche regime plan per symbol (`execution_regime`)

## LLM Final Review Layer
- Script: `scripts/pipeline/review_actions.py`
- Input: latest `model_v0_actions.json`
- Output: `model_v0_reviewed.json`
- Hard rule: LLM is interpretation-only and must not perform calculations.
