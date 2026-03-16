# market_regime intraday integration plan v1

## Goal

Make `market_regime_v1` intraday-aware without breaking the existing medium-horizon daily thesis logic.

This phase is only for:

- `src/tradly/models/market_regime.py`
- `src/tradly/pipeline/run_market_regime.py`
- tests for `market_regime_v1`

It does **not** update:

- `sector_movement_v1`
- `symbol_movement_v1`
- `ensemble_v1`
- `recommendation_v1`
- `recommendation_review_v1`
- dashboard rendering

## Current problem

Right now `market_regime_v1` reads only `1d` bars for:

- `SPY`
- `QQQ`
- `VIXY`
- `TLT`
- `IEF`
- `SHY`

That means the DB and freshness layers know about:

- `1m` bars
- snapshots

but the actual regime signal math ignores them.

The current old assumption we are removing is:

- tactical regime quality can be inferred from `1d` bars alone

## Scope of change

We will keep the current daily regime logic as the base regime state.

We will add a deterministic intraday overlay on top of it.

The final regime output should reflect two layers:

1. daily thesis state
2. intraday tape confirmation or rejection

## DB reads

### Existing daily read

Keep the current `1d` query for `REGIME_SYMBOLS`.

### New minute-bar read

Add a `1m` query for the same `REGIME_SYMBOLS`.

Required window:

- last `2` calendar days of `1m` bars

Reason:

- enough to capture the prior session close and current extended-hours tape
- small enough to stay cheap and deterministic

### New snapshot read

Add a read from `market_snapshots` for the same `REGIME_SYMBOLS`.

Use the latest available snapshot per symbol.

## Data structures

### Keep

Keep the existing daily `Bar` structure for `1d` history.

### Add

Add two new internal structures in `market_regime.py`:

- `IntradayBar`
- `SnapshotPoint`

Suggested fields:

`IntradayBar`

- `ts_utc`
- `close`
- `volume`
- `data_status`

`SnapshotPoint`

- `as_of_utc`
- `last_trade_price`
- `prev_close`
- `change_pct`
- `day_vwap`
- `market_status`
- `data_status`

No raw quote/trade archive logic belongs here.

## New intraday feature block

Add a deterministic intraday overlay computed from:

- latest `1m` tape
- latest snapshot

### Feature families

1. broad tape direction

- `spy_intraday_return_pct`
- `qqq_intraday_return_pct`

Computed from:

- latest `1m` close vs prior-session daily close

2. fear / stress overlay

- `vixy_intraday_return_pct`

Computed from:

- latest `1m` close vs prior-session daily close

3. duration / safety overlay

- `tlt_intraday_return_pct`
- `ief_intraday_return_pct`
- `shy_intraday_return_pct`

Computed from:

- latest `1m` close vs prior-session daily close

4. snapshot confirmation

- `spy_snapshot_change_pct`
- `qqq_snapshot_change_pct`
- `vixy_snapshot_change_pct`

Use snapshot values when present.

5. tape breadth proxy

Deterministic agreement count over:

- `SPY`
- `QQQ`
- `VIXY`
- `TLT`

This is not true breadth, but it is enough for a first regime overlay.

## New overlay classifications

Add an intraday overlay classification separate from `signal_direction`.

Allowed values:

- `supportive`
- `mixed`
- `risk_off`
- `unavailable`

### Classification rules

`supportive`

- `SPY` intraday positive
- `QQQ` intraday positive or flat
- `VIXY` intraday not rising sharply

`risk_off`

- `SPY` intraday negative
- `QQQ` intraday negative
- `VIXY` intraday rising

`mixed`

- anything in between

`unavailable`

- insufficient fresh `1m` and no usable snapshot

This stays deterministic and intentionally simple in phase 1.

## Score integration

Do **not** replace the current daily raw score.

Add an intraday overlay adjustment to the current score.

Suggested approach:

- compute `daily_raw_score` exactly as today
- compute `intraday_overlay_score` in a small bounded range, for example `[-4, +4]`
- final:
  - `score_raw = daily_raw_score + intraday_overlay_score`

This prevents intraday tape from completely overriding the daily thesis while still making it matter.

## Why codes

Add new regime why-codes only when the overlay is available.

New codes:

- `intraday_tape_supportive`
- `intraday_tape_risk_off`
- `intraday_tape_mixed`
- `snapshot_confirms_risk_on`
- `snapshot_confirms_risk_off`
- `intraday_overlay_unavailable`

Do not remove the current daily why-codes.

## Evidence payload

Add an `intraday_overlay` block to `evidence`.

Required fields:

- `intraday_overlay_state`
- `latest_intraday_ts_utc`
- `latest_snapshot_ts_utc`
- `spy_intraday_return_pct`
- `qqq_intraday_return_pct`
- `vixy_intraday_return_pct`
- `tlt_intraday_return_pct`
- `ief_intraday_return_pct`
- `shy_intraday_return_pct`
- `spy_snapshot_change_pct`
- `qqq_snapshot_change_pct`
- `vixy_snapshot_change_pct`

Keep all current daily evidence fields.

## Coverage / freshness behavior

Do not hard-fail the model just because intraday is unavailable.

Rules:

- if daily evidence is good and intraday is unavailable:
  - model still runs
  - overlay state becomes `unavailable`
  - quality can degrade to `thin_evidence`
- if daily evidence is good and intraday exists:
  - model runs with overlay

This is important because the medium-horizon system must still function overnight and on weekends.

## Pipeline changes

Update `run_market_regime.py` to:

1. keep the current `1d` bar query
2. add a `1m` bar query for `REGIME_SYMBOLS`
3. add a latest-snapshot query for `REGIME_SYMBOLS`
4. pass both into `build_market_regime_row()`

New helper functions to add in `run_market_regime.py`:

- `_latest_bar_by_day(...)`
  - keep existing daily helper
- `_recent_intraday_bars(...)`
- `_latest_snapshots_by_symbol(...)`

## Delete old shit

These assumptions should be deleted from the implementation when we do the code change:

1. `market_regime_v1` only needs `1d` bars for tactical posture
2. the latest daily bar alone is enough to express near-term regime state
3. snapshot and `1m` data are merely external freshness concepts rather than regime inputs

We are **not** deleting:

- daily regime logic
- macro freshness checks
- medium-horizon lanes

## Tests required before accept

Add focused tests for:

1. regime runs with daily-only data and `intraday_overlay_state = unavailable`
2. supportive intraday tape produces:
   - positive overlay score
   - `intraday_tape_supportive`
3. risk-off intraday tape produces:
   - negative overlay score
   - `intraday_tape_risk_off`
4. mixed tape produces:
   - `intraday_tape_mixed`
5. snapshot values are used when minute bars are thin but snapshot is present
6. `why_code` remains stable and bounded
7. daily score still dominates when intraday signals are small

## Audit required after implementation

After code lands:

1. rerun `market_regime_v1`
2. inspect the output artifact
3. confirm new evidence fields exist
4. confirm daily and intraday timestamps are both present
5. confirm the regime output changes in sensible ways under:
   - overnight off-hours
   - live premarket
   - live risk-off tape

## Success condition

This step is successful when:

- `market_regime_v1` still works with daily-only evidence
- `market_regime_v1` now uses `1m` and snapshot data when available
- the output artifact clearly distinguishes:
  - daily thesis
  - intraday tape overlay
- no other model layer has been changed yet
