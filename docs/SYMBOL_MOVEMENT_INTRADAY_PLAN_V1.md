# symbol_movement intraday integration plan v1

## Goal

Make `symbol_movement_v1` intraday-aware without replacing its daily base thesis.

This phase is only for:

- `src/tradly/models/symbol_movement.py`
- `src/tradly/pipeline/run_symbol_movement.py`
- focused `symbol_movement` tests

It does **not** change:

- `ensemble_v1`
- `recommendation_v1`
- `recommendation_review_v1`
- dashboard rendering

## Current problem

Right now `symbol_movement_v1` uses:

- daily symbol bars
- daily market regime overlay
- daily sector movement overlay

It does **not** use:

- recent `1m` symbol tape
- latest symbol snapshot

That means the symbol layer is still blind to:

- short-horizon confirmation
- short-horizon failure
- symbol-vs-sector intraday divergence

## Core design

Keep the current daily symbol score as the base.

Add a bounded intraday symbol overlay that answers:

- is this symbol confirming its own daily thesis right now?
- is it outperforming or lagging its sector intraday?
- is it diverging from the market/sector overlays?

## DB reads

### Keep

Keep the current `1d` read for all model symbols.

### Add

Add `1m` bars for all model symbols using:

- last `2` calendar days

Add latest snapshot per symbol from `market_snapshots`.

## New data structures

Reuse:

- `IntradayBar`
- `SnapshotPoint`

from `market_regime.py`

Do not invent a third intraday structure.

## New overlay fields

Each symbol row gets `evidence.intraday_overlay`.

Required fields:

- `symbol_intraday_overlay_state`
- `symbol_intraday_overlay_freshness`
- `latest_intraday_ts_utc`
- `latest_snapshot_ts_utc`
- `symbol_intraday_return_pct`
- `sector_intraday_return_pct`
- `market_intraday_return_pct`
- `relative_intraday_vs_sector_pct`
- `relative_intraday_vs_market_pct`
- `symbol_snapshot_change_pct`

## Overlay states

Allowed values:

- `confirming`
- `mixed`
- `fading`
- `unavailable`

Freshness values:

- `minute_confirmed`
- `snapshot_only`
- `unavailable`

## Feature logic

### 1. Symbol intraday return

Compute:

- latest `1m` close vs latest daily close

Fallback:

- latest snapshot change pct

### 2. Relative intraday strength vs sector

For stock names:

- compare symbol intraday return to the sector proxy intraday return

If sector minute data is unavailable:

- compare to sector snapshot-only move if available

### 3. Relative intraday strength vs market

Compare symbol intraday return to market proxy return.

Use:

- `SPY` for broad market
- `QQQ` when the symbol is in Technology / Communication Services or clearly growth-sensitive

Keep the first implementation simple:

- use `SPY` for all

### 4. Confirmation rules

`confirming`

- symbol intraday move agrees with the symbol daily signal
- and symbol is not lagging sector/market badly

`fading`

- symbol intraday move conflicts with the symbol daily signal
- or symbol is materially lagging sector/market

`mixed`

- neither clear confirming nor clear fading

`unavailable`

- no usable minute bars and no usable snapshot

## Score integration

Keep daily score dominant.

Add:

- `intraday_overlay_score` in a bounded small range, for example `[-20, +20]`

Final:

- `score_raw = daily_raw_score + intraday_overlay_score`

The intraday overlay should matter more here than in regime/sector, because symbol confirmation is more local and actionable.

## Why codes

Add:

- `symbol_intraday_confirming`
- `symbol_intraday_fading`
- `symbol_intraday_mixed`
- `symbol_snapshot_confirming`
- `symbol_snapshot_fading`
- `symbol_intraday_overlay_unavailable`
- `symbol_lagging_sector_intraday`
- `symbol_outperforming_sector_intraday`
- `symbol_lagging_market_intraday`
- `symbol_outperforming_market_intraday`

Do not delete the current daily why-codes.

## Coverage behavior

Do not fail the symbol row just because intraday is unavailable.

Rules:

- daily-only symbol rows still run
- overlay becomes `unavailable`
- coverage can remain based on the existing daily logic

This keeps medium-horizon behavior intact.

## Runner changes

Update `run_symbol_movement.py` to:

1. keep current `1d` read
2. add `1m` read for model symbols for last `2` days
3. add latest snapshot read for model symbols
4. pass both into `build_symbol_movement_rows`

Update `input_summary` to include:

- `intraday_symbol_count`
- `snapshot_symbol_count`

## Delete old shit

Delete these assumptions from the implementation:

1. symbol tactical posture can be derived from `1d` bars alone
2. sector and market overlays are sufficient without symbol tape confirmation
3. snapshot/minute data are only freshness concerns rather than symbol evidence

Do **not** delete:

- daily trend logic
- liquidity logic
- volatility logic
- sector and market overlay logic

## Required tests

Add focused tests for:

1. symbol overlay unavailable with daily-only inputs
2. bullish daily setup + positive intraday confirmation -> `confirming`
3. bullish daily setup + intraday weakness -> `fading`
4. snapshot-only confirmation path
5. sector-relative intraday underperformance path
6. market-relative intraday underperformance path
7. score remains bounded and daily still dominates

## Audit after implementation

After code lands:

1. rerun `symbol_movement_v1`
2. inspect live output
3. confirm `evidence.intraday_overlay` exists
4. confirm overlay freshness is truthful
5. rerun:
   - `ensemble`
   - `recommendation`
   - `recommendation_review`
   - `recommendation_scorecard`
   - `recommendation_scorecard_history`
6. audit recommendation mix changes

## Success condition

This step succeeds when:

- `symbol_movement_v1` still works on daily-only inputs
- it now uses `1m` and snapshots when available
- the output clearly distinguishes:
  - daily symbol thesis
  - intraday symbol confirmation / fading
- downstream recommendation changes can be traced back to the new symbol overlay
