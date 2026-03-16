# Trading Calendar Context V1

Trading-calendar context is now a first-class system primitive.

## Why

Weekend and holiday handling must not be treated as generic staleness.

We need to distinguish:

- market closed normally
- market holiday
- pre-market / market-hours / after-hours
- unexpected stale market bars

That distinction affects:

- runtime freshness
- horizon usability
- short-horizon execution framing
- dashboard wording

## Canonical Calendar Table

`market_calendar`

One row per date with:

- `calendar_date`
- `day_of_week`
- `day_name`
- `is_weekend`
- `is_market_holiday`
- `is_trading_day`
- `market_calendar_state`
- `last_cash_session_date`
- `next_cash_session_date`

`market_calendar_state` values in v1:

- `trading_day`
- `weekend`
- `holiday`

## Stamped Calendar Fields

The following tables now carry calendar context directly:

- `market_bars`
- `news_events`
- `news_interpretations`
- `news_pull_usage`
- `macro_points`

Stamped fields:

- `calendar_date`
- `day_of_week`
- `day_name`
- `is_weekend`
- `is_market_holiday`
- `is_trading_day`
- `market_calendar_state`
- `last_cash_session_date`

## Refresh / Backfill Process

`tradly.ops.refresh_market_calendar_context`

This operation:

1. builds / refreshes the `market_calendar` dimension
2. backfills calendar fields into stamped tables
3. runs in the active cycle before model execution

This makes calendar context part of the normal system path rather than an optional post-processing step.

## Runtime Freshness Output

Runtime freshness now surfaces calendar-aware metadata including:

- `market_session_state`
- `latest_daily_bar_market_date`
- `expected_min_market_date`
- `market_bar_status`
- current day-of-week / holiday / trading-day flags
- `last_cash_session_date`

This lets downstream surfaces say:

- `Market closed for weekend`
- instead of only `61h old`

## Propagation Order

V1 propagation intentionally focuses on:

1. data/runtime metadata
2. audit semantics
3. dashboard/operator interpretation

It does **not** yet rewrite every model’s scoring logic.

That later step can use the same calendar fields without changing the underlying contract again.
