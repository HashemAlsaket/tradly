# Trading Guardrails V1

Date locked: March 8, 2026 (America/Chicago)

## 1) Non-Negotiable Safety Rules
- Manual execution only: system cannot place or route broker orders.
- Long-only equities/ETFs only.
- No shorting.
- No options.
- If any safety gate fails, actionable output must be blocked.

## 2) Account Mode (Required Config)
Set one explicit runtime mode:
- `ACCOUNT_MODE=CASH`
- `ACCOUNT_MODE=MARGIN`

If mode is unknown, system state = `not action-safe`.

## 3) PDT Guardrail (Margin/Limited Margin)
Applies only when `ACCOUNT_MODE=MARGIN`.

### 3.1 Rule Inputs
- `day_trades_5d`: count of completed day trades in rolling 5 trading days.
- `total_trades_5d`: total completed trades in same window.
- `portfolio_equity`: prior-day closing equity.

### 3.2 Hard Blocks
- Block opening any new intraday round-trip if:
  - `day_trades_5d >= 3` AND projected new round-trip would make `day_trades_5d >= 4`.
- Block all intraday trades if account is flagged PDT and `portfolio_equity < 25000`.

### 3.3 Warning States
- `pdt_risk_high` if `day_trades_5d == 2`.
- `pdt_risk_critical` if `day_trades_5d == 3`.
- Surface warning on dashboard and action queue.

## 4) Cash-Account Settlement Guardrail
Applies only when `ACCOUNT_MODE=CASH`.

### 4.1 Hard Blocks
- Block buys that require unsettled proceeds beyond allowed settled cash.
- Block sequences that can create freeriding risk.

### 4.2 Required Fields
- `settled_cash`
- `unsettled_cash`
- `cash_available_to_trade`
- `next_settlement_timestamps`

If any field is stale/missing, state = `not action-safe`.

## 5) Frequency and Cadence Guardrail
Default operating cadence:
- `PRIMARY_CADENCE=DAILY`
- `SECONDARY_MONITOR_CADENCE=HOURLY`

### 5.1 Intraday Budget
- `MAX_INTRADAY_ACTIONS_PER_DAY=1` (default)
- `MAX_INTRADAY_ROUND_TRIPS_5D=2` (default risk budget)

System may propose additional intraday opportunities as `Watch` only when budget is exhausted.

## 6) Horizon Policy
Every non-Watch recommendation must include:
- `horizon_type`: `intraday` | `hourly` | `daily` | `weekly` | `multi_week`
- `buy_til`
- `hold_til`
- `sell_by` or `sell_at`
- `invalidation_price`
- `invalidation_narrative`

Missing any required horizon field -> recommendation invalid.

## 7) Session and Liquidity Guardrail
- Outside regular market hours, only allow recommendations if spread/liquidity checks pass.
- If spread exceeds configured threshold, downgrade to `Watch`.
- If liquidity check fails, block action and flag `session_liquidity_block`.

## 8) Data Freshness Guardrail
Before any actionable run:
- broker state freshness <= 120 seconds
- market snapshot freshness <= 60 seconds (when market open)
- news freshness <= 5 minutes for event-driven runs

Any breach -> `not action-safe`.

## 9) Audit and Provenance Guardrail
Actionable output requires all pass:
- Data Audit = pass
- Calculation Audit = pass
- Decision Audit = pass

All numeric fields must include provenance:
- `computed_by`
- `input_snapshot`
- `calculated_at`
- `method`

## 10) Actionability State Machine
- `research_only`: insights allowed, no action labels requiring execution.
- `action_safe`: all guardrails pass; actionable labels allowed.
- `blocked`: one or more hard rules failed.

Transitions:
- Any hard failure -> `blocked`
- Any missing critical field -> `blocked`
- All checks pass -> `action_safe`

## 11) Dashboard Minimum Safety Widgets
Must always show:
- account mode (`cash` or `margin`)
- pdt usage meter (`day_trades_5d / 3 safe limit`)
- intraday budget remaining
- settled vs unsettled cash
- current system state (`research_only`, `action_safe`, `blocked`)
- blocking reason list (if blocked)

## 12) Initial Defaults (Conservative)
- `MAX_INTRADAY_ACTIONS_PER_DAY=1`
- `MAX_INTRADAY_ROUND_TRIPS_5D=2`
- `BLOCK_NEW_INTRADAY_AT_DAY_TRADES_5D=3`
- `BROKER_STATE_SLA_SECONDS=120`
- `MARKET_SNAPSHOT_SLA_SECONDS=60`
- `NEWS_SLA_SECONDS=300`

## 13) Implementation Sequence
1. Add guardrail config object and validator.
2. Implement account-mode detector and required field checks.
3. Implement PDT counter service and blocker.
4. Implement cash-settlement blocker.
5. Wire guardrail outputs into actionability state machine.
6. Expose all safety counters in mobile dashboard.
