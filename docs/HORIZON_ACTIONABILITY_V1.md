# Horizon Actionability V1

## Purpose

This document defines how Tradly should describe actionability by explicit holding period.

Operator-facing horizon labels should be:

1. `1to3d`
2. `1to2w`
3. `2to6w`

The goal is to stop collapsing all actionability into one global state when different horizons can have materially different trust, freshness adequacy, and evidence quality.

## Core Principle

The dashboard should separate:

1. global system health
2. horizon-specific actionability

That means the system can honestly say:

1. `1to3d`: research only
2. `1to2w`: actionable
3. `2to6w`: not yet supported

without forcing one sweeping label to stand in for all timeframes.

## Canonical Operator Horizons

Phase 1 operator horizons:

1. `1to3d`
2. `1to2w`
3. `2to6w`

Internal alias mapping may continue in code for transition:

1. `near_term` -> `1to3d`
2. `swing_term` -> `1to2w`
3. `position_term` -> `2to6w`

Rule:

1. internal aliases are implementation detail
2. dashboard and operator-facing summaries should prefer explicit horizon labels

## Actionability States

Each horizon should have its own state:

1. `actionable`
2. `research_only`
3. `blocked`
4. `not_supported`

Definitions:

1. `actionable`
   - the system has enough aligned evidence quality to support decisions for that horizon
2. `research_only`
   - the system can still provide useful directional context for that horizon, but evidence is not strong enough for action
3. `blocked`
   - required artifacts, alignment, or freshness contracts are broken for that horizon
4. `not_supported`
   - the horizon exists in taxonomy but is not yet active as an operator-facing decision lane

## Phase 1 Horizon Intent

### `1to3d`

Use for:

1. short tactical moves
2. next-few-session positioning
3. higher sensitivity to recent price and catalyst flow

Expected sensitivity:

1. highest sensitivity to delayed market data
2. highest sensitivity to thin symbol-news coverage
3. highest sensitivity to weak market-context support

Default interpretation:

1. this horizon should be the first to drop from `actionable` to `research_only`

### `1to2w`

Use for:

1. swing trades
2. multi-session to multi-week positioning
3. broader regime, sector leadership, symbol movement, and range framing

Expected sensitivity:

1. more tolerant of delayed daily bars than `1to3d`
2. more tolerant of slightly older macro context
3. can remain actionable even when the shorter horizon is only research-grade

Default interpretation:

1. this is the strongest phase-1 actionability horizon for the current delayed-data system

### `2to6w`

Use for:

1. broader position framing
2. medium-duration backdrop reasoning
3. slower-moving regime and range context

Expected sensitivity:

1. least sensitive to delayed daily bars among the active taxonomy horizons
2. likely more tolerant of slower macro cadence

Phase 1 status:

1. this horizon is supported in taxonomy and by `range_expectation_v1`
2. it is not yet a fully active operator lane for the dashboard
3. default dashboard state should be `not_supported` until broader directional specialist support exists

## Phase 1 Evidence Expectations

### `1to3d`

To be `actionable`, the system should generally have:

1. aligned specialist + ensemble artifacts
2. no global blockers
3. ensemble support that is not `thin_evidence` for this horizon
4. symbol-level evidence that is not overly dependent on thin news or weak market context

To be `research_only`, common reasons include:

1. thin symbol-news support
2. thin upstream market-context support
3. delayed data that is still usable for interpretation but not strong enough for action

### `1to2w`

To be `actionable`, the system should generally have:

1. aligned specialist + ensemble artifacts
2. no global blockers
3. acceptable movement + regime + range support
4. no major ensemble quality failure for this horizon

To be `research_only`, common reasons include:

1. still-thin ensemble support
2. sparse symbol-news coverage if news remains an active ensemble input
3. unresolved cross-model conflicts

### `2to6w`

Phase 1 actionability:

1. `not_supported`

Reason:

1. the current directional stack is not yet explicitly lane-complete at `2to6w`
2. `range_expectation_v1` alone is not enough to make this a true operator lane

## Dashboard Contract

The dashboard should present:

1. global system health
2. `1to3d` state
3. `1to2w` state
4. `2to6w` state

Suggested interpretation pattern:

1. `Global: usable`
2. `1to3d: research_only`
3. `1to2w: actionable`
4. `2to6w: not_supported`

Rules:

1. global `blocked` still overrides all horizons
2. if the system is not globally blocked, horizon states should be shown independently
3. one weak horizon must not automatically flatten a stronger horizon to the same state

## Delayed-To-Realtime Compatibility

This contract should survive the real-time data upgrade.

What changes later:

1. `1to3d` should become more often actionable
2. intraday may later become a supported operator lane
3. freshness penalties may relax for shorter horizons

What does not change:

1. actionability must remain horizon-specific
2. the dashboard should not collapse all horizons into one label
3. global health and horizon actionability remain separate concepts
