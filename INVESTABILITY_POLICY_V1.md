# Investability Policy V1

## Goal
Allow broad market/news analysis while blocking portfolio actions on symbols that are likely non-halal.

## Statuses
- `investable`: eligible for all actions, including `Strong Buy` and `Buy`.
- `review_required`: analyzable; cannot receive `Strong Buy`/`Buy` until manually approved.
- `probably_not_halal`: analyzable only; cannot be purchased.
- `not_halal`: analyzable only; cannot be purchased.

## Hard Rules
- System may ingest and model any symbol regardless of status.
- Symbols with `probably_not_halal` or `not_halal` must never produce `Strong Buy` or `Buy`.
- If such symbols produce bullish model outputs, final action is forced to `Watch` with reason code `investability_blocked`.
- `review_required` symbols are forced to `Watch` for buy-side proposals with reason code `investability_review_required`.

## Allowed Actions By Status
- `investable`: `Strong Buy`, `Buy`, `Watch`, `Trim`, `Exit`
- `review_required`: `Watch`, `Trim`, `Exit`
- `probably_not_halal`: `Watch`, `Trim`, `Exit`
- `not_halal`: `Watch`, `Trim`, `Exit`

## Data Model Mapping
- `instruments.halal_flag` stores one of:
  - `investable`
  - `review_required`
  - `probably_not_halal`
  - `not_halal`

## Governance
- Universe and statuses are reviewed monthly.
- Any status change requires a dated note in universe change log.
