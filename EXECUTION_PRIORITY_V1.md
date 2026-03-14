# Tradly Execution Priority (V1)

Status key: `pending` | `in_progress` | `done`

1. Proper seeding baseline (prices/news/interpretations/macro) - `done`
2. Hard freshness + data-quality gates (auto-block on stale/broken inputs) - `in_progress`
3. Cycle reliability (scheduled runs, retries, manifests, failure visibility) - `pending`
4. Intraday news override engine (high-impact event posture changes) - `pending`
5. Session/calendar awareness (market hours + earnings/macro event timing) - `pending`
6. Risk controls (exposure caps, daily loss cap, concentration limits) - `pending`
7. Broker/manual execution sync + journaling (what was executed and outcomes) - `pending`
8. Model versioning and auditability (reproducible decisions) - `pending`
9. Dashboard clarity upgrades (base vs override vs final, recency in CST) - `pending`
10. Security hardening (key rotation, leak checks, secret hygiene) - `pending`
