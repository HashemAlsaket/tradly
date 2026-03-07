# Trading Spec V1

## 1) Objective and Scope
- System role: integrated trading intelligence platform (research, scoring, audit, and dashboards).
- Execution mode: all orders are placed manually in Robinhood by the portfolio owner.
- Strategy constraints: long-only equities/ETFs; no shorting; no options.
- Goal: stream high-quality buy/sell decisions with clear rationale, horizon qualifiers, confidence provenance, and live risk context.

## 2) Broker State Awareness (Mandatory)
- The system must always maintain the latest known Robinhood portfolio state:
  - account arrangement, cash, buying power, positions, holdings weights, realized/unrealized PnL, and open orders.
- Any recommendation run is blocked if broker state is stale beyond the freshness SLA.
- Every action recommendation is portfolio-aware (position size, concentration, cash impact, overlap with existing holdings).

## 3) Action Labels with Horizon Qualifiers
- Strong Buy
- Buy
- Watch
- Trim
- Exit

Each non-Watch action must include horizon qualifiers:
- `buy_til`: condition/date until the buy thesis remains valid.
- `hold_til`: condition/date until hold remains valid.
- `sell_by` or `sell_at`: condition/level/date for de-risking or exit.
- `invalidation`: price and narrative invalidation rules.

## 4) Decision Model (Mixed, Practical)
The model outputs an action package per symbol with score, rationale, horizon qualifiers, and risk impact.

### 4.1 Component Signals (V1 Weights)
- Technical confidence interval signal: 35%
- News headline signal: 25%
- Sector strength/rotation signal: 20%
- Volatility/regime (Brownian-inspired) signal: 15%
- Liquidity/slippage sanity penalty: 5%

### 4.2 Score-to-Action Mapping (V1)
- 80-100: Strong Buy
- 65-79: Buy
- 45-64: Watch
- 30-44: Trim
- 0-29: Exit

## 5) Calculation and Provenance Policy (Hard Rule)
- All numeric calculations must be executed by deterministic code paths (services/scripts/functions), never by LLM/VLM free-form estimation.
- LLM/VLM/STT systems may orchestrate, interpret unstructured inputs, and generate narrative summaries.
- Every reported number must include metadata:
  - `computed_by`: code module and version
  - `input_snapshot`: dataset version/hash
  - `timestamp`
  - `method`
- If any value is heuristic or qualitative ("gut feeling"), it must be explicitly tagged `qualitative_non_numeric` and must not be mixed into numeric scoring unless converted by an explicit coded method.

## 6) Risk Defaults (Aggressive but Controlled)
- Max single position size: 20% of portfolio market value.
- Max sector concentration: 40% of portfolio market value.
- Max new capital deployed per day: 25% of available cash.
- Minimum cash buffer target: 10% (can drop to 5% only on exceptional conviction clusters).
- Drawdown pause rule: at 12% rolling drawdown, pause new buys for 2 market sessions.

## 7) Universe Rule (Generally Halal, Practical)
- Prefer companies with clear primary businesses in acceptable sectors.
- Prefer liquid US-listed stocks and broad/sector ETFs.
- Exclude obvious primary exposure to prohibited industries.
- Borderline symbols are tagged `Needs Review` and capped at half-size.
- Universe list is versioned and reviewed monthly.

## 8) Dashboard Requirement (Core Product, Not Optional)
Dashboards are first-class outputs and must be mobile-first.

### 8.1 Required Mobile Widgets
- Top Picks: symbol, action, confidence, horizon qualifiers, delta vs prior run.
- Portfolio Mirror: broker positions, weights, cash, buying power, PnL.
- Risk Status: concentration, drawdown, rule breaches, pause state.
- Action Queue: Buy/Trim/Exit with `buy_til/hold_til/sell_by/sell_at` and invalidation.
- News and Sector Impact Feed: why scores changed.
- Model Health: last successful run, data freshness, failed checks, audit status.

### 8.2 UX Rules
- Phone-first performance and layout.
- Color plus text labels for every state.
- One-line "why now" for each actionable item.
- No broker API order execution in V1.

## 9) AI-First Processing Policy
- The system should heavily use LLM/VLM/STT for understanding, summarization, classification, and operator interaction.
- Regex-based and hard-coded string search logic is disallowed for decision-critical understanding paths.
- Structured extraction should use model-based extraction and/or schema-constrained parsers.

## 10) Audit Agents (Mandatory)
At least three agent roles run on every cycle:
- Data Audit Agent: validates freshness, completeness, and integrity.
- Calculation Audit Agent: verifies numeric outputs were code-computed with reproducible inputs.
- Decision Audit Agent: checks recommendation consistency with risk, horizon, and portfolio constraints.

If any audit fails:
- mark run as `not action-safe`
- block actionable recommendations
- surface failure details on dashboard

## 11) Update Cadence (V1)
- Pre-market update (US)
- Post-close update (US)
- Event-driven updates on major earnings/news
- Broker-state sync on schedule and before each recommendation cycle

## 12) Logging and Review
- Recommendation record:
  - timestamp, symbol, action, horizon qualifiers, confidence, component scores, rationale, invalidation, provenance metadata.
- Execution journal record:
  - timestamp, intended action, manual fill price, position size, follow-up notes.
- Weekly review:
  - hit rate, average move, MAE/MFE, rule violations, audit failure count.

## 13) Immediate Next Steps
1. Create `DATA_CONTRACT_V1.md` including broker-state schema and freshness SLAs.
2. Define computation service boundaries so all numbers are code-generated with provenance.
3. Define audit agent interfaces and failure gates.
4. Build first mobile dashboard from required widgets.
