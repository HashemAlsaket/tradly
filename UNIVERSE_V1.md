# Universe V1

## Scope
- A lean starter universe for daily and delayed-intraday decisioning.
- Includes large-cap stocks and broad ETFs for reliable liquidity.

## Source of Truth
- Seed file: `db/seeds/universe_v1.csv`
- Loaded into DuckDB table: `instruments`

## Status Convention
- `investable`
- `review_required`
- `probably_not_halal`
- `not_halal`

## Liquidity Rule (V1)
- Keep symbols with strong daily liquidity profiles (large-cap equities and major ETFs).
- Remove symbols with repeated execution friction.

## Change Process
1. Edit `db/seeds/universe_v1.csv`.
2. Run `python scripts/setup/load_universe.py`.
3. Verify counts and status distribution in `instruments`.
