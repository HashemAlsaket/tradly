from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from tradly.config import get_model_registry_entry
from tradly.models.calibration import audit_model_artifact
from tradly.models.market_regime import Bar, REGIME_SYMBOLS, build_market_regime_row
from tradly.paths import get_repo_root
from tradly.services.time_context import get_time_context


def _latest_bar_by_day(rows: list[tuple]) -> dict[str, list[Bar]]:
    grouped: dict[str, dict[object, tuple[int, Bar]]] = defaultdict(dict)
    for symbol, ts_utc, close, volume, data_status, correction_seq in rows:
        if close is None:
            continue
        bar = Bar(ts_utc=ts_utc, close=float(close), volume=volume, data_status=data_status)
        current = grouped[symbol].get(ts_utc)
        if current is None or correction_seq > current[0]:
            grouped[symbol][ts_utc] = (correction_seq, bar)

    out: dict[str, list[Bar]] = {}
    for symbol, by_day in grouped.items():
        bars = [item[1] for item in by_day.values()]
        bars.sort(key=lambda item: item.ts_utc)
        out[symbol] = bars
    return out


def main() -> int:
    repo_root = get_repo_root()
    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        return 1

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 2

    registry_entry = get_model_registry_entry("market_regime_v1")
    time_ctx = get_time_context()

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in REGIME_SYMBOLS)
        bar_rows = conn.execute(
            f"""
            SELECT symbol, ts_utc, close, volume, data_status, correction_seq
            FROM market_bars
            WHERE timeframe = '1d'
              AND symbol IN ({placeholders})
            ORDER BY symbol, ts_utc
            """,
            list(REGIME_SYMBOLS),
        ).fetchall()
        latest_macro_ts_utc = conn.execute("SELECT MAX(ts_utc) FROM macro_points").fetchone()[0]
        latest_macro_news_ts_utc = conn.execute(
            """
            SELECT MAX(interpreted_at_utc)
            FROM news_interpretations
            WHERE bucket = 'macro'
            """
        ).fetchone()[0]
    finally:
        conn.close()

    if not bar_rows:
        print("market_regime_v1_failed:no_market_bars")
        return 3

    bars_by_symbol = _latest_bar_by_day(bar_rows)
    row = build_market_regime_row(
        bars_by_symbol=bars_by_symbol,
        now_utc=time_ctx.now_utc,
        latest_macro_ts_utc=latest_macro_ts_utc,
        latest_macro_news_ts_utc=latest_macro_news_ts_utc,
    )
    quality_audit = audit_model_artifact([row]).to_dict()

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "market_regime_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "regime_symbols": list(REGIME_SYMBOLS),
            "bar_symbol_count": len(bars_by_symbol),
            "latest_macro_ts_utc": row["evidence"].get("latest_macro_ts_utc"),
            "latest_macro_news_ts_utc": row["evidence"].get("latest_macro_news_ts_utc"),
        },
        "quality_audit": quality_audit,
        "rows": [row],
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"signal_direction={row['signal_direction']}")
    print(f"score_normalized={row['score_normalized']}")
    print(f"confidence_score={row['confidence_score']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
