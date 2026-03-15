from __future__ import annotations

import json
from collections import defaultdict
from zoneinfo import ZoneInfo

from tradly.config import get_model_registry_entry
from tradly.models.calibration import audit_model_artifact
from tradly.models.range_expectation import DailyBar, build_range_expectation_rows
from tradly.paths import get_repo_root
from tradly.services.market_calendar import previous_trading_day
from tradly.services.time_context import get_time_context

MARKET_TZ = ZoneInfo("America/New_York")


def _latest_bar_by_day(rows: list[tuple]) -> dict[str, list[DailyBar]]:
    grouped: dict[str, dict[object, tuple[int, DailyBar]]] = defaultdict(dict)
    for symbol, ts_utc, open_, high, low, close, volume, data_status, correction_seq in rows:
        if None in {open_, high, low, close}:
            continue
        bar = DailyBar(
            ts_utc=ts_utc,
            open=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=float(volume) if volume is not None else None,
            data_status=data_status,
        )
        current = grouped[symbol].get(ts_utc)
        if current is None or correction_seq > current[0]:
            grouped[symbol][ts_utc] = (correction_seq, bar)

    out: dict[str, list[DailyBar]] = {}
    for symbol, by_day in grouped.items():
        bars = [item[1] for item in by_day.values()]
        bars.sort(key=lambda item: item.ts_utc)
        out[symbol] = bars
    return out


def main() -> int:
    repo_root = get_repo_root()
    db_path = repo_root / "data" / "tradly.duckdb"
    scope_manifest_path = repo_root / "data" / "manual" / "universe_runtime_scopes.json"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        return 1
    if not scope_manifest_path.exists():
        print(f"scope manifest missing: {scope_manifest_path}")
        return 2

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 3

    registry_entry = get_model_registry_entry("range_expectation_v1")
    scope_manifest = json.loads(scope_manifest_path.read_text(encoding="utf-8"))
    model_symbols = scope_manifest["scopes"]["model_symbols"]
    if not isinstance(model_symbols, list) or not model_symbols:
        print("range_expectation_v1_failed:model_symbols_missing")
        return 4

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in model_symbols)
        bar_rows = conn.execute(
            f"""
            SELECT symbol, ts_utc, open, high, low, close, volume, data_status, correction_seq
            FROM market_bars
            WHERE timeframe = '1d'
              AND symbol IN ({placeholders})
            ORDER BY symbol, ts_utc
            """,
            model_symbols,
        ).fetchall()
        metadata_rows = conn.execute(
            f"""
            SELECT symbol, asset_type, sector
            FROM instruments
            WHERE symbol IN ({placeholders})
            ORDER BY symbol
            """,
            model_symbols,
        ).fetchall()
    finally:
        conn.close()

    if not bar_rows:
        print("range_expectation_v1_failed:no_market_bars")
        return 5

    bars_by_symbol = _latest_bar_by_day(bar_rows)
    symbol_metadata = {
        str(symbol): {"asset_type": str(asset_type), "sector": str(sector)}
        for symbol, asset_type, sector in metadata_rows
    }

    time_ctx = get_time_context()
    expected_min_market_date = previous_trading_day(time_ctx.now_utc.astimezone(MARKET_TZ).date())
    rows = build_range_expectation_rows(
        bars_by_symbol=bars_by_symbol,
        symbol_metadata=symbol_metadata,
        model_symbols=sorted(str(symbol).strip().upper() for symbol in model_symbols),
        now_utc=time_ctx.now_utc,
        expected_min_market_date=expected_min_market_date,
    )
    quality_audit = audit_model_artifact(rows).to_dict()

    coverage_counts: dict[str, int] = {}
    blocked_symbols: list[str] = []
    for row in rows:
        coverage_state = str(row.get("coverage_state", "unknown"))
        coverage_counts[coverage_state] = coverage_counts.get(coverage_state, 0) + 1
        if coverage_state == "insufficient_evidence":
            blocked_symbols.append(str(row.get("scope_id", "")))

    input_status = "ready" if not blocked_symbols else "thin_evidence"

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "range_expectation_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "model_symbol_count": len(model_symbols),
            "bar_symbol_count": len(bars_by_symbol),
        },
        "input_audit": {
            "status": input_status,
            "required_model_symbols": sorted(str(symbol).strip().upper() for symbol in model_symbols),
            "present_bar_symbols": sorted(bars_by_symbol),
            "missing_bar_symbols": sorted(symbol for symbol in model_symbols if symbol not in bars_by_symbol),
        },
        "row_audit": {
            "coverage_counts": coverage_counts,
            "blocked_symbols": blocked_symbols,
        },
        "quality_audit": quality_audit,
        "rows": rows,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"range_rows={len(rows)}")
    print(f"quality_audit_status={quality_audit['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
