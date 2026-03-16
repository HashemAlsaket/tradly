from __future__ import annotations

import json
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from tradly.config import get_model_registry_entry
from tradly.models.calibration import audit_model_artifact
from tradly.models.market_regime import Bar, IntradayBar, SnapshotPoint
from tradly.models.symbol_movement import build_symbol_movement_rows
from tradly.paths import get_repo_root
from tradly.services.artifact_alignment import assess_artifact_alignment
from tradly.services.time_context import get_time_context

MAX_OVERLAY_AGE = timedelta(hours=6)


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


def _recent_intraday_bars(rows: list[tuple]) -> dict[str, list[IntradayBar]]:
    grouped: dict[str, list[IntradayBar]] = defaultdict(list)
    for symbol, ts_utc, close, volume, data_status in rows:
        if close is None:
            continue
        grouped[symbol].append(
            IntradayBar(ts_utc=ts_utc, close=float(close), volume=volume, data_status=data_status)
        )
    for bars in grouped.values():
        bars.sort(key=lambda item: item.ts_utc)
    return dict(grouped)


def _latest_snapshots_by_symbol(rows: list[tuple]) -> dict[str, SnapshotPoint]:
    out: dict[str, SnapshotPoint] = {}
    for symbol, as_of_utc, last_trade_price, prev_close, change_pct, day_vwap, market_status, data_status in rows:
        out[str(symbol)] = SnapshotPoint(
            as_of_utc=as_of_utc,
            last_trade_price=float(last_trade_price) if last_trade_price is not None else None,
            prev_close=float(prev_close) if prev_close is not None else None,
            change_pct=float(change_pct) if change_pct is not None else None,
            day_vwap=float(day_vwap) if day_vwap is not None else None,
            market_status=str(market_status) if market_status is not None else None,
            data_status=str(data_status) if data_status is not None else None,
        )
    return out


def _load_latest_json(runs_dir: Path, pattern: str) -> dict:
    candidates = sorted(runs_dir.glob(pattern))
    if not candidates:
        return {}
    try:
        payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def main() -> int:
    repo_root = get_repo_root()
    db_path = repo_root / "data" / "tradly.duckdb"
    scope_manifest_path = repo_root / "data" / "manual" / "universe_runtime_scopes.json"
    runs_dir = repo_root / "data" / "runs"
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

    market_payload = _load_latest_json(runs_dir, "*/market_regime_v1.json")
    sector_payload = _load_latest_json(runs_dir, "*/sector_movement_v1.json")
    market_rows = market_payload.get("rows")
    sector_rows = sector_payload.get("rows")
    if not isinstance(market_rows, list) or not market_rows:
        print("symbol_movement_v1_failed:market_regime_missing")
        return 4
    if not isinstance(sector_rows, list) or not sector_rows:
        print("symbol_movement_v1_failed:sector_movement_missing")
        return 5

    registry_entry = get_model_registry_entry("symbol_movement_v1")
    scope_manifest = json.loads(scope_manifest_path.read_text(encoding="utf-8"))
    model_symbols = scope_manifest["scopes"]["model_symbols"]
    if not isinstance(model_symbols, list) or not model_symbols:
        print("symbol_movement_v1_failed:model_symbols_missing")
        return 6

    time_ctx = get_time_context()
    market_alignment = assess_artifact_alignment(
        artifact_name="market_regime_v1",
        payload=market_payload,
        now_utc=time_ctx.now_utc,
        max_age=MAX_OVERLAY_AGE,
    )
    sector_alignment = assess_artifact_alignment(
        artifact_name="sector_movement_v1",
        payload=sector_payload,
        now_utc=time_ctx.now_utc,
        max_age=MAX_OVERLAY_AGE,
    )
    market_overlay_fresh = market_alignment.valid
    sector_overlay_fresh = sector_alignment.valid
    if not market_overlay_fresh or not sector_overlay_fresh:
        print("symbol_movement_v1_failed:stale_overlay_inputs")
        for reason in market_alignment.reason_codes:
            print(f"error={reason}")
        for reason in sector_alignment.reason_codes:
            print(f"error={reason}")
        return 8
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in model_symbols)
        intraday_from_utc = time_ctx.now_utc - timedelta(days=2)
        bar_rows = conn.execute(
            f"""
            SELECT symbol, ts_utc, close, volume, data_status, correction_seq
            FROM market_bars
            WHERE timeframe = '1d'
              AND symbol IN ({placeholders})
            ORDER BY symbol, ts_utc
            """,
            model_symbols,
        ).fetchall()
        intraday_rows = conn.execute(
            f"""
            SELECT symbol, ts_utc, close, volume, data_status
            FROM market_bars
            WHERE timeframe = '1m'
              AND symbol IN ({placeholders})
              AND ts_utc >= ?
            ORDER BY symbol, ts_utc
            """,
            [*model_symbols, intraday_from_utc],
        ).fetchall()
        snapshot_rows = conn.execute(
            f"""
            SELECT symbol, as_of_utc, last_trade_price, prev_close, change_pct, day_vwap, market_status, data_status
            FROM (
                SELECT *,
                       row_number() over (partition by symbol order by as_of_utc desc) as rn
                FROM market_snapshots
                WHERE symbol IN ({placeholders})
            ) t
            WHERE rn = 1
            ORDER BY symbol
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
        print("symbol_movement_v1_failed:no_market_bars")
        return 7

    bars_by_symbol = _latest_bar_by_day(bar_rows)
    intraday_bars_by_symbol = _recent_intraday_bars(intraday_rows)
    latest_snapshots_by_symbol = _latest_snapshots_by_symbol(snapshot_rows)
    symbol_metadata = {
        str(symbol): {"asset_type": str(asset_type), "sector": str(sector)}
        for symbol, asset_type, sector in metadata_rows
    }
    sector_rows_by_scope = {
        str(row.get("scope_id", "")): row
        for row in sector_rows
        if isinstance(row, dict)
    }

    rows = build_symbol_movement_rows(
        bars_by_symbol=bars_by_symbol,
        symbol_metadata=symbol_metadata,
        market_regime_row=market_rows[0],
        sector_rows_by_scope=sector_rows_by_scope,
        model_symbols=sorted(str(symbol).strip().upper() for symbol in model_symbols),
        now_utc=time_ctx.now_utc,
        market_overlay_fresh=market_overlay_fresh,
        sector_overlay_fresh=sector_overlay_fresh,
        intraday_bars_by_symbol=intraday_bars_by_symbol,
        latest_snapshots_by_symbol=latest_snapshots_by_symbol,
    )
    quality_audit = audit_model_artifact(rows).to_dict()

    coverage_counts: dict[str, int] = {}
    blocked_symbols: list[str] = []
    for row in rows:
        coverage_state = str(row.get("coverage_state", "unknown"))
        coverage_counts[coverage_state] = coverage_counts.get(coverage_state, 0) + 1
        if coverage_state == "insufficient_evidence":
            blocked_symbols.append(str(row.get("scope_id", "")))

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "symbol_movement_v1.json"

    input_status = "ready"
    if blocked_symbols or not market_overlay_fresh or not sector_overlay_fresh:
        input_status = "thin_evidence"

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
            "intraday_symbol_count": len(intraday_bars_by_symbol),
            "snapshot_symbol_count": len(latest_snapshots_by_symbol),
            "market_regime_present": True,
            "sector_row_count": len(sector_rows_by_scope),
        },
        "input_audit": {
            "status": input_status,
            "required_model_symbols": sorted(str(symbol).strip().upper() for symbol in model_symbols),
            "present_bar_symbols": sorted(bars_by_symbol),
            "missing_bar_symbols": sorted(symbol for symbol in model_symbols if symbol not in bars_by_symbol),
            "market_regime_run_timestamp_utc": market_alignment.run_timestamp_utc,
            "sector_movement_run_timestamp_utc": sector_alignment.run_timestamp_utc,
            "market_overlay_age_sec": market_alignment.age_sec,
            "sector_overlay_age_sec": sector_alignment.age_sec,
            "market_overlay_fresh": market_overlay_fresh,
            "sector_overlay_fresh": sector_overlay_fresh,
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
    print(f"symbol_rows={len(rows)}")
    if rows:
        scores = [float(row.get("score_normalized", 0.0)) for row in rows]
        confidences = [int(row.get("confidence_score", 0)) for row in rows]
        print(f"score_range=({min(scores):.4f},{max(scores):.4f})")
        print(f"confidence_range=({min(confidences)},{max(confidences)})")
    print(f"quality_audit_status={quality_audit['status']}")
    if quality_audit["failure_reasons"]:
        print(f"quality_audit_failures={','.join(quality_audit['failure_reasons'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
