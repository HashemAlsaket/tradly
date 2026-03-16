from __future__ import annotations

import json
from collections import defaultdict
from datetime import timedelta

from tradly.config import get_model_registry_entry
from tradly.models.calibration import audit_model_artifact
from tradly.models.market_regime import Bar, IntradayBar, SnapshotPoint
from tradly.models.sector_movement import (
    BROAD_MARKET_PROXIES,
    CANONICAL_SECTOR_PROXIES,
    build_sector_movement_rows,
)
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

    registry_entry = get_model_registry_entry("sector_movement_v1")
    time_ctx = get_time_context()
    symbols = sorted(set(BROAD_MARKET_PROXIES) | set(CANONICAL_SECTOR_PROXIES.values()))

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        placeholders = ", ".join("?" for _ in symbols)
        intraday_from_utc = time_ctx.now_utc - timedelta(days=2)
        bar_rows = conn.execute(
            f"""
            SELECT symbol, ts_utc, close, volume, data_status, correction_seq
            FROM market_bars
            WHERE timeframe = '1d'
              AND symbol IN ({placeholders})
            ORDER BY symbol, ts_utc
            """,
            symbols,
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
            [*symbols, intraday_from_utc],
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
            symbols,
        ).fetchall()
    finally:
        conn.close()

    if not bar_rows:
        print("sector_movement_v1_failed:no_market_bars")
        return 4

    bars_by_symbol = _latest_bar_by_day(bar_rows)
    intraday_bars_by_symbol = _recent_intraday_bars(intraday_rows)
    latest_snapshots_by_symbol = _latest_snapshots_by_symbol(snapshot_rows)
    present_symbols = sorted(bars_by_symbol)
    required_sector_proxies = sorted(CANONICAL_SECTOR_PROXIES.values())
    missing_broad_market_proxies = sorted(symbol for symbol in BROAD_MARKET_PROXIES if symbol not in bars_by_symbol)
    missing_sector_proxies = sorted(symbol for symbol in required_sector_proxies if symbol not in bars_by_symbol)
    sector_proxy_status_by_sector = {
        sector: {
            "proxy_symbol": proxy_symbol,
            "present": proxy_symbol in bars_by_symbol,
        }
        for sector, proxy_symbol in CANONICAL_SECTOR_PROXIES.items()
    }

    rows = build_sector_movement_rows(
        bars_by_symbol=bars_by_symbol,
        now_utc=time_ctx.now_utc,
        sector_members=json.loads(scope_manifest_path.read_text(encoding="utf-8"))["groupings"]["by_sector"],
        intraday_bars_by_symbol=intraday_bars_by_symbol,
        latest_snapshots_by_symbol=latest_snapshots_by_symbol,
    )
    quality_audit = audit_model_artifact(rows).to_dict()

    coverage_counts: dict[str, int] = {}
    blocked_sectors: list[str] = []
    for row in rows:
        coverage_state = str(row.get("coverage_state", "unknown"))
        coverage_counts[coverage_state] = coverage_counts.get(coverage_state, 0) + 1
        if coverage_state == "insufficient_evidence":
            blocked_sectors.append(str(row.get("scope_id", "")))

    if missing_sector_proxies:
        run_status = "blocked_missing_inputs"
    elif blocked_sectors:
        run_status = "thin_evidence"
    else:
        run_status = "ready"

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sector_movement_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "sector_count": len(CANONICAL_SECTOR_PROXIES),
            "sector_proxies": CANONICAL_SECTOR_PROXIES,
            "broad_market_proxies": list(BROAD_MARKET_PROXIES),
            "bar_symbol_count": len(bars_by_symbol),
            "intraday_symbol_count": len(intraday_bars_by_symbol),
            "snapshot_symbol_count": len(latest_snapshots_by_symbol),
        },
        "input_audit": {
            "status": run_status,
            "required_broad_market_proxies": list(BROAD_MARKET_PROXIES),
            "required_sector_proxies": required_sector_proxies,
            "present_symbols": present_symbols,
            "missing_broad_market_proxies": missing_broad_market_proxies,
            "missing_sector_proxies": missing_sector_proxies,
            "sector_proxy_status_by_sector": sector_proxy_status_by_sector,
            "unblock_actions": [
                "seed_missing_sector_etf_daily_bars" if missing_sector_proxies else "none",
                "rerun_sector_movement_v1_after_bar_coverage" if missing_sector_proxies else "none",
            ],
        },
        "row_audit": {
            "coverage_counts": coverage_counts,
            "blocked_sectors": blocked_sectors,
        },
        "quality_audit": quality_audit,
        "rows": rows,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"sector_rows={len(rows)}")
    print(f"input_audit_status={run_status}")
    if rows:
        scores = [float(row.get("score_normalized", 0.0)) for row in rows]
        confidences = [int(row.get("confidence_score", 0)) for row in rows]
        print(f"score_range=({min(scores):.4f},{max(scores):.4f})")
        print(f"confidence_range=({min(confidences)},{max(confidences)})")
    print(f"quality_audit_status={quality_audit['status']}")
    if quality_audit["failure_reasons"]:
        print(f"quality_audit_failures={','.join(quality_audit['failure_reasons'])}")
    if rows:
        top = rows[0]
        print(f"sample_sector={top['scope_id']}")
        print(f"sample_score={top['score_normalized']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
