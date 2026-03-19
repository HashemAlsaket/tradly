from __future__ import annotations

import json
from pathlib import Path

from tradly.paths import get_repo_root
from tradly.services.time_context import get_time_context
from tradly.services.universe_registry import load_normalized_registry


def _load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_latest_json(runs_dir: Path, pattern: str) -> dict:
    candidates = sorted(runs_dir.glob(pattern))
    if not candidates:
        return {}
    return _load_json_file(candidates[-1])


def _expected_flags(row: dict) -> dict[str, bool]:
    stage = str(row.get("onboarding_stage", "")).strip()
    return {
        "expects_market_data": stage in {"market_data_only", "modeled", "modeled_with_direct_news", "portfolio_eligible"},
        "expects_model": stage in {"modeled", "modeled_with_direct_news", "portfolio_eligible"},
        "expects_direct_news": stage in {"modeled_with_direct_news", "portfolio_eligible"},
        "expects_portfolio": bool(row.get("portfolio_eligible", False)),
    }


def _check_result(value: bool, *, required: bool) -> dict[str, object]:
    if not required:
        return {"status": "skipped", "required": False}
    return {"status": "pass" if value else "fail", "required": True}


def _build_checks(
    *,
    symbol: str,
    sector_proxy: str | None,
    flags: dict[str, bool],
    instrument_symbols: set[str],
    market_data_symbols: set[str],
    model_symbols: set[str],
    direct_news_symbols: set[str],
    portfolio_symbols: set[str],
    daily_dates: set[tuple[str, str]],
    latest_daily_market_date: str,
    intraday_symbols: set[str],
    watermark_symbols: set[str],
    snapshot_symbols: set[str],
    symbol_movement_symbols: set[str],
    recommendation_symbols: set[str],
    symbol_news_symbols: set[str],
    portfolio_policy_symbols: set[str],
) -> dict[str, dict[str, object]]:
    return {
        "instrument_loaded": _check_result(symbol in instrument_symbols, required=True),
        "in_market_data_scope": _check_result(symbol in market_data_symbols, required=flags["expects_market_data"]),
        "daily_bar_present": _check_result(
            bool(latest_daily_market_date) and (symbol, latest_daily_market_date) in daily_dates,
            required=flags["expects_market_data"],
        ),
        "intraday_present": _check_result(
            symbol in intraday_symbols or symbol in watermark_symbols,
            required=flags["expects_market_data"],
        ),
        "snapshot_present": _check_result(symbol in snapshot_symbols, required=flags["expects_market_data"]),
        "sector_proxy_present": _check_result(sector_proxy in market_data_symbols if sector_proxy else True, required=True),
        "in_model_scope": _check_result(symbol in model_symbols, required=flags["expects_model"]),
        "symbol_movement_present": _check_result(symbol in symbol_movement_symbols, required=flags["expects_model"]),
        "recommendation_present": _check_result(symbol in recommendation_symbols, required=flags["expects_model"]),
        "in_direct_news_scope": _check_result(symbol in direct_news_symbols, required=flags["expects_direct_news"]),
        "symbol_news_present": _check_result(symbol in symbol_news_symbols, required=flags["expects_direct_news"]),
        "portfolio_scope_present": _check_result(symbol in portfolio_symbols, required=flags["expects_portfolio"]),
        "portfolio_row_present": _check_result(symbol in portfolio_policy_symbols, required=flags["expects_portfolio"]),
    }


def main() -> int:
    repo_root = get_repo_root()
    manual_dir = repo_root / "data" / "manual"
    runs_dir = repo_root / "data" / "runs"
    db_path = repo_root / "data" / "tradly.duckdb"
    time_ctx = get_time_context()

    try:
        universe_registry = load_normalized_registry(manual_dir / "universe_registry.json")
    except Exception:
        universe_registry = {}
    runtime_scopes = _load_json_file(manual_dir / "universe_runtime_scopes.json")
    freshness_snapshot = _load_json_file(repo_root / "data" / "journal" / "freshness_snapshot.json")
    symbol_movement_payload = _load_latest_json(runs_dir, "*/symbol_movement_v1.json")
    recommendation_payload = _load_latest_json(runs_dir, "*/recommendation_v1.json")
    symbol_news_payload = _load_latest_json(runs_dir, "*/symbol_news_v1.json")
    portfolio_payload = _load_latest_json(runs_dir, "*/portfolio_policy_v1.json")

    if not universe_registry or not runtime_scopes or not db_path.exists():
        print("universe_onboarding_audit_v1_failed:missing_inputs")
        return 1

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 2

    symbols = [
        row
        for row in universe_registry.get("symbols", [])
        if isinstance(row, dict) and str(row.get("onboarding_stage", "")).strip()
    ]
    if not symbols:
        print("universe_onboarding_audit_v1_skipped:no_staged_symbols")
        return 0

    market_data_symbols = set(runtime_scopes.get("scopes", {}).get("market_data_symbols", []))
    model_symbols = set(runtime_scopes.get("scopes", {}).get("model_symbols", []))
    direct_news_symbols = set(runtime_scopes.get("scopes", {}).get("direct_news_symbols", []))
    portfolio_symbols = set(runtime_scopes.get("scopes", {}).get("portfolio_eligible_symbols", []))
    symbol_movement_symbols = {
        str(row.get("scope_id", "")).strip().upper()
        for row in symbol_movement_payload.get("rows", [])
        if isinstance(row, dict)
    }
    recommendation_symbols = {
        str(row.get("scope_id", row.get("symbol", ""))).strip().upper()
        for row in recommendation_payload.get("rows", [])
        if isinstance(row, dict)
    }
    symbol_news_symbols = {
        str(row.get("scope_id", "")).strip().upper()
        for row in symbol_news_payload.get("rows", [])
        if isinstance(row, dict)
    }
    portfolio_policy_symbols = {
        str(row.get("symbol", "")).strip().upper()
        for row in portfolio_payload.get("rows", [])
        if isinstance(row, dict)
    }
    latest_daily_market_date = str((freshness_snapshot.get("freshness", {}) or {}).get("metrics", {}).get("latest_daily_bar_market_date", "")).strip()

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        instrument_symbols = {
            str(symbol).strip().upper()
            for (symbol,) in conn.execute("SELECT symbol FROM instruments").fetchall()
        }
        daily_dates = {
            (str(symbol).strip().upper(), str(dt))
            for symbol, dt in conn.execute(
                "SELECT symbol, CAST(ts_utc AS DATE) FROM market_bars WHERE timeframe='1d'"
            ).fetchall()
        }
        intraday_symbols = {
            str(symbol).strip().upper()
            for (symbol,) in conn.execute(
                "SELECT DISTINCT symbol FROM market_bars WHERE timeframe='1m' AND ts_utc >= ?",
                [time_ctx.now_utc.replace(hour=0, minute=0, second=0, microsecond=0)],
            ).fetchall()
        }
        snapshot_symbols = {
            str(symbol).strip().upper()
            for (symbol,) in conn.execute("SELECT DISTINCT symbol FROM market_snapshots").fetchall()
        }
        watermark_symbols = {
            str(scope_key).strip().upper()
            for (scope_key,) in conn.execute(
                """
                SELECT scope_key
                FROM pipeline_watermarks
                WHERE source_name = 'market_bars_1m'
                """
            ).fetchall()
        }
    finally:
        conn.close()

    sector_proxy_checks = {
        "Healthcare": "XLV",
    }

    rows: list[dict] = []
    failure_count = 0
    for row in symbols:
        symbol = str(row.get("symbol", "")).strip().upper()
        stage = str(row.get("onboarding_stage", "")).strip()
        sector = str(row.get("sector", "")).strip()
        flags = _expected_flags(row)
        sector_proxy = sector_proxy_checks.get(sector)
        checks = _build_checks(
            symbol=symbol,
            sector_proxy=sector_proxy,
            flags=flags,
            instrument_symbols=instrument_symbols,
            market_data_symbols=market_data_symbols,
            model_symbols=model_symbols,
            direct_news_symbols=direct_news_symbols,
            portfolio_symbols=portfolio_symbols,
            daily_dates=daily_dates,
            latest_daily_market_date=latest_daily_market_date,
            intraday_symbols=intraday_symbols,
            watermark_symbols=watermark_symbols,
            snapshot_symbols=snapshot_symbols,
            symbol_movement_symbols=symbol_movement_symbols,
            recommendation_symbols=recommendation_symbols,
            symbol_news_symbols=symbol_news_symbols,
            portfolio_policy_symbols=portfolio_policy_symbols,
        )
        failures = [
            name
            for name, result in checks.items()
            if bool(result.get("required")) and result.get("status") == "fail"
        ]
        if failures:
            failure_count += 1
        rows.append(
            {
                "symbol": symbol,
                "sector": sector,
                "onboarding_stage": stage,
                "portfolio_eligible": bool(row.get("portfolio_eligible", False)),
                "checks": checks,
                "status": "pass" if not failures else "fail",
                "failure_reasons": failures,
            }
        )

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "universe_onboarding_audit_v1.json"
    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "input_summary": {
            "staged_symbol_count": len(rows),
            "latest_daily_bar_market_date": latest_daily_market_date,
        },
        "quality_audit": {
            "status": "pass" if failure_count == 0 else "fail",
            "failure_count": failure_count,
        },
        "rows": rows,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"staged_symbols={len(rows)}")
    print(f"quality_audit_status={payload['quality_audit']['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
