from __future__ import annotations

import csv
from datetime import datetime, timezone

from tradly.paths import get_repo_root


def main() -> int:
    repo_root = get_repo_root()
    seed_path = repo_root / "db" / "seeds" / "universe_v1.csv"
    db_path = repo_root / "data" / "tradly.duckdb"

    if not seed_path.exists():
        print(f"seed file not found: {seed_path}")
        return 1
    if not db_path.exists():
        print(f"duckdb file not found: {db_path}")
        print("run: python scripts/setup/init_db.py")
        return 2

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 3

    now = datetime.now(timezone.utc)

    rows: list[dict] = []
    with seed_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(
                {
                    "symbol": row["symbol"].strip().upper(),
                    "asset_type": row["asset_type"].strip(),
                    "sector": row["sector"].strip(),
                    "industry": row["industry"].strip(),
                    "halal_flag": row["halal_flag"].strip(),
                    "active": row["active"].strip().lower() == "true",
                    "as_of_utc": now,
                    "ingested_at_utc": now,
                }
            )

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TEMP TABLE tmp_instruments (
              symbol TEXT,
              asset_type TEXT,
              sector TEXT,
              industry TEXT,
              halal_flag TEXT,
              active BOOLEAN,
              as_of_utc TIMESTAMP,
              ingested_at_utc TIMESTAMP
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO tmp_instruments
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["symbol"],
                    r["asset_type"],
                    r["sector"],
                    r["industry"],
                    r["halal_flag"],
                    r["active"],
                    r["as_of_utc"],
                    r["ingested_at_utc"],
                )
                for r in rows
            ],
        )

        conn.execute(
            """
            INSERT INTO instruments
            SELECT * FROM tmp_instruments
            ON CONFLICT(symbol) DO UPDATE SET
              asset_type=excluded.asset_type,
              sector=excluded.sector,
              industry=excluded.industry,
              halal_flag=excluded.halal_flag,
              active=excluded.active,
              as_of_utc=excluded.as_of_utc,
              ingested_at_utc=excluded.ingested_at_utc
            """
        )

        total = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        by_flag = conn.execute(
            "SELECT halal_flag, COUNT(*) FROM instruments GROUP BY halal_flag ORDER BY halal_flag"
        ).fetchall()
    finally:
        conn.close()

    print(f"loaded_rows={len(rows)}")
    print(f"instruments_total={total}")
    print(f"status_breakdown={by_flag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
