from __future__ import annotations

from pathlib import Path
import sys


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    schema_path = repo_root / "db" / "schema_v1.sql"
    db_path = repo_root / "data" / "tradly.duckdb"

    if not schema_path.exists():
        print(f"schema file not found: {schema_path}")
        return 1

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 2

    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = schema_path.read_text(encoding="utf-8")

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(schema_sql)
    finally:
        conn.close()

    print(f"initialized_db={db_path}")
    print(f"applied_schema={schema_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
