from __future__ import annotations

from pathlib import Path

import pytest

from tradly.paths import ensure_path_allowed_for_duckdb_ingest


def test_ensure_path_allowed_for_duckdb_ingest_allows_repo_inputs(tmp_path: Path) -> None:
    repo_root = tmp_path
    allowed = repo_root / "data" / "manual" / "watchlists.json"
    allowed.parent.mkdir(parents=True)
    allowed.write_text("{}", encoding="utf-8")

    assert ensure_path_allowed_for_duckdb_ingest(allowed, repo_root=repo_root) == allowed.resolve()


def test_ensure_path_allowed_for_duckdb_ingest_blocks_robinhood_data(tmp_path: Path) -> None:
    repo_root = tmp_path
    forbidden = repo_root / "robinhood_data" / "account.csv"
    forbidden.parent.mkdir(parents=True)
    forbidden.write_text("header\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="forbidden_duckdb_ingest_source"):
        ensure_path_allowed_for_duckdb_ingest(forbidden, repo_root=repo_root)
