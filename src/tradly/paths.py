from __future__ import annotations

from pathlib import Path


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def ensure_path_allowed_for_duckdb_ingest(path: Path, *, repo_root: Path | None = None) -> Path:
    resolved_repo_root = repo_root.resolve() if repo_root is not None else get_repo_root().resolve()
    resolved_path = path.resolve()
    forbidden_root = (resolved_repo_root / "robinhood_data").resolve()
    if resolved_path == forbidden_root or forbidden_root in resolved_path.parents:
        raise RuntimeError(f"forbidden_duckdb_ingest_source:{resolved_path}")
    return resolved_path
