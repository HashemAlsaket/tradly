from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class CanonicalRecord:
    """Base record shape required by DATA_CONTRACT_V1."""

    record_id: UUID
    as_of_timestamp: datetime
    ingested_at: datetime
    source: str
    source_ref: str
    schema_version: str

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("source must not be empty")
        if not self.source_ref:
            raise ValueError("source_ref must not be empty")
        if not self.schema_version:
            raise ValueError("schema_version must not be empty")
        if self.as_of_timestamp.tzinfo is None:
            raise ValueError("as_of_timestamp must be timezone-aware")
        if self.ingested_at.tzinfo is None:
            raise ValueError("ingested_at must be timezone-aware")
