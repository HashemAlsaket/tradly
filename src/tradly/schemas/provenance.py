from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class NumericProvenance:
    """Required metadata for any computed numeric field."""

    computed_by: str
    input_snapshot: str
    calculated_at: datetime
    method: str

    def __post_init__(self) -> None:
        if not self.computed_by:
            raise ValueError("computed_by must not be empty")
        if not self.input_snapshot:
            raise ValueError("input_snapshot must not be empty")
        if not self.method:
            raise ValueError("method must not be empty")
        if self.calculated_at.tzinfo is None:
            raise ValueError("calculated_at must be timezone-aware")


@dataclass(frozen=True)
class QualitativeTag:
    """Marks non-numeric LLM outputs so they are never treated as computed numbers."""

    qualitative_label: str
    is_qualitative_non_numeric: bool = True

    def __post_init__(self) -> None:
        if not self.qualitative_label:
            raise ValueError("qualitative_label must not be empty")
