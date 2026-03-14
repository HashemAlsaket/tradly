from __future__ import annotations

from datetime import date, datetime, time, timezone


def utc_now_db() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_db_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("database timestamps must be timezone-aware before normalization")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def from_db_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc)
    return value.replace(tzinfo=timezone.utc)


def date_to_db_utc(value: date) -> datetime:
    return datetime.combine(value, time.min)
