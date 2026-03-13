from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_LOCAL_TIMEZONE = "America/Chicago"


@dataclass(frozen=True)
class TimeContext:
    now_utc: datetime
    now_local: datetime
    local_timezone: str


def get_time_context(
    local_timezone: str = DEFAULT_LOCAL_TIMEZONE,
    now_utc: datetime | None = None,
) -> TimeContext:
    current_utc = now_utc or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")

    try:
        zone = ZoneInfo(local_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid timezone: {local_timezone}") from exc

    return TimeContext(
        now_utc=current_utc.astimezone(timezone.utc),
        now_local=current_utc.astimezone(zone),
        local_timezone=local_timezone,
    )
