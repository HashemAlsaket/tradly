from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_LOCAL_TIMEZONE = "America/Chicago"
NOW_UTC_OVERRIDE_ENV = "TRADLY_NOW_UTC_OVERRIDE"


@dataclass(frozen=True)
class TimeContext:
    now_utc: datetime
    now_local: datetime
    local_timezone: str


def get_time_context(
    local_timezone: str = DEFAULT_LOCAL_TIMEZONE,
    now_utc: datetime | None = None,
) -> TimeContext:
    current_utc = now_utc
    if current_utc is None:
        override_raw = os.getenv(NOW_UTC_OVERRIDE_ENV, "").strip()
        if override_raw:
            try:
                override_dt = datetime.fromisoformat(override_raw)
            except ValueError as exc:
                raise ValueError(f"invalid {NOW_UTC_OVERRIDE_ENV}: {override_raw}") from exc
            if override_dt.tzinfo is None:
                raise ValueError(f"{NOW_UTC_OVERRIDE_ENV} must be timezone-aware")
            current_utc = override_dt.astimezone(timezone.utc)
    current_utc = current_utc or datetime.now(timezone.utc)
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
