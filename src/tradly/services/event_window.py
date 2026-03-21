from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


EARNINGS_TZ = ZoneInfo("America/New_York")
PRE_EVENT_LOOKAHEAD = timedelta(hours=24)
POST_EVENT_WINDOW = timedelta(hours=18)
DIGESTION_WINDOW = timedelta(hours=36)


@dataclass(frozen=True)
class EventWindow:
    symbol: str
    company: str
    event_type: str
    event_timestamp_utc: datetime | None
    event_timestamp_local: str | None
    event_phase: str
    event_active: bool
    source_note: str
    notes: str


def _parse_event_timestamp_utc(event: dict) -> datetime | None:
    report_date = str(event.get("report_date_et", "")).strip()
    if not report_date:
        return None
    call_time = str(event.get("call_time_et", "")).strip() or "16:00"
    try:
        local_dt = datetime.fromisoformat(f"{report_date}T{call_time}:00")
    except ValueError:
        return None
    localized = local_dt.replace(tzinfo=EARNINGS_TZ)
    return localized.astimezone(timezone.utc)


def _event_phase(*, now_utc: datetime, event_ts_utc: datetime | None) -> str:
    if event_ts_utc is None:
        return "inactive"
    if event_ts_utc - PRE_EVENT_LOOKAHEAD <= now_utc < event_ts_utc:
        return "pre_event"
    if event_ts_utc <= now_utc <= event_ts_utc + POST_EVENT_WINDOW:
        return "post_event"
    if event_ts_utc + POST_EVENT_WINDOW < now_utc <= event_ts_utc + DIGESTION_WINDOW:
        return "digestion_window"
    return "inactive"


def load_event_windows(*, watchlist_path: Path, now_utc: datetime) -> dict[str, EventWindow]:
    payload = json.loads(watchlist_path.read_text(encoding="utf-8"))
    events = payload.get("events", []) if isinstance(payload, dict) else []
    rows: dict[str, EventWindow] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        symbol = str(event.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        event_ts_utc = _parse_event_timestamp_utc(event)
        phase = _event_phase(now_utc=now_utc, event_ts_utc=event_ts_utc)
        rows[symbol] = EventWindow(
            symbol=symbol,
            company=str(event.get("company", "")).strip(),
            event_type="earnings",
            event_timestamp_utc=event_ts_utc,
            event_timestamp_local=event_ts_utc.astimezone(EARNINGS_TZ).isoformat() if event_ts_utc else None,
            event_phase=phase,
            event_active=phase != "inactive",
            source_note=str(event.get("source_note", "")).strip(),
            notes=str(event.get("notes", "")).strip(),
        )
    return rows
