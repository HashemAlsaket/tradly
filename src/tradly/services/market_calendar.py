from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


MARKET_TZ = ZoneInfo("America/New_York")
PREMARKET_OPEN_ET = time(4, 0)
MARKET_OPEN_ET = time(9, 30)
MARKET_CLOSE_ET = time(16, 0)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    current += timedelta(weeks=n - 1)
    return current


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        current = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        current = date(year, month + 1, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def us_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),   # MLK Day
        _nth_weekday(year, 2, 0, 3),   # Presidents Day
        _easter_sunday(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, 0),     # Memorial Day
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),   # Labor Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        _observed_fixed_holiday(year, 12, 25),
    }
    return holidays


def is_us_market_holiday(day: date) -> bool:
    return day in us_market_holidays(day.year)


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and not is_us_market_holiday(day)


def previous_trading_day(day: date) -> date:
    current = day - timedelta(days=1)
    while not is_trading_day(current):
        current -= timedelta(days=1)
    return current


def next_trading_day(day: date) -> date:
    current = day + timedelta(days=1)
    while not is_trading_day(current):
        current += timedelta(days=1)
    return current


def market_calendar_state_for_date(day: date) -> str:
    if is_us_market_holiday(day):
        return "holiday"
    if day.weekday() >= 5:
        return "weekend"
    return "trading_day"


def market_session_state(now_utc: datetime) -> str:
    now_et = now_utc.astimezone(MARKET_TZ)
    day = now_et.date()
    if is_us_market_holiday(day):
        return "holiday"
    if day.weekday() >= 5:
        return "weekend"
    current_time = now_et.time()
    if current_time < PREMARKET_OPEN_ET:
        return "overnight"
    if current_time < MARKET_OPEN_ET:
        return "pre_market"
    if current_time <= MARKET_CLOSE_ET:
        return "market_hours"
    return "after_hours"


def horizon_execution_ready(*, horizon: str, now_utc: datetime) -> bool:
    session = market_session_state(now_utc)
    if horizon == "1to3d" and session in {"weekend", "holiday"}:
        return False
    return True


def market_closed_reason_code(*, now_utc: datetime) -> str | None:
    session = market_session_state(now_utc)
    if session == "weekend":
        return "market_closed_weekend"
    if session == "holiday":
        return "market_closed_holiday"
    return None


@dataclass(frozen=True)
class TradingCalendarRow:
    calendar_date: date
    day_of_week: int
    day_name: str
    is_weekend: bool
    is_market_holiday: bool
    is_trading_day: bool
    market_calendar_state: str
    last_cash_session_date: date
    next_cash_session_date: date


def build_trading_calendar_row(day: date) -> TradingCalendarRow:
    weekend = day.weekday() >= 5
    holiday = is_us_market_holiday(day)
    trading = is_trading_day(day)
    return TradingCalendarRow(
        calendar_date=day,
        day_of_week=(day.weekday() + 1) % 7,
        day_name=day.strftime("%A"),
        is_weekend=weekend,
        is_market_holiday=holiday,
        is_trading_day=trading,
        market_calendar_state=market_calendar_state_for_date(day),
        last_cash_session_date=day if trading else previous_trading_day(day),
        next_cash_session_date=day if trading else next_trading_day(day),
    )
