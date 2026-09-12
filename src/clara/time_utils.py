"""Time helpers.

Clara bills by the second, so every timestamp is timezone-aware UTC and every
duration is explicit. Naive datetimes are rejected rather than guessed at.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

UTC = timezone.utc


def utcnow() -> datetime:
    """Current time as an aware UTC datetime."""
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    """Coerce to aware UTC, rejecting naive datetimes.

    Silently localising a naive datetime is how billing periods drift, so this
    raises instead.
    """
    if value.tzinfo is None:
        raise ValueError("naive datetime is not allowed; attach a timezone (UTC)")
    return value.astimezone(UTC)


def to_iso(value: datetime) -> str:
    """RFC 3339 / ISO 8601 string with a trailing Z."""
    return ensure_utc(value).isoformat().replace("+00:00", "Z")


def from_iso(value: str) -> datetime:
    """Parse an ISO 8601 timestamp, accepting a trailing Z."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def seconds_between(start: datetime, end: datetime) -> float:
    """Non-negative elapsed seconds between two instants."""
    return max(0.0, (ensure_utc(end) - ensure_utc(start)).total_seconds())


def month_bounds(moment: datetime | date | None = None) -> tuple[datetime, datetime]:
    """Half-open UTC bounds ``[start, end)`` of the calendar month containing ``moment``."""
    if moment is None:
        moment = utcnow()
    day = ensure_utc(moment).date() if isinstance(moment, datetime) else moment
    start = datetime(day.year, day.month, 1, tzinfo=UTC)
    end = (
        datetime(day.year + 1, 1, 1, tzinfo=UTC)
        if day.month == 12
        else datetime(day.year, day.month + 1, 1, tzinfo=UTC)
    )
    return start, end


def day_bounds(moment: datetime | None = None) -> tuple[datetime, datetime]:
    """Half-open UTC bounds of the calendar day containing ``moment``."""
    now = ensure_utc(moment or utcnow())
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def hours_in_month(moment: datetime | None = None) -> float:
    """Hours in the calendar month — used to prorate GB-month storage charges."""
    start, end = month_bounds(moment)
    return (end - start).total_seconds() / 3600.0


def format_duration(seconds: float) -> str:
    """Compact human duration: ``1.2s``, ``3m 04s``, ``2h 07m``."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60):02d}s"
    return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60):02d}m"
