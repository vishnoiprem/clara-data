"""Cron scheduling.

A dependency-free five-field cron parser, plus friendly aliases (``@daily``,
``every 15 minutes``). Written rather than pulled in because scheduling is the
one piece of orchestration that must work identically on a laptop, in CI and in
production, with no optional extra installed.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator

from clara.errors import ValidationError
from clara.time_utils import UTC, ensure_utc, utcnow

#: Named shorthands. ``@hourly`` and friends match standard cron.
ALIASES: dict[str, str] = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

_EVERY_RE = re.compile(
    r"^every\s+(?P<count>\d+)?\s*(?P<unit>minute|minutes|hour|hours|day|days)$",
    re.IGNORECASE,
)

#: Day-of-week allows 7 as well as 0 for Sunday, as standard cron does; it is
#: normalised to 0 after parsing.
_FIELD_BOUNDS = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]
_FIELD_NAMES = ("minute", "hour", "day-of-month", "month", "day-of-week")

_MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
_DAYS = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}


@dataclass
class CronSchedule:
    """A parsed cron expression, evaluated in UTC."""

    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    #: True when day-of-month and day-of-week are both restricted. Cron's
    #: historical behaviour is to OR them, which surprises people, so it is
    #: made explicit here.
    or_day_fields: bool = False

    @classmethod
    def parse(cls, expression: str) -> CronSchedule:
        text = expression.strip().lower()
        if not text:
            raise ValidationError("empty schedule expression")

        if text in ALIASES:
            text = ALIASES[text]
        elif match := _EVERY_RE.match(text):
            count = int(match.group("count") or 1)
            unit = match.group("unit").rstrip("s")
            if unit == "minute":
                if not 1 <= count <= 59:
                    raise ValidationError("'every N minutes' requires 1 <= N <= 59")
                text = f"*/{count} * * * *"
            elif unit == "hour":
                text = f"0 */{count} * * *"
            else:
                text = f"0 0 */{count} * *"

        fields = text.split()
        if len(fields) != 5:
            raise ValidationError(
                f"cron expression must have 5 fields, got {len(fields)}: {expression!r}"
            )

        parsed = [
            _parse_field(raw, index) for index, raw in enumerate(fields)
        ]
        dom_restricted = fields[2] != "*"
        dow_restricted = fields[4] != "*"

        return cls(
            expression=expression.strip(),
            minutes=parsed[0],
            hours=parsed[1],
            days_of_month=parsed[2],
            months=parsed[3],
            days_of_week=parsed[4],
            or_day_fields=dom_restricted and dow_restricted,
        )

    # ------------------------------------------------------------- evaluation

    def matches(self, moment: datetime) -> bool:
        """Whether a minute matches this schedule."""
        moment = ensure_utc(moment)
        if moment.minute not in self.minutes or moment.hour not in self.hours:
            return False
        if moment.month not in self.months:
            return False

        # Python's weekday() is Mon=0; cron uses Sun=0.
        dow = (moment.weekday() + 1) % 7
        dom_match = moment.day in self.days_of_month
        dow_match = dow in self.days_of_week

        return (dom_match or dow_match) if self.or_day_fields else (dom_match and dow_match)

    def next_after(self, moment: datetime | None = None) -> datetime:
        """The next firing strictly after ``moment``.

        Steps minute by minute with a hard bound of four years, which covers
        every valid expression (Feb 29 on a non-leap-year pattern is the worst
        case) and guarantees termination on an impossible one.
        """
        current = ensure_utc(moment or utcnow()).replace(second=0, microsecond=0)
        current += timedelta(minutes=1)
        limit = current + timedelta(days=366 * 4)

        while current <= limit:
            if self.matches(current):
                return current
            # Skip a whole day when the date fields cannot match.
            if current.month not in self.months or not self._day_possible(current):
                current = (current + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            current += timedelta(minutes=1)

        raise ValidationError(f"schedule never fires: {self.expression!r}")

    def _day_possible(self, moment: datetime) -> bool:
        dow = (moment.weekday() + 1) % 7
        dom_match = moment.day in self.days_of_month
        dow_match = dow in self.days_of_week
        return (dom_match or dow_match) if self.or_day_fields else (dom_match and dow_match)

    def upcoming(self, count: int = 5, after: datetime | None = None) -> list[datetime]:
        """The next ``count`` firings — shown when a user sets a schedule."""
        moment = after or utcnow()
        results: list[datetime] = []
        for _ in range(count):
            moment = self.next_after(moment)
            results.append(moment)
        return results

    def iter_between(self, start: datetime, end: datetime) -> Iterator[datetime]:
        """Every firing in ``[start, end)`` — used to plan a backfill."""
        moment = ensure_utc(start) - timedelta(minutes=1)
        upper = ensure_utc(end)
        while True:
            moment = self.next_after(moment)
            if moment >= upper:
                return
            yield moment

    def describe(self) -> str:
        """Human description of the schedule, for the console."""
        if self.expression in ALIASES:
            return self.expression
        for alias, expression in ALIASES.items():
            if self.expression == expression:
                return f"{alias} ({self.expression})"
        if len(self.minutes) == 1 and len(self.hours) == 1:
            minute, hour = next(iter(self.minutes)), next(iter(self.hours))
            return f"daily at {hour:02d}:{minute:02d} UTC"
        if len(self.minutes) > 1 and self.hours == frozenset(range(24)):
            step = sorted(self.minutes)
            if len(step) > 1:
                return f"every {step[1] - step[0]} minutes"
        return self.expression

    def to_dict(self) -> dict[str, object]:
        from clara.time_utils import to_iso

        return {
            "expression": self.expression,
            "description": self.describe(),
            "timezone": "UTC",
            "upcoming": [to_iso(m) for m in self.upcoming(3)],
        }


def _parse_field(raw: str, index: int) -> frozenset[int]:
    """Parse one cron field into the set of values it matches."""
    low, high = _FIELD_BOUNDS[index]
    values: set[int] = set()

    for part in raw.split(","):
        part = part.strip()
        if not part:
            raise ValidationError(f"empty {_FIELD_NAMES[index]} field")

        step = 1
        if "/" in part:
            part, _, step_text = part.partition("/")
            if not step_text.isdigit() or int(step_text) < 1:
                raise ValidationError(f"invalid step in {_FIELD_NAMES[index]}: /{step_text}")
            step = int(step_text)

        if part in ("*", "?"):
            start, end = low, high
        elif "-" in part.lstrip("-"):
            start_text, _, end_text = part.partition("-")
            start, end = _value(start_text, index), _value(end_text, index)
        else:
            start = end = _value(part, index)
            if step > 1:
                end = high

        if start > end:
            raise ValidationError(f"inverted range in {_FIELD_NAMES[index]}: {part}")
        if not (low <= start <= high and low <= end <= high):
            raise ValidationError(
                f"{_FIELD_NAMES[index]} out of range [{low}-{high}]: {part}"
            )
        values.update(range(start, end + 1, step))

    # Cron accepts 7 for Sunday.
    if index == 4 and 7 in values:
        values.discard(7)
        values.add(0)
    return frozenset(values)


def _value(text: str, index: int) -> int:
    text = text.strip().lower()
    if index == 3 and text in _MONTHS:
        return _MONTHS[text]
    if index == 4 and text in _DAYS:
        return _DAYS[text]
    if not text.isdigit():
        raise ValidationError(f"invalid {_FIELD_NAMES[index]} value: {text!r}")
    return int(text)


def next_run_at(expression: str, after: datetime | None = None) -> datetime:
    """Convenience: next firing of an expression."""
    return CronSchedule.parse(expression).next_after(after)


def is_due(expression: str, last_run_at: datetime | None, now: datetime | None = None) -> bool:
    """Whether a schedule is due, given when it last ran.

    Used by the polling scheduler. Comparing against the last run rather than
    matching the current minute means a scheduler that was down for ten minutes
    still fires the missed window once, instead of skipping it silently.
    """
    moment = ensure_utc(now or utcnow())
    schedule = CronSchedule.parse(expression)
    if last_run_at is None:
        # Never run: fire if a firing has already passed today.
        return schedule.matches(moment.replace(second=0, microsecond=0)) or any(
            schedule.matches(moment - timedelta(minutes=i)) for i in range(1, 60)
        )
    return schedule.next_after(ensure_utc(last_run_at)) <= moment


def parse(expression: str) -> CronSchedule:
    """Parse an expression, raising ``ValidationError`` if malformed."""
    return CronSchedule.parse(expression)


__all__ = [
    "ALIASES",
    "UTC",
    "CronSchedule",
    "is_due",
    "next_run_at",
    "parse",
]
