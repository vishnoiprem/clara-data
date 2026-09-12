"""Usage event storage.

The interface is narrow on purpose — append and aggregate. That makes it
implementable over anything: the in-memory store here, the control plane's SQL
store, or a customer's own warehouse table if they want their usage data in
their lakehouse (which, for an open platform, they should be able to have).
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from datetime import datetime

from clara.metering.events import Meter, UsageEvent, UsageSummary
from clara.time_utils import ensure_utc, month_bounds


class UsageStore(ABC):
    """Append-only usage event storage."""

    @abstractmethod
    def record(self, event: UsageEvent) -> UsageEvent:
        """Persist one event and return it."""

    def record_many(self, events: list[UsageEvent]) -> int:
        for event in events:
            self.record(event)
        return len(events)

    @abstractmethod
    def query(
        self,
        tenant_id: str,
        *,
        start: datetime,
        end: datetime,
        meter: Meter | None = None,
        workspace_id: str | None = None,
        limit: int | None = None,
    ) -> list[UsageEvent]:
        """Events in ``[start, end)``, newest last."""

    def summarize(
        self,
        tenant_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        workspace_id: str | None = None,
    ) -> UsageSummary:
        """Aggregate a period, defaulting to the current calendar month."""
        if start is None or end is None:
            start, end = month_bounds()
        events = self.query(tenant_id, start=start, end=end, workspace_id=workspace_id)
        return UsageSummary.from_events(tenant_id, events, start, end)

    def total(
        self, tenant_id: str, meter: Meter, *, start: datetime, end: datetime
    ) -> float:
        """Summed quantity for one meter — the hot path for quota checks."""
        return sum(
            e.quantity for e in self.query(tenant_id, start=start, end=end, meter=meter)
        )


class InMemoryUsageStore(UsageStore):
    """Thread-safe in-process store. Used by tests, the CLI and dry runs."""

    def __init__(self) -> None:
        self._events: list[UsageEvent] = []
        self._lock = threading.RLock()

    def record(self, event: UsageEvent) -> UsageEvent:
        with self._lock:
            self._events.append(event)
        return event

    def query(
        self,
        tenant_id: str,
        *,
        start: datetime,
        end: datetime,
        meter: Meter | None = None,
        workspace_id: str | None = None,
        limit: int | None = None,
    ) -> list[UsageEvent]:
        lower, upper = ensure_utc(start), ensure_utc(end)
        with self._lock:
            matched = [
                e
                for e in self._events
                if e.tenant_id == tenant_id
                and lower <= ensure_utc(e.occurred_at) < upper
                and (meter is None or e.meter is meter)
                and (workspace_id is None or e.workspace_id == workspace_id)
            ]
        matched.sort(key=lambda e: e.occurred_at)
        return matched[-limit:] if limit else matched

    def clear(self) -> None:
        with self._lock:
            self._events.clear()

    def __len__(self) -> int:
        return len(self._events)
