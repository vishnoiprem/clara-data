"""The metering facade.

Everything chargeable goes through a ``UsageMeter``. It owns the one piece of
knowledge callers should not have to repeat: how a physical quantity (query
seconds, stored bytes, ingested rows) becomes a metered quantity with a
snapshotted infrastructure unit cost.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from clara.engines.warehouse import CCU_GB_RAM, CCU_VCPUS, Warehouse
from clara.logging_setup import get_logger
from clara.metering.events import Meter, UsageEvent
from clara.metering.store import InMemoryUsageStore, UsageStore
from clara.providers import CloudProvider
from clara.time_utils import hours_in_month, utcnow

log = get_logger(__name__)

#: Object-storage PUTs per GB written, assuming 128 MB multipart chunks. Used to
#: attribute request costs to ingestion rather than burying them in overhead.
_PUTS_PER_GB = 8.0

#: Shape of an orchestration worker: small, since tasks mostly wait on I/O.
_TASK_VCPUS = 1.0
_TASK_GB_RAM = 2.0


class UsageMeter:
    """Records usage events with infrastructure cost attached."""

    def __init__(
        self,
        provider: CloudProvider,
        store: UsageStore | None = None,
        *,
        tenant_id: str,
        workspace_id: str | None = None,
        minimum_billable_seconds: float = 1.0,
    ) -> None:
        self.provider = provider
        # Explicit None check: stores define __len__, so an empty store is
        # falsy and `store or InMemoryUsageStore()` would silently discard it.
        self.store = store if store is not None else InMemoryUsageStore()
        self.tenant_id = tenant_id
        self.workspace_id = workspace_id
        self.minimum_billable_seconds = minimum_billable_seconds

    # ------------------------------------------------------------- unit costs

    def unit_cost(self, meter: Meter, *, spot: bool = False) -> float:
        """Infrastructure cost per unit of a meter, on this provider right now."""
        rates = self.provider.rates()
        if meter is Meter.COMPUTE:
            return rates.compute_cost_per_hour(CCU_VCPUS, CCU_GB_RAM, spot=spot) / 60.0
        if meter is Meter.STORAGE:
            return rates.storage_gb_month
        if meter is Meter.INGEST:
            return (_PUTS_PER_GB / 1000.0) * rates.put_requests_per_1k
        if meter is Meter.ORCHESTRATION:
            return rates.compute_cost_per_hour(_TASK_VCPUS, _TASK_GB_RAM, spot=spot) / 60.0
        if meter is Meter.EGRESS:
            return rates.egress_gb
        return 0.0

    # --------------------------------------------------------------- recording

    def record(
        self,
        meter: Meter,
        quantity: float,
        *,
        resource_type: str | None = None,
        resource_id: str | None = None,
        occurred_at: datetime | None = None,
        spot: bool = False,
        **attributes: Any,
    ) -> UsageEvent | None:
        """Record one event. Returns ``None`` for zero/negative quantities."""
        if quantity <= 0:
            return None
        event = UsageEvent(
            meter=meter,
            quantity=quantity,
            tenant_id=self.tenant_id,
            workspace_id=self.workspace_id,
            occurred_at=occurred_at or utcnow(),
            resource_type=resource_type,
            resource_id=resource_id,
            provider=self.provider.name,
            region=self.provider.storage.region,
            unit_cost_usd=self.unit_cost(meter, spot=spot),
            attributes=attributes,
        )
        return self.store.record(event)

    # ------------------------------------------------------------ convenience

    def record_query(
        self,
        stats: Any,
        warehouse: Warehouse,
        *,
        query_id: str | None = None,
        routing_reason: str | None = None,
    ) -> UsageEvent | None:
        """Meter one query's compute.

        Cache hits are free, and every query is charged a minimum billable
        duration so that thousands of sub-second statements do not round to
        nothing (or, worse, to per-statement overhead that exceeds the work).
        """
        if getattr(stats, "cache_hit", False):
            return None

        seconds = max(
            getattr(stats, "warehouse_seconds", 0.0) or getattr(stats, "wall_seconds", 0.0),
            self.minimum_billable_seconds,
        )
        ccu_minutes = (seconds / 60.0) * warehouse.ccu_per_minute
        return self.record(
            Meter.COMPUTE,
            ccu_minutes,
            resource_type="warehouse",
            resource_id=warehouse.id,
            spot=warehouse.spot,
            query_id=query_id,
            engine=getattr(stats, "engine", None),
            bytes_scanned=getattr(stats, "bytes_scanned", 0),
            warehouse_size=warehouse.size.value,
            routing_reason=routing_reason,
        )

    def record_warehouse_uptime(
        self, warehouse: Warehouse, seconds: float
    ) -> UsageEvent | None:
        """Meter idle warehouse uptime.

        A running warehouse costs money whether or not it is serving queries,
        so idle time is metered honestly rather than hidden. This is also what
        makes the auto-suspend saving visible on the invoice.
        """
        ccu_minutes = (seconds / 60.0) * warehouse.ccu_per_minute
        return self.record(
            Meter.COMPUTE,
            ccu_minutes,
            resource_type="warehouse",
            resource_id=warehouse.id,
            spot=warehouse.spot,
            kind="uptime",
            warehouse_size=warehouse.size.value,
        )

    def record_storage_snapshot(
        self, bytes_stored: float, *, hours: float = 1.0, table: str | None = None
    ) -> UsageEvent | None:
        """Meter stored bytes for a window, prorated to GB-months.

        Storage is sampled hourly and prorated rather than measured at
        month-end, so a customer who loads and deletes a large table pays for
        the hours it existed — not for the peak, and not for nothing.
        """
        gb = bytes_stored / 1_000_000_000
        gb_months = gb * (hours / hours_in_month())
        return self.record(
            Meter.STORAGE,
            gb_months,
            resource_type="table" if table else "lakehouse",
            resource_id=table,
            bytes_stored=int(bytes_stored),
            window_hours=hours,
        )

    def record_sync(
        self,
        *,
        bytes_moved: float,
        rows: int,
        connector: str,
        pipeline_id: str | None = None,
    ) -> UsageEvent | None:
        """Meter a connector sync's data volume."""
        return self.record(
            Meter.INGEST,
            bytes_moved / 1_000_000_000,
            resource_type="pipeline",
            resource_id=pipeline_id,
            connector=connector,
            rows=rows,
        )

    def record_task(
        self, *, seconds: float, task_name: str, run_id: str | None = None
    ) -> UsageEvent | None:
        """Meter orchestration task runtime outside a warehouse."""
        return self.record(
            Meter.ORCHESTRATION,
            seconds / 60.0,
            resource_type="run",
            resource_id=run_id,
            task=task_name,
        )

    def record_egress(self, bytes_out: float, *, reason: str = "export") -> UsageEvent | None:
        return self.record(Meter.EGRESS, bytes_out / 1_000_000_000, reason=reason)

    def record_api_call(self, route: str) -> UsageEvent | None:
        return self.record(Meter.API, 1.0, route=route)

    # ------------------------------------------------------------------ reading

    def summarize(self, **kwargs: Any) -> Any:
        """Current-period usage summary for this tenant."""
        return self.store.summarize(self.tenant_id, workspace_id=self.workspace_id, **kwargs)
