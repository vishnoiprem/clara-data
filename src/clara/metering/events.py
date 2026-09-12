"""Usage events — the billing source of truth.

Every chargeable action emits one immutable event. Two design decisions matter:

* **Unit cost is snapshotted at capture time.** Provider prices change; an
  invoice must not. Storing the unit cost on the event means a re-run of billing
  for March produces March's number, forever.
* **Events record infrastructure cost, not price.** Price is derived later by
  the pricing engine from the plan and rate card. The same event stream
  therefore supports "what did this cost us" and "what do we charge" without
  double bookkeeping.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from clara.ids import Prefix, new_id
from clara.time_utils import to_iso, utcnow


class Meter(str, Enum):
    """The billable dimensions. Adding one is a pricing decision, not a code
    convenience — each must be independently explainable on an invoice."""

    #: Warehouse compute, in CCU-minutes. The dominant cost on any lakehouse.
    COMPUTE = "compute_ccu_minute"
    #: Lakehouse storage, in GB-months (prorated hourly).
    STORAGE = "storage_gb_month"
    #: Data pulled by managed connectors, in GB.
    INGEST = "ingest_gb"
    #: Scheduled task runtime outside a warehouse, in task-minutes.
    ORCHESTRATION = "orchestration_task_minute"
    #: Data leaving the provider network, in GB. Passed through at cost.
    EGRESS = "egress_gb"
    #: Control-plane calls. Metered for abuse protection, effectively free.
    API = "api_request"

    @property
    def unit(self) -> str:
        return {
            Meter.COMPUTE: "CCU-min",
            Meter.STORAGE: "GB-month",
            Meter.INGEST: "GB",
            Meter.ORCHESTRATION: "task-min",
            Meter.EGRESS: "GB",
            Meter.API: "requests",
        }[self]


@dataclass
class UsageEvent:
    """One metered occurrence. Immutable once recorded."""

    meter: Meter
    quantity: float
    tenant_id: str
    id: str = field(default_factory=lambda: new_id(Prefix.USAGE_EVENT))
    workspace_id: str | None = None
    occurred_at: datetime = field(default_factory=utcnow)

    #: What produced the usage, for chargeback and per-resource attribution.
    resource_type: str | None = None
    resource_id: str | None = None

    #: Where it ran, and what infrastructure cost per unit at that moment.
    provider: str = "local"
    region: str | None = None
    unit_cost_usd: float = 0.0

    #: Anything useful for debugging a line item: query id, engine, table.
    attributes: dict[str, Any] = field(default_factory=dict)

    @property
    def infra_cost_usd(self) -> float:
        """What this usage cost the operator in raw infrastructure."""
        return self.quantity * self.unit_cost_usd

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "workspace_id": self.workspace_id,
            "meter": self.meter.value,
            "unit": self.meter.unit,
            "quantity": self.quantity,
            "occurred_at": to_iso(self.occurred_at),
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "provider": self.provider,
            "region": self.region,
            "unit_cost_usd": self.unit_cost_usd,
            "infra_cost_usd": round(self.infra_cost_usd, 6),
            "attributes": self.attributes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> UsageEvent:
        from clara.time_utils import from_iso

        return cls(
            id=payload.get("id") or new_id(Prefix.USAGE_EVENT),
            tenant_id=payload["tenant_id"],
            workspace_id=payload.get("workspace_id"),
            meter=Meter(payload["meter"]),
            quantity=float(payload["quantity"]),
            occurred_at=from_iso(payload["occurred_at"])
            if payload.get("occurred_at")
            else utcnow(),
            resource_type=payload.get("resource_type"),
            resource_id=payload.get("resource_id"),
            provider=payload.get("provider", "local"),
            region=payload.get("region"),
            unit_cost_usd=float(payload.get("unit_cost_usd", 0.0)),
            attributes=payload.get("attributes", {}),
        )


@dataclass
class UsageSummary:
    """Aggregated usage for one tenant over one period.

    This is what the pricing engine consumes. Keeping aggregation separate from
    pricing means the same summary can be priced against several rate cards —
    which is exactly what the "what would this cost on plan X" estimator does.
    """

    tenant_id: str
    period_start: datetime
    period_end: datetime
    quantities: dict[Meter, float] = field(default_factory=lambda: defaultdict(float))
    infra_cost_usd: dict[Meter, float] = field(default_factory=lambda: defaultdict(float))
    event_count: int = 0
    #: Per-resource totals, for chargeback by warehouse/pipeline.
    by_resource: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    providers: set[str] = field(default_factory=set)

    def add(self, event: UsageEvent) -> None:
        self.quantities[event.meter] = self.quantities.get(event.meter, 0.0) + event.quantity
        self.infra_cost_usd[event.meter] = (
            self.infra_cost_usd.get(event.meter, 0.0) + event.infra_cost_usd
        )
        self.event_count += 1
        self.providers.add(event.provider)
        if event.resource_id:
            key = f"{event.resource_type or 'resource'}:{event.resource_id}"
            self.by_resource[key] = self.by_resource.get(key, 0.0) + event.infra_cost_usd

    def quantity(self, meter: Meter) -> float:
        return self.quantities.get(meter, 0.0)

    def cost(self, meter: Meter) -> float:
        return self.infra_cost_usd.get(meter, 0.0)

    @property
    def total_infra_cost_usd(self) -> float:
        """Total raw infrastructure cost — the base of the platform fee."""
        return sum(self.infra_cost_usd.values())

    @classmethod
    def from_events(
        cls, tenant_id: str, events: list[UsageEvent], period_start: datetime, period_end: datetime
    ) -> UsageSummary:
        summary = cls(tenant_id=tenant_id, period_start=period_start, period_end=period_end)
        for event in events:
            summary.add(event)
        return summary

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "period_start": to_iso(self.period_start),
            "period_end": to_iso(self.period_end),
            "event_count": self.event_count,
            "providers": sorted(self.providers),
            "quantities": {m.value: round(q, 6) for m, q in self.quantities.items()},
            "infra_cost_usd": {m.value: round(c, 6) for m, c in self.infra_cost_usd.items()},
            "total_infra_cost_usd": round(self.total_infra_cost_usd, 4),
            "top_resources": sorted(
                ({"resource": k, "infra_cost_usd": round(v, 4)} for k, v in self.by_resource.items()),
                key=lambda r: r["infra_cost_usd"],  # type: ignore[arg-type,return-value]
                reverse=True,
            )[:10],
        }
