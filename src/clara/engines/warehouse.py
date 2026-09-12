"""Warehouses and the Clara Compute Unit.

A *warehouse* is the unit customers reason about and pay for: a named,
right-sized, auto-suspending pool of compute. The **CCU** (Clara Compute Unit)
is its billing unit:

    1 CCU = 1 vCPU + 4 GB RAM, for 1 minute

Why define it publicly, when Databricks' DBU and Snowflake's credit are
deliberately opaque? Because a unit anchored to real hardware can be checked
against the cloud bill. A customer can compute their own infrastructure cost and
verify Clara's platform fee — which is the foundation of the cost-plus pricing
model in ``docs/pricing.md``.

Every size keeps a 1:4 vCPU:RAM ratio, so CCU/minute equals total vCPUs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from clara.errors import ValidationError
from clara.ids import Prefix, new_id
from clara.time_utils import seconds_between, utcnow

#: The CCU definition. Changing these changes the meaning of every invoice, so
#: they are constants, not configuration.
CCU_VCPUS = 1.0
CCU_GB_RAM = 4.0


class WarehouseSize(str, Enum):
    """T-shirt sizes. Each step up doubles capacity and doubles cost."""

    XS = "xs"
    S = "s"
    M = "m"
    L = "l"
    XL = "xl"
    XXL = "2xl"
    XXXL = "3xl"
    XXXXL = "4xl"


@dataclass(frozen=True)
class SizeSpec:
    """The physical shape behind a t-shirt size."""

    size: WarehouseSize
    nodes: int
    vcpus_per_node: int
    gb_ram_per_node: int

    @property
    def total_vcpus(self) -> int:
        return self.nodes * self.vcpus_per_node

    @property
    def total_gb_ram(self) -> int:
        return self.nodes * self.gb_ram_per_node

    @property
    def ccu_per_minute(self) -> float:
        """CCU burn rate: whichever of CPU or RAM is the binding constraint."""
        return max(self.total_vcpus / CCU_VCPUS, self.total_gb_ram / CCU_GB_RAM)

    @property
    def ccu_per_hour(self) -> float:
        return self.ccu_per_minute * 60.0

    def infra_cost_per_hour(self, rates: Any, spot: bool = False) -> float:
        """What this shape costs the operator per hour on a given provider."""
        return rates.compute_cost_per_hour(self.total_vcpus, self.total_gb_ram, spot=spot)


#: The size catalogue. Single node up to S, then scale out.
SIZES: dict[WarehouseSize, SizeSpec] = {
    WarehouseSize.XS: SizeSpec(WarehouseSize.XS, 1, 2, 8),
    WarehouseSize.S: SizeSpec(WarehouseSize.S, 1, 4, 16),
    WarehouseSize.M: SizeSpec(WarehouseSize.M, 2, 4, 16),
    WarehouseSize.L: SizeSpec(WarehouseSize.L, 4, 4, 16),
    WarehouseSize.XL: SizeSpec(WarehouseSize.XL, 8, 4, 16),
    WarehouseSize.XXL: SizeSpec(WarehouseSize.XXL, 16, 4, 16),
    WarehouseSize.XXXL: SizeSpec(WarehouseSize.XXXL, 32, 4, 16),
    WarehouseSize.XXXXL: SizeSpec(WarehouseSize.XXXXL, 64, 4, 16),
}


def spec_for(size: WarehouseSize | str) -> SizeSpec:
    """Look up a size spec, accepting the string form used in APIs and YAML."""
    resolved = WarehouseSize(str(size).lower()) if not isinstance(size, WarehouseSize) else size
    return SIZES[resolved]


class WarehouseState(str, Enum):
    """Lifecycle. Suspended warehouses cost nothing — the single biggest lever
    on a data platform bill, so Clara suspends aggressively by default."""

    SUSPENDED = "suspended"
    STARTING = "starting"
    RUNNING = "running"
    SUSPENDING = "suspending"
    FAILED = "failed"


@dataclass
class Warehouse:
    """A compute pool."""

    name: str
    size: WarehouseSize = WarehouseSize.XS
    #: ``auto`` lets the router pick per query; pin it to force one engine.
    engine: str = "auto"
    id: str = field(default_factory=lambda: new_id(Prefix.WAREHOUSE))
    workspace_id: str | None = None
    state: WarehouseState = WarehouseState.SUSPENDED

    #: Idle seconds before auto-suspend. 60 s is deliberately aggressive: most
    #: analytics is bursty, and a warehouse left running overnight is pure waste.
    auto_suspend_seconds: int = 60
    #: Start the warehouse on demand when a query arrives.
    auto_resume: bool = True
    #: Use pre-emptible/spot capacity. Cheap, and safe for retryable batch work.
    spot: bool = False
    #: Scale-out bounds. ``max_clusters > 1`` enables multi-cluster concurrency.
    min_clusters: int = 1
    max_clusters: int = 1

    created_at: datetime = field(default_factory=utcnow)
    last_activity_at: datetime | None = None
    started_at: datetime | None = None
    properties: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValidationError("warehouse requires a name")
        if isinstance(self.size, str):
            self.size = WarehouseSize(self.size.lower())
        if self.auto_suspend_seconds < 0:
            raise ValidationError("auto_suspend_seconds cannot be negative")
        if self.max_clusters < self.min_clusters:
            raise ValidationError("max_clusters must be >= min_clusters")

    # ------------------------------------------------------------- properties

    @property
    def spec(self) -> SizeSpec:
        return spec_for(self.size)

    @property
    def ccu_per_minute(self) -> float:
        """Burn rate at the current cluster count."""
        return self.spec.ccu_per_minute * max(self.min_clusters, 1)

    @property
    def is_running(self) -> bool:
        return self.state is WarehouseState.RUNNING

    # ---------------------------------------------------------- auto-suspend

    def idle_seconds(self, now: datetime | None = None) -> float:
        """Seconds since the last query finished."""
        if self.last_activity_at is None:
            return 0.0
        return seconds_between(self.last_activity_at, now or utcnow())

    def should_suspend(self, now: datetime | None = None) -> bool:
        """Whether the idle timer has expired.

        ``auto_suspend_seconds == 0`` means "never suspend" — an explicit opt-in
        for latency-critical workloads that accept the cost.
        """
        if self.state is not WarehouseState.RUNNING or self.auto_suspend_seconds == 0:
            return False
        return self.idle_seconds(now) >= self.auto_suspend_seconds

    def uptime_seconds(self, now: datetime | None = None) -> float:
        """Seconds the warehouse has been running — the metered quantity."""
        if self.started_at is None or self.state is WarehouseState.SUSPENDED:
            return 0.0
        return seconds_between(self.started_at, now or utcnow())

    # -------------------------------------------------------------- mutation

    def mark_started(self, now: datetime | None = None) -> None:
        moment = now or utcnow()
        self.state = WarehouseState.RUNNING
        self.started_at = moment
        self.last_activity_at = moment

    def mark_activity(self, now: datetime | None = None) -> None:
        self.last_activity_at = now or utcnow()

    def mark_suspended(self) -> None:
        self.state = WarehouseState.SUSPENDED
        self.started_at = None

    def resize(self, size: WarehouseSize | str) -> None:
        """Change size. Takes effect on next start; running queries are unaffected."""
        self.size = WarehouseSize(str(size).lower())

    # ------------------------------------------------------------ estimation

    def cost_per_hour(self, provider: Any, credit_price: float) -> dict[str, float]:
        """Cost breakdown for an hour of uptime.

        Returns both the operator's infrastructure cost and the customer's CCU
        charge, because showing both is the entire point of the pricing model.
        """
        infra = self.spec.infra_cost_per_hour(provider.rates(), spot=self.spot)
        infra *= max(self.min_clusters, 1)
        ccu = self.ccu_per_minute * 60.0
        return {
            "infra_cost": round(infra, 4),
            "ccu": round(ccu, 2),
            "ccu_charge": round(ccu * credit_price, 4),
            "total": round(infra + ccu * credit_price, 4),
        }

    def to_dict(self) -> dict[str, Any]:
        from clara.time_utils import to_iso

        return {
            "id": self.id,
            "name": self.name,
            "workspace_id": self.workspace_id,
            "size": self.size.value,
            "engine": self.engine,
            "state": self.state.value,
            "nodes": self.spec.nodes,
            "total_vcpus": self.spec.total_vcpus,
            "total_gb_ram": self.spec.total_gb_ram,
            "ccu_per_minute": self.ccu_per_minute,
            "auto_suspend_seconds": self.auto_suspend_seconds,
            "auto_resume": self.auto_resume,
            "spot": self.spot,
            "min_clusters": self.min_clusters,
            "max_clusters": self.max_clusters,
            "created_at": to_iso(self.created_at),
            "last_activity_at": to_iso(self.last_activity_at) if self.last_activity_at else None,
            "idle_seconds": round(self.idle_seconds(), 1),
        }


def recommend_size(
    bytes_scanned: float, target_seconds: float = 30.0, *, throughput_mb_s_per_vcpu: float = 120.0
) -> WarehouseSize:
    """Recommend the smallest size that should hit a latency target.

    Powers the "no data engineer" promise: the platform sizes the warehouse
    instead of asking a human to guess. Throughput defaults to a conservative
    120 MB/s per vCPU for Parquet scans on object storage.
    """
    if bytes_scanned <= 0:
        return WarehouseSize.XS
    mb = bytes_scanned / 1_000_000
    needed_vcpus = mb / (throughput_mb_s_per_vcpu * max(target_seconds, 1.0))
    for size in WarehouseSize:
        if SIZES[size].total_vcpus >= needed_vcpus:
            return size
    return WarehouseSize.XXXXL
