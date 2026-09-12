"""The IaaS provider contract.

Requirement: Clara must run on *any* infrastructure provider, including cheap
ones — Tencent, Alibaba, Hetzner, OVH — not just the big three. That is only
achievable if the platform depends on a narrow, genuinely portable interface.

A provider supplies exactly three things:

1. **Object storage** — addressed as S3-compatible, because every serious
   provider offers an S3 API (AWS S3, Azure Blob via its S3 proxy, GCS via its
   interoperability API, Tencent COS, Alibaba OSS, R2, B2, MinIO).
2. **Unit economics** — what a vCPU-hour, a GB-month and an egress GB actually
   cost here. This feeds Clara's cost-plus pricing, so the customer sees real
   infrastructure cost rather than a marked-up abstraction.
3. **Free-tier limits** — what the provider gives away, which is exactly what
   Clara's trial plan is bounded by.

Deliberately *not* in this interface: VM provisioning, Kubernetes, IAM. Clara
runs its engines as containers on whatever compute the customer already has;
attempting to abstract cluster provisioning across six clouds is how portability
projects die.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class InfraRates:
    """What raw infrastructure costs on this provider, in USD.

    These are list prices for a representative general-purpose instance family
    and standard object storage. They are defaults, not promises: an operator
    with negotiated or committed-use pricing overrides them per deployment, and
    Clara's cost-plus model then bills against their real numbers.
    """

    #: Compute, on-demand.
    vcpu_hour: float
    gb_ram_hour: float
    #: Fraction of on-demand price for pre-emptible/spot capacity.
    spot_discount: float = 0.7
    #: Object storage, standard tier.
    storage_gb_month: float = 0.023
    #: Per 1,000 requests.
    put_requests_per_1k: float = 0.005
    get_requests_per_1k: float = 0.0004
    #: Data leaving the provider's network, per GB.
    egress_gb: float = 0.09
    #: Cross-AZ traffic, per GB. Often the hidden cost of a distributed engine.
    interzone_gb: float = 0.01

    def compute_cost_per_hour(self, vcpus: float, gb_ram: float, spot: bool = False) -> float:
        """Hourly infrastructure cost of a node of this shape."""
        base = vcpus * self.vcpu_hour + gb_ram * self.gb_ram_hour
        return base * (1.0 - self.spot_discount) if spot else base


@dataclass(frozen=True)
class FreeTier:
    """What this provider gives away for free.

    Clara's trial plan is pinned to these numbers so that a trial genuinely
    costs the operator nothing to host — a self-hosting user on a free cloud
    account can run Clara indefinitely without a bill.
    """

    #: Compute hours per month at the free instance size.
    compute_hours_month: float = 0.0
    #: Free vCPU count at that size (AWS t3.micro = 2 vCPU / 1 GB).
    free_vcpus: float = 2.0
    free_gb_ram: float = 1.0
    #: Object storage.
    storage_gb: float = 5.0
    #: Egress per month.
    egress_gb_month: float = 1.0
    #: Whether the allowance renews monthly or expires after a fixed window.
    expires_after_days: int | None = 365
    notes: str = ""

    def ccu_minutes_month(self, ccu_vcpus: float = 1.0, ccu_gb_ram: float = 4.0) -> float:
        """Free allowance expressed in Clara Compute Units.

        One CCU is defined as 1 vCPU + 4 GB RAM for one minute, so a provider's
        free instance is usually a fraction of a CCU; the conversion is
        whichever of CPU or memory runs out first.
        """
        if self.compute_hours_month <= 0:
            return 0.0
        ccu_per_node = min(self.free_vcpus / ccu_vcpus, self.free_gb_ram / ccu_gb_ram)
        return self.compute_hours_month * 60.0 * max(ccu_per_node, 0.0)


@dataclass
class ObjectStoreConfig:
    """Everything needed to talk S3 to this provider."""

    bucket: str
    prefix: str = "warehouse"
    endpoint: str | None = None
    region: str = "us-east-1"
    access_key: str | None = None
    secret_key: str | None = None
    path_style: bool = True
    #: Scheme used in table locations: ``s3`` for most, ``s3a``/``gs``/``oss`` vary.
    scheme: str = "s3"

    @property
    def uri(self) -> str:
        return f"{self.scheme}://{self.bucket}/{self.prefix.strip('/')}"


@dataclass
class ProviderRegion:
    """A region, with the tag Clara uses to prefer cheap ones."""

    id: str
    name: str
    #: Price multiplier relative to the provider's cheapest region.
    price_index: float = 1.0
    tier: str = "standard"


class CloudProvider(ABC):
    """One infrastructure provider."""

    #: Stable key used in configuration (``CLARA_PROVIDER``).
    name: str = "abstract"
    #: Human label.
    display_name: str = "Abstract Provider"
    #: Whether object storage speaks the S3 API natively.
    s3_compatible: bool = True

    def __init__(self, storage: ObjectStoreConfig) -> None:
        self.storage = storage

    # ------------------------------------------------------------- economics

    @abstractmethod
    def rates(self, region: str | None = None) -> InfraRates:
        """Raw infrastructure unit costs, optionally region-adjusted."""

    @abstractmethod
    def free_tier(self) -> FreeTier:
        """The provider's free allowance, bounding Clara's trial plan."""

    def regions(self) -> list[ProviderRegion]:
        """Known regions. Used to recommend cheap placement."""
        return [ProviderRegion(self.storage.region, self.storage.region)]

    def cheapest_region(self) -> ProviderRegion | None:
        regions = self.regions()
        return min(regions, key=lambda r: r.price_index) if regions else None

    # --------------------------------------------------------------- storage

    def object_store(self) -> Any:
        """An ``ObjectStore`` client for this provider's bucket."""
        from clara.providers.objectstore import S3ObjectStore

        return S3ObjectStore(self.storage)

    def iceberg_properties(self) -> dict[str, str]:
        """PyIceberg FileIO properties for this provider's storage.

        This one method is why Clara is portable: Iceberg's S3 FileIO works
        against any S3-compatible endpoint, so switching clouds is a config
        change, not a migration.
        """
        props: dict[str, str] = {
            "s3.region": self.storage.region,
            "s3.path-style-access": "true" if self.storage.path_style else "false",
        }
        if self.storage.endpoint:
            props["s3.endpoint"] = self.storage.endpoint
        if self.storage.access_key:
            props["s3.access-key-id"] = self.storage.access_key
        if self.storage.secret_key:
            props["s3.secret-access-key"] = self.storage.secret_key
        return props

    def trino_catalog_properties(self) -> dict[str, str]:
        """Trino ``iceberg.properties`` entries for this provider."""
        props = {
            "connector.name": "iceberg",
            "iceberg.file-format": "PARQUET",
            "fs.native-s3.enabled": "true",
            "s3.region": self.storage.region,
            "s3.path-style-access": "true" if self.storage.path_style else "false",
        }
        if self.storage.endpoint:
            props["s3.endpoint"] = self.storage.endpoint
        if self.storage.access_key:
            props["s3.aws-access-key"] = self.storage.access_key
        if self.storage.secret_key:
            props["s3.aws-secret-key"] = self.storage.secret_key
        return props

    def duckdb_secret_sql(self) -> str | None:
        """A DuckDB ``CREATE SECRET`` statement for this provider's storage."""
        if not (self.storage.access_key and self.storage.secret_key):
            return None
        parts = [
            "TYPE s3",
            f"KEY_ID '{self.storage.access_key}'",
            f"SECRET '{self.storage.secret_key}'",
            f"REGION '{self.storage.region}'",
            f"URL_STYLE '{'path' if self.storage.path_style else 'vhost'}'",
        ]
        if self.storage.endpoint:
            host = self.storage.endpoint.split("://", 1)[-1]
            parts.append(f"ENDPOINT '{host}'")
            parts.append(f"USE_SSL {'false' if self.storage.endpoint.startswith('http://') else 'true'}")
        return f"CREATE OR REPLACE SECRET clara_storage ({', '.join(parts)})"

    # ---------------------------------------------------------------- summary

    def describe(self) -> dict[str, Any]:
        """Provider summary, surfaced by the API and the CLI."""
        rates = self.rates()
        free = self.free_tier()
        cheapest = self.cheapest_region()
        return {
            "name": self.name,
            "display_name": self.display_name,
            "s3_compatible": self.s3_compatible,
            "storage_uri": self.storage.uri,
            "region": self.storage.region,
            "cheapest_region": cheapest.id if cheapest else None,
            "rates": {
                "vcpu_hour": rates.vcpu_hour,
                "gb_ram_hour": rates.gb_ram_hour,
                "storage_gb_month": rates.storage_gb_month,
                "egress_gb": rates.egress_gb,
                "spot_discount": rates.spot_discount,
            },
            "free_tier": {
                "compute_hours_month": free.compute_hours_month,
                "storage_gb": free.storage_gb,
                "egress_gb_month": free.egress_gb_month,
                "ccu_minutes_month": round(free.ccu_minutes_month(), 1),
                "expires_after_days": free.expires_after_days,
                "notes": free.notes,
            },
        }

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"<{type(self).__name__} name={self.name} bucket={self.storage.bucket}>"


@dataclass
class RegionSet:
    """Helper for declaring a provider's regions compactly."""

    entries: list[ProviderRegion] = field(default_factory=list)

    @classmethod
    def of(cls, *specs: tuple[str, str, float]) -> list[ProviderRegion]:
        return [ProviderRegion(id=i, name=n, price_index=p) for i, n, p in specs]
