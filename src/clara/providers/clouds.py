"""Built-in provider implementations.

The rate tables live together in one file on purpose: their whole value is being
comparable, and a pricing change on one cloud is usually reviewed against the
others. Adding a provider elsewhere is still fully supported — see
``clara.providers.register_provider``.

All figures are on-demand US list prices for a representative 2 vCPU / 8 GB
general-purpose instance and standard object storage, normalised to a per-vCPU
and per-GB-RAM split (70/30, the conventional allocation). They are **defaults
for estimation**, refreshed by ``clara providers refresh``; any operator with
committed-use or negotiated pricing overrides them, and Clara's cost-plus
billing then works from their real numbers.

The spread is the point: the generic/bare-metal tier is roughly an order of
magnitude cheaper per vCPU-hour than the hyperscalers, which is what lets a
mid-sized company run a lakehouse at a price Databricks cannot match.
"""

from __future__ import annotations

from clara.providers.base import (
    CloudProvider,
    FreeTier,
    InfraRates,
    ObjectStoreConfig,
    ProviderRegion,
    RegionSet,
)


class AWSProvider(CloudProvider):
    """Amazon Web Services. Reference implementation — m6i + S3."""

    name = "aws"
    display_name = "Amazon Web Services"

    def rates(self, region: str | None = None) -> InfraRates:
        index = self._region_index(region)
        return InfraRates(
            vcpu_hour=0.0336 * index,
            gb_ram_hour=0.0036 * index,
            spot_discount=0.70,
            storage_gb_month=0.023 * index,
            put_requests_per_1k=0.005,
            get_requests_per_1k=0.0004,
            egress_gb=0.09,
            interzone_gb=0.01,
        )

    def free_tier(self) -> FreeTier:
        return FreeTier(
            compute_hours_month=750.0,  # t3.micro
            free_vcpus=2.0,
            free_gb_ram=1.0,
            storage_gb=5.0,
            egress_gb_month=100.0,
            expires_after_days=365,
            notes="750 h/month t3.micro + 5 GB S3 for the first 12 months.",
        )

    def regions(self) -> list[ProviderRegion]:
        return RegionSet.of(
            ("us-east-1", "N. Virginia", 1.00),
            ("us-west-2", "Oregon", 1.00),
            ("eu-central-1", "Frankfurt", 1.08),
            ("ap-southeast-1", "Singapore", 1.14),
            ("ap-southeast-7", "Thailand", 1.12),
            ("ap-south-1", "Mumbai", 0.96),
        )

    def _region_index(self, region: str | None) -> float:
        target = region or self.storage.region
        match = next((r for r in self.regions() if r.id == target), None)
        return match.price_index if match else 1.0


class AzureProvider(CloudProvider):
    """Microsoft Azure. Dsv5 + Blob Storage (S3 access via a gateway)."""

    name = "azure"
    display_name = "Microsoft Azure"

    def rates(self, region: str | None = None) -> InfraRates:
        return InfraRates(
            vcpu_hour=0.0336,
            gb_ram_hour=0.0036,
            spot_discount=0.72,
            storage_gb_month=0.018,  # hot LRS is cheaper than S3 standard
            put_requests_per_1k=0.0055,
            get_requests_per_1k=0.0004,
            egress_gb=0.087,
            interzone_gb=0.01,
        )

    def free_tier(self) -> FreeTier:
        return FreeTier(
            compute_hours_month=750.0,  # B1S
            free_vcpus=1.0,
            free_gb_ram=1.0,
            storage_gb=5.0,
            egress_gb_month=100.0,
            expires_after_days=365,
            notes="750 h/month B1S + 5 GB Blob for the first 12 months.",
        )

    def regions(self) -> list[ProviderRegion]:
        return RegionSet.of(
            ("eastus", "East US", 1.00),
            ("westeurope", "West Europe", 1.09),
            ("southeastasia", "Southeast Asia", 1.12),
            ("centralindia", "Central India", 0.95),
        )

    def iceberg_properties(self) -> dict[str, str]:
        # Blob is reachable over the S3 API only through a gateway; without one,
        # PyIceberg's ADLS FileIO is the supported path.
        if self.storage.endpoint:
            return super().iceberg_properties()
        props = {"adls.account-name": self.storage.bucket}
        if self.storage.secret_key:
            props["adls.account-key"] = self.storage.secret_key
        return props


class GCPProvider(CloudProvider):
    """Google Cloud. n2-standard + GCS (S3 interoperability API)."""

    name = "gcp"
    display_name = "Google Cloud Platform"

    def rates(self, region: str | None = None) -> InfraRates:
        return InfraRates(
            vcpu_hour=0.0340,
            gb_ram_hour=0.0036,
            spot_discount=0.80,  # deepest spot discount of the big three
            storage_gb_month=0.020,
            put_requests_per_1k=0.005,
            get_requests_per_1k=0.0004,
            egress_gb=0.12,
            interzone_gb=0.01,
        )

    def free_tier(self) -> FreeTier:
        return FreeTier(
            compute_hours_month=744.0,  # e2-micro, always free
            free_vcpus=2.0,
            free_gb_ram=1.0,
            storage_gb=5.0,
            egress_gb_month=1.0,
            expires_after_days=None,  # always-free, does not expire
            notes="e2-micro + 5 GB GCS always free (us-* regions), no expiry.",
        )

    def regions(self) -> list[ProviderRegion]:
        return RegionSet.of(
            ("us-central1", "Iowa", 1.00),
            ("europe-west4", "Netherlands", 1.08),
            ("asia-southeast1", "Singapore", 1.13),
            ("asia-south1", "Mumbai", 0.98),
        )

    def iceberg_properties(self) -> dict[str, str]:
        props = super().iceberg_properties()
        # GCS speaks S3 via storage.googleapis.com with HMAC keys.
        props.setdefault("s3.endpoint", "https://storage.googleapis.com")
        return props


class TencentProvider(CloudProvider):
    """Tencent Cloud. CVM S5 + COS — materially cheaper than the big three."""

    name = "tencent"
    display_name = "Tencent Cloud"

    def rates(self, region: str | None = None) -> InfraRates:
        return InfraRates(
            vcpu_hour=0.0217,
            gb_ram_hour=0.0023,
            spot_discount=0.75,
            storage_gb_month=0.0138,
            put_requests_per_1k=0.0015,
            get_requests_per_1k=0.0002,
            egress_gb=0.08,
            interzone_gb=0.005,
        )

    def free_tier(self) -> FreeTier:
        return FreeTier(
            compute_hours_month=0.0,  # trial is credit-based, not hour-based
            storage_gb=50.0,
            egress_gb_month=10.0,
            expires_after_days=180,
            notes="50 GB COS free for 6 months; compute via trial credits.",
        )

    def regions(self) -> list[ProviderRegion]:
        return RegionSet.of(
            ("ap-bangkok", "Bangkok", 1.00),
            ("ap-singapore", "Singapore", 1.05),
            ("ap-guangzhou", "Guangzhou", 0.92),
            ("ap-hongkong", "Hong Kong", 1.03),
        )

    def iceberg_properties(self) -> dict[str, str]:
        props = super().iceberg_properties()
        props.setdefault("s3.endpoint", f"https://cos.{self.storage.region}.myqcloud.com")
        return props


class AlibabaProvider(CloudProvider):
    """Alibaba Cloud. ECS g7 + OSS."""

    name = "alibaba"
    display_name = "Alibaba Cloud"

    def rates(self, region: str | None = None) -> InfraRates:
        return InfraRates(
            vcpu_hour=0.0242,
            gb_ram_hour=0.0026,
            spot_discount=0.80,
            storage_gb_month=0.0148,
            put_requests_per_1k=0.0015,
            get_requests_per_1k=0.0002,
            egress_gb=0.074,
            interzone_gb=0.005,
        )

    def free_tier(self) -> FreeTier:
        return FreeTier(
            compute_hours_month=0.0,
            storage_gb=5.0,
            egress_gb_month=10.0,
            expires_after_days=365,
            notes="5 GB OSS for 12 months; compute via trial credits.",
        )

    def regions(self) -> list[ProviderRegion]:
        return RegionSet.of(
            ("ap-southeast-1", "Singapore", 1.00),
            ("ap-southeast-7", "Bangkok", 1.02),
            ("cn-hangzhou", "Hangzhou", 0.88),
            ("ap-south-1", "Mumbai", 0.97),
        )

    def iceberg_properties(self) -> dict[str, str]:
        props = super().iceberg_properties()
        props.setdefault("s3.endpoint", f"https://oss-{self.storage.region}.aliyuncs.com")
        return props


class GenericProvider(CloudProvider):
    """Any other provider: Hetzner, OVH, Scaleway, DigitalOcean, bare metal.

    Defaults reflect European VPS + S3-compatible object storage pricing, which
    is roughly 10× cheaper per vCPU-hour than hyperscaler on-demand. This is the
    tier that makes a lakehouse affordable for a mid-sized company — and the
    reason Clara refuses to assume a hyperscaler anywhere in its design.
    """

    name = "generic"
    display_name = "Generic / Bring-your-own S3"

    def __init__(self, storage: ObjectStoreConfig, rates: InfraRates | None = None) -> None:
        super().__init__(storage)
        self._rates = rates or InfraRates(
            vcpu_hour=0.0029,
            gb_ram_hour=0.0007,
            spot_discount=0.0,  # no spot market at this tier
            storage_gb_month=0.006,
            put_requests_per_1k=0.0,
            get_requests_per_1k=0.0,
            egress_gb=0.001,  # usually bundled up to a generous cap
            interzone_gb=0.0,
        )

    def rates(self, region: str | None = None) -> InfraRates:
        return self._rates

    def free_tier(self) -> FreeTier:
        return FreeTier(
            compute_hours_month=0.0,
            storage_gb=0.0,
            egress_gb_month=0.0,
            expires_after_days=None,
            notes="No free tier assumed; set rates to match your contract.",
        )


class LocalProvider(CloudProvider):
    """The developer's laptop. Costs nothing, limits nothing."""

    name = "local"
    display_name = "Local (no cloud)"
    s3_compatible = False

    def rates(self, region: str | None = None) -> InfraRates:
        return InfraRates(
            vcpu_hour=0.0,
            gb_ram_hour=0.0,
            spot_discount=0.0,
            storage_gb_month=0.0,
            put_requests_per_1k=0.0,
            get_requests_per_1k=0.0,
            egress_gb=0.0,
            interzone_gb=0.0,
        )

    def free_tier(self) -> FreeTier:
        return FreeTier(
            compute_hours_month=float("inf"),
            storage_gb=float("inf"),
            egress_gb_month=float("inf"),
            expires_after_days=None,
            notes="Local development: unmetered.",
        )

    def object_store(self):  # noqa: ANN201 - matches base signature
        from clara.providers.objectstore import LocalObjectStore
        from clara.settings import get_settings

        return LocalObjectStore(get_settings().storage.local_root, self.storage)

    def iceberg_properties(self) -> dict[str, str]:
        return {}

    def trino_catalog_properties(self) -> dict[str, str]:
        return {"connector.name": "iceberg", "iceberg.file-format": "PARQUET"}

    def duckdb_secret_sql(self) -> str | None:
        return None


#: Built-ins, keyed by the value of ``CLARA_PROVIDER``.
BUILTIN_PROVIDERS: dict[str, type[CloudProvider]] = {
    AWSProvider.name: AWSProvider,
    AzureProvider.name: AzureProvider,
    GCPProvider.name: GCPProvider,
    TencentProvider.name: TencentProvider,
    AlibabaProvider.name: AlibabaProvider,
    GenericProvider.name: GenericProvider,
    LocalProvider.name: LocalProvider,
}
