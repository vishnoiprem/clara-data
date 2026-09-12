"""Pluggable infrastructure providers.

Third parties add a provider without touching Clara:

    from clara.providers import register_provider
    register_provider(MyCloudProvider)

or by publishing a ``clara.providers`` entry point in their own package, which
is discovered automatically on first use.
"""

from __future__ import annotations

from clara.errors import ValidationError
from clara.logging_setup import get_logger
from clara.providers.base import (
    CloudProvider,
    FreeTier,
    InfraRates,
    ObjectStoreConfig,
    ProviderRegion,
)
from clara.providers.clouds import BUILTIN_PROVIDERS
from clara.providers.objectstore import LocalObjectStore, ObjectStore, S3ObjectStore
from clara.settings import Settings, get_settings

log = get_logger(__name__)

__all__ = [
    "CloudProvider",
    "FreeTier",
    "InfraRates",
    "LocalObjectStore",
    "ObjectStore",
    "ObjectStoreConfig",
    "ProviderRegion",
    "S3ObjectStore",
    "available_providers",
    "build_provider",
    "get_provider",
    "register_provider",
]

_REGISTRY: dict[str, type[CloudProvider]] = dict(BUILTIN_PROVIDERS)
_ENTRY_POINTS_LOADED = False
_CACHE: dict[str, CloudProvider] = {}


def register_provider(provider: type[CloudProvider], *, replace: bool = False) -> None:
    """Register a provider implementation under its ``name``."""
    if not provider.name or provider.name == "abstract":
        raise ValidationError(f"{provider.__name__} must define a unique 'name'")
    if provider.name in _REGISTRY and not replace:
        raise ValidationError(f"provider already registered: {provider.name}")
    _REGISTRY[provider.name] = provider
    log.debug("registered provider", extra={"provider": provider.name})


def _load_entry_points() -> None:
    """Discover third-party providers published as ``clara.providers`` entry points."""
    global _ENTRY_POINTS_LOADED
    if _ENTRY_POINTS_LOADED:
        return
    _ENTRY_POINTS_LOADED = True
    from importlib.metadata import entry_points

    for entry in entry_points(group="clara.providers"):
        try:
            register_provider(entry.load(), replace=True)
        except Exception as exc:  # noqa: BLE001 - a broken plugin must not break startup
            log.warning(
                "could not load provider plugin",
                extra={"entry_point": entry.name, "error": str(exc)},
            )


def available_providers() -> list[str]:
    """Names of every registered provider."""
    _load_entry_points()
    return sorted(_REGISTRY)


def storage_config_from_settings(settings: Settings) -> ObjectStoreConfig:
    """Translate settings into an object-store config."""
    schemes = {"gcp": "s3", "azure": "s3", "alibaba": "s3", "tencent": "s3"}
    return ObjectStoreConfig(
        bucket=settings.storage.bucket,
        prefix=settings.storage.prefix,
        endpoint=settings.storage.endpoint,
        region=settings.storage.region,
        access_key=settings.storage.access_key,
        secret_key=settings.storage.secret_key,
        path_style=settings.storage.path_style,
        scheme=schemes.get(settings.provider, "s3"),
    )


def build_provider(settings: Settings | None = None, name: str | None = None) -> CloudProvider:
    """Instantiate a provider. Always returns a fresh object."""
    cfg = settings or get_settings()
    _load_entry_points()
    resolved = name or cfg.provider
    implementation = _REGISTRY.get(resolved)
    if implementation is None:
        raise ValidationError(
            f"unknown provider: {resolved}", available=sorted(_REGISTRY)
        )
    return implementation(storage_config_from_settings(cfg))


def get_provider(settings: Settings | None = None, name: str | None = None) -> CloudProvider:
    """Cached provider instance, keyed by name and bucket."""
    cfg = settings or get_settings()
    resolved = name or cfg.provider
    key = f"{resolved}:{cfg.storage.bucket}:{cfg.storage.endpoint}"
    if key not in _CACHE:
        _CACHE[key] = build_provider(cfg, resolved)
    return _CACHE[key]


def reset_provider_cache() -> None:
    """Drop cached providers. Used by tests and config reloads."""
    _CACHE.clear()


def compare_providers(settings: Settings | None = None) -> list[dict[str, object]]:
    """Cost comparison across every provider, cheapest vCPU-hour first.

    Powers ``clara providers compare`` — the answer to "where should we run
    this, and what would it cost?"
    """
    cfg = settings or get_settings()
    rows: list[dict[str, object]] = []
    for name in available_providers():
        try:
            provider = build_provider(cfg, name)
        except Exception:  # noqa: BLE001 - skip providers that cannot configure
            continue
        summary = provider.describe()
        rates = provider.rates()
        # Cost of one medium warehouse (8 vCPU / 32 GB) running for an hour.
        summary["medium_warehouse_hour"] = round(
            rates.compute_cost_per_hour(8, 32), 4
        )
        rows.append(summary)
    return sorted(rows, key=lambda r: r["medium_warehouse_hour"])  # type: ignore[arg-type,return-value]
