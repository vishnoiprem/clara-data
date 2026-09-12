"""Table catalog: what tables exist, their shape, and where their data lives.

Use ``get_catalog()`` rather than instantiating an implementation directly —
that is what keeps the rest of the platform portable between a laptop and a
production Iceberg deployment.
"""

from __future__ import annotations

from clara.catalog.base import Catalog, TableInfo, TableRef
from clara.catalog.local import LocalCatalog
from clara.catalog.schema import DataType, Field_, Schema, ddl_columns, sql_type
from clara.errors import ValidationError
from clara.settings import Settings, get_settings

__all__ = [
    "Catalog",
    "DataType",
    "Field_",
    "LocalCatalog",
    "Schema",
    "TableInfo",
    "TableRef",
    "build_catalog",
    "ddl_columns",
    "get_catalog",
    "sql_type",
]

_CACHE: dict[str, Catalog] = {}


def build_catalog(settings: Settings | None = None) -> Catalog:
    """Construct a catalog from settings. Always returns a fresh instance."""
    cfg = settings or get_settings()
    kind = cfg.catalog.kind

    if kind == "memory":
        return LocalCatalog(root=None)

    # A sqlite catalog with no object storage behind it is a local dev setup, so
    # use the filesystem catalog; with real storage, use Iceberg-on-SQLite.
    if kind == "sqlite" and cfg.provider == "local":
        return LocalCatalog(root=cfg.state_dir / "catalog")

    if kind in {"sqlite", "rest", "glue"}:
        from clara.catalog.iceberg import IcebergCatalog
        from clara.providers import get_provider

        return IcebergCatalog.from_settings(
            cfg.catalog, storage_props=get_provider(cfg).iceberg_properties()
        )

    raise ValidationError(f"unknown catalog kind: {kind}")


def get_catalog(settings: Settings | None = None) -> Catalog:
    """Process-wide cached catalog, keyed by configuration.

    Catalog clients hold connections and metadata caches, so sharing one per
    configuration matters for request latency.
    """
    cfg = settings or get_settings()
    key = f"{cfg.catalog.kind}:{cfg.catalog.uri}:{cfg.catalog.warehouse}:{cfg.provider}"
    if key not in _CACHE:
        _CACHE[key] = build_catalog(cfg)
    return _CACHE[key]


def reset_catalog_cache() -> None:
    """Drop cached catalogs. Used by tests and by config reloads."""
    for catalog in _CACHE.values():
        catalog.close()
    _CACHE.clear()