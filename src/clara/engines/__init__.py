"""Query execution.

``get_router()`` is the entry point: it builds whichever engines are actually
installed and reachable, and returns a router that picks between them per query.
"""

from __future__ import annotations

from clara.catalog import Catalog, get_catalog
from clara.engines.base import Engine, EngineCapabilities, QueryResult, QueryStats
from clara.engines.duckdb_engine import DuckDBEngine
from clara.engines.router import EngineRouter, RoutingDecision
from clara.engines.warehouse import (
    CCU_GB_RAM,
    CCU_VCPUS,
    SIZES,
    SizeSpec,
    Warehouse,
    WarehouseSize,
    WarehouseState,
    recommend_size,
    spec_for,
)
from clara.errors import EngineError
from clara.logging_setup import get_logger
from clara.settings import Settings, get_settings

log = get_logger(__name__)

__all__ = [
    "CCU_GB_RAM",
    "CCU_VCPUS",
    "DuckDBEngine",
    "Engine",
    "EngineCapabilities",
    "EngineRouter",
    "QueryResult",
    "QueryStats",
    "RoutingDecision",
    "SIZES",
    "SizeSpec",
    "Warehouse",
    "WarehouseSize",
    "WarehouseState",
    "build_engines",
    "get_engine",
    "get_router",
    "recommend_size",
    "spec_for",
]

_ROUTER_CACHE: dict[str, EngineRouter] = {}


def build_engines(
    settings: Settings | None = None, catalog: Catalog | None = None
) -> dict[str, Engine]:
    """Build every engine that is installed and reachable.

    Unreachable engines are skipped with a warning rather than raising: a
    developer with no Trino running should still be able to query locally.
    """
    cfg = settings or get_settings()
    resolved_catalog = catalog or get_catalog(cfg)
    engines: dict[str, Engine] = {}

    try:
        duckdb_engine = DuckDBEngine(cfg.duckdb, catalog=resolved_catalog, settings=cfg)
        if duckdb_engine.test_connection():
            duckdb_engine.sync_catalog()
            engines[duckdb_engine.name] = duckdb_engine
    except Exception as exc:  # noqa: BLE001
        log.warning("duckdb engine unavailable", extra={"error": str(exc)})

    try:
        from clara.engines.trino_engine import TrinoEngine

        trino_engine = TrinoEngine(cfg.trino, settings=cfg)
        if trino_engine.test_connection():
            engines[trino_engine.name] = trino_engine
        else:
            log.info(
                "trino not reachable; single-node only",
                extra={"host": cfg.trino.host, "port": cfg.trino.port},
            )
    except Exception as exc:  # noqa: BLE001 - optional extra not installed
        log.debug("trino engine unavailable", extra={"error": str(exc)})

    if not engines:
        raise EngineError(
            "no query engine available; install an extra with "
            "pip install 'clara-data[duckdb]' or configure Trino"
        )
    return engines


def get_router(settings: Settings | None = None, catalog: Catalog | None = None) -> EngineRouter:
    """Cached router. Engines hold connections, so reuse matters."""
    cfg = settings or get_settings()
    key = f"{cfg.provider}:{cfg.catalog.uri}:{cfg.trino.host}:{cfg.duckdb.path}"
    if key not in _ROUTER_CACHE:
        resolved_catalog = catalog or get_catalog(cfg)
        _ROUTER_CACHE[key] = EngineRouter(
            build_engines(cfg, resolved_catalog), catalog=resolved_catalog
        )
    return _ROUTER_CACHE[key]


def get_engine(
    name: str | None = None, settings: Settings | None = None, catalog: Catalog | None = None
) -> Engine:
    """A specific engine by name, or the configured default."""
    cfg = settings or get_settings()
    router = get_router(cfg, catalog)
    resolved = name or cfg.default_engine
    if resolved in (None, "auto"):
        # Prefer the cheap engine as the default handle.
        return router.engines.get("duckdb") or next(iter(router.engines.values()))
    engine = router.engines.get(resolved)
    if engine is None:
        raise EngineError(f"engine not available: {resolved}", available=sorted(router.engines))
    return engine


def reset_engine_cache() -> None:
    """Close and drop cached routers. Used by tests and config reloads."""
    for router in _ROUTER_CACHE.values():
        router.close()
    _ROUTER_CACHE.clear()
