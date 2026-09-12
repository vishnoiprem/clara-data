"""Shared test fixtures.

Every fixture is hermetic: no network, no cloud account, no Trino, no Docker.
The local catalog plus DuckDB gives a real lakehouse in a temp directory, so the
tests exercise the same code paths production uses rather than mocks.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from clara.catalog.local import LocalCatalog
from clara.engines.duckdb_engine import DuckDBEngine
from clara.engines.router import EngineRouter
from clara.logging_setup import configure_logging
from clara.metering import InMemoryUsageStore, UsageMeter
from clara.providers import build_provider, reset_provider_cache
from clara.settings import Settings, reset_settings

configure_logging("WARNING", force=True)


@pytest.fixture(autouse=True)
def _isolate_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Stop ambient CLARA_* variables from leaking into tests."""
    for key in list(os.environ):
        if key.startswith("CLARA_"):
            monkeypatch.delenv(key, raising=False)
    reset_settings()
    reset_provider_cache()
    yield
    reset_settings()
    reset_provider_cache()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Local settings rooted in a temp directory."""
    cfg = Settings(env="local", provider="local", state_dir=tmp_path / "state")
    cfg.duckdb.path = ":memory:"
    cfg.catalog.kind = "memory"
    return cfg


@pytest.fixture
def catalog(tmp_path: Path) -> LocalCatalog:
    """A filesystem-backed catalog, so Parquet writes are genuinely exercised."""
    return LocalCatalog(root=tmp_path / "catalog")


@pytest.fixture
def memory_catalog() -> LocalCatalog:
    """An in-memory catalog for tests that do not need files."""
    return LocalCatalog(root=None)


@pytest.fixture
def engine(settings: Settings, catalog: LocalCatalog) -> Iterator[DuckDBEngine]:
    duckdb_engine = DuckDBEngine(settings.duckdb, catalog=catalog, settings=settings)
    yield duckdb_engine
    duckdb_engine.close()


@pytest.fixture
def router(engine: DuckDBEngine, catalog: LocalCatalog) -> EngineRouter:
    return EngineRouter({"duckdb": engine}, catalog=catalog)


@pytest.fixture
def usage_store() -> InMemoryUsageStore:
    return InMemoryUsageStore()


@pytest.fixture
def provider(settings: Settings):  # noqa: ANN201
    return build_provider(settings, "aws")


@pytest.fixture
def meter(provider, usage_store: InMemoryUsageStore) -> UsageMeter:  # noqa: ANN001
    return UsageMeter(provider, usage_store, tenant_id="ten_test", workspace_id="ws_test")


@pytest.fixture
def sample_rows() -> list[dict[str, object]]:
    return [
        {"id": 1, "customer": "acme", "amount": 120.5, "country": "TH"},
        {"id": 2, "customer": "globex", "amount": 80.0, "country": "SG"},
        {"id": 3, "customer": "acme", "amount": 45.25, "country": "TH"},
    ]
