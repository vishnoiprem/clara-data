"""Engine, routing and DAG tests."""

from __future__ import annotations

import pytest

from clara.catalog import Schema, TableRef
from clara.catalog.local import LocalCatalog
from clara.engines.duckdb_engine import DuckDBEngine
from clara.engines.router import EngineRouter
from clara.engines.warehouse import Warehouse, WarehouseSize
from clara.errors import EngineError, ValidationError
from clara.orchestration.graph import DAG


@pytest.fixture
def loaded(catalog: LocalCatalog, engine: DuckDBEngine, sample_rows: list[dict]) -> TableRef:
    ref = TableRef("raw", "orders")
    catalog.create_table(
        ref,
        Schema.from_simple(
            {"id": "long", "customer": "string", "amount": "double", "country": "string"}
        ),
    )
    catalog.append(ref, sample_rows)
    engine.sync_catalog(force=True)
    return ref


class TestDuckDBEngine:
    def test_connects(self, engine: DuckDBEngine) -> None:
        assert engine.test_connection()

    def test_queries_catalog_tables(self, engine: DuckDBEngine, loaded: TableRef) -> None:
        result = engine.execute("SELECT count(*) FROM raw.orders")
        assert result.scalar() == 3

    def test_aggregates_correctly(self, engine: DuckDBEngine, loaded: TableRef) -> None:
        result = engine.execute(
            "SELECT customer, sum(amount) AS total FROM raw.orders GROUP BY 1 ORDER BY 2 DESC"
        )
        assert result.columns == ["customer", "total"]
        assert result.rows[0] == ("acme", pytest.approx(165.75))

    def test_result_helpers(self, engine: DuckDBEngine, loaded: TableRef) -> None:
        result = engine.execute("SELECT customer, amount FROM raw.orders ORDER BY id")
        assert len(result) == 3
        assert result.dicts()[0]["customer"] == "acme"
        assert result.column("amount")[1] == pytest.approx(80.0)

    def test_max_rows_truncates_without_losing_the_count(
        self, engine: DuckDBEngine, loaded: TableRef
    ) -> None:
        result = engine.execute("SELECT * FROM raw.orders", max_rows=2)
        assert len(result.rows) == 2

    def test_empty_table_is_still_queryable(
        self, catalog: LocalCatalog, engine: DuckDBEngine
    ) -> None:
        # A pipeline's first run queries tables before any data lands; this must
        # return zero rows with correct types rather than failing.
        ref = TableRef("raw", "fresh")
        catalog.create_table(ref, Schema.from_simple({"id": "long", "name": "string"}))
        engine.sync_catalog(force=True)

        result = engine.execute("SELECT * FROM raw.fresh")
        assert result.rows == []
        assert [c.lower() for c in result.columns] == ["id", "name"]

    def test_rejects_empty_statement(self, engine: DuckDBEngine) -> None:
        with pytest.raises(EngineError):
            engine.execute("   ")

    def test_wraps_sql_errors(self, engine: DuckDBEngine) -> None:
        with pytest.raises(EngineError):
            engine.execute("SELECT * FROM nonexistent.table_xyz")

    def test_records_stats(self, engine: DuckDBEngine, loaded: TableRef) -> None:
        result = engine.execute("SELECT * FROM raw.orders")
        assert result.stats.engine == "duckdb"
        assert result.stats.wall_seconds > 0
        assert result.stats.rows_produced == 3

    def test_materializes_table_through_the_catalog(
        self, engine: DuckDBEngine, catalog: LocalCatalog, loaded: TableRef
    ) -> None:
        # Model output must land in Clara's catalog, not only inside DuckDB,
        # or it would be invisible to other engines and lost on restart.
        target = TableRef("analytics", "by_country")
        engine.materialize(
            target, "SELECT country, sum(amount) AS revenue FROM raw.orders GROUP BY 1"
        )
        assert catalog.table_exists(target)
        info = catalog.load_table(target)
        assert info.format == "local"
        assert info.row_count == 2
        assert engine.execute("SELECT count(*) FROM analytics.by_country").scalar() == 2

    def test_materializes_view_as_a_view(
        self, engine: DuckDBEngine, catalog: LocalCatalog, loaded: TableRef
    ) -> None:
        target = TableRef("analytics", "as_view")
        engine.materialize(target, "SELECT * FROM raw.orders", mode="view")
        assert catalog.load_table(target).format == "view"
        assert engine.execute("SELECT count(*) FROM analytics.as_view").scalar() == 3

    def test_incremental_materialize_upserts_on_key(
        self, engine: DuckDBEngine, catalog: LocalCatalog, loaded: TableRef
    ) -> None:
        target = TableRef("analytics", "facts")
        sql = "SELECT id, amount FROM raw.orders"
        engine.materialize(target, sql, unique_key=["id"])
        assert catalog.load_table(target).row_count == 3

        # Re-running with the same keys must not duplicate.
        engine.materialize(target, sql, unique_key=["id"], incremental=True)
        assert catalog.load_table(target).row_count == 3

    def test_ensure_namespace_is_idempotent(self, engine: DuckDBEngine) -> None:
        engine.ensure_namespace("brand_new")
        engine.ensure_namespace("brand_new")

    def test_view_registered_after_tables_on_resync(
        self, engine: DuckDBEngine, catalog: LocalCatalog, loaded: TableRef
    ) -> None:
        # A view's SQL references tables, so ordering matters when re-syncing.
        view = TableRef("analytics", "v")
        catalog.create_view(
            view, "SELECT * FROM raw.orders", Schema.from_simple({"id": "long"})
        )
        assert engine.sync_catalog(force=True) >= 2
        assert engine.execute("SELECT count(*) FROM analytics.v").scalar() == 3


class TestRouter:
    def test_requires_an_engine(self, catalog: LocalCatalog) -> None:
        with pytest.raises(EngineError):
            EngineRouter({}, catalog=catalog)

    def test_extracts_referenced_tables(self, router: EngineRouter) -> None:
        tables = router.referenced_tables(
            "SELECT * FROM raw.orders o JOIN raw.customers c USING (id)"
        )
        assert {t.fqn for t in tables} == {"raw.orders", "raw.customers"}

    def test_uses_default_namespace_for_bare_names(self, router: EngineRouter) -> None:
        tables = router.referenced_tables("SELECT * FROM orders", "raw")
        assert tables[0].fqn == "raw.orders"

    def test_single_engine_is_always_chosen(self, router: EngineRouter) -> None:
        decision = router.route("SELECT 1")
        assert decision.engine.name == "duckdb"
        assert "only configured engine" in decision.reason

    def test_pinned_engine_wins(self, router: EngineRouter) -> None:
        warehouse = Warehouse(name="wh", engine="duckdb")
        assert router.route("SELECT 1", warehouse=warehouse).reason.startswith("pinned")

    def test_pinning_an_absent_engine_fails_loudly(self, router: EngineRouter) -> None:
        with pytest.raises(EngineError):
            router.route("SELECT 1", warehouse=Warehouse(name="wh", engine="trino"))

    def test_routes_small_scans_to_duckdb(self, catalog: LocalCatalog, engine: DuckDBEngine) -> None:
        class FakeTrino:
            name = "trino"
            from clara.engines.base import EngineCapabilities

            capabilities = EngineCapabilities(distributed=True)

            def close(self) -> None: ...

        router = EngineRouter({"duckdb": engine, "trino": FakeTrino()}, catalog=catalog)
        decision = router.route("SELECT * FROM raw.orders")
        assert decision.engine.name == "duckdb"
        assert "single-node" in decision.reason

    def test_routes_large_scans_to_trino(self, catalog: LocalCatalog, engine: DuckDBEngine) -> None:
        class FakeTrino:
            name = "trino"
            from clara.engines.base import EngineCapabilities

            capabilities = EngineCapabilities(distributed=True)

            def close(self) -> None: ...

        router = EngineRouter(
            {"duckdb": engine, "trino": FakeTrino()}, catalog=catalog, scan_limit_bytes=10
        )
        ref = TableRef("raw", "big")
        catalog.create_table(ref, Schema.from_simple({"id": "long"}))
        catalog.append(ref, [{"id": i} for i in range(100)])

        decision = router.route("SELECT * FROM raw.big")
        assert decision.engine.name == "trino"
        assert "exceeds single-node" in decision.reason

    def test_maintenance_statements_require_trino(
        self, catalog: LocalCatalog, engine: DuckDBEngine
    ) -> None:
        class FakeTrino:
            name = "trino"
            from clara.engines.base import EngineCapabilities

            capabilities = EngineCapabilities(distributed=True)

            def close(self) -> None: ...

        router = EngineRouter({"duckdb": engine, "trino": FakeTrino()}, catalog=catalog)
        decision = router.route("ALTER TABLE raw.orders EXECUTE optimize")
        assert decision.engine.name == "trino"

    def test_invalidating_stats_clears_the_cache(self, router: EngineRouter) -> None:
        router.estimate_bytes([TableRef("raw", "orders")])
        router.invalidate_stats()
        assert router._size_cache == {}

    def test_recommends_a_warehouse_size(self, router: EngineRouter, loaded: TableRef) -> None:
        assert router.recommend_warehouse_size("SELECT * FROM raw.orders") is WarehouseSize.XS


class TestDAG:
    def test_orders_dependencies(self) -> None:
        graph = DAG()
        graph.add_edge("a", "b")
        graph.add_edge("b", "c")
        assert graph.topological_order() == ["a", "b", "c"]

    def test_detects_cycles(self) -> None:
        graph = DAG()
        graph.add_edge("a", "b")
        graph.add_edge("b", "a")
        assert graph.has_cycle()
        with pytest.raises(ValidationError, match="cycle"):
            graph.topological_order()

    def test_rejects_self_dependency(self) -> None:
        with pytest.raises(ValidationError):
            DAG().add_edge("a", "a")

    def test_batches_group_independent_nodes(self) -> None:
        graph = DAG()
        graph.add_edge("source", "left")
        graph.add_edge("source", "right")
        graph.add_edge("left", "join")
        graph.add_edge("right", "join")
        assert graph.batches() == [["source"], ["left", "right"], ["join"]]

    def test_ancestors_and_descendants_are_transitive(self) -> None:
        graph = DAG()
        graph.add_edge("a", "b")
        graph.add_edge("b", "c")
        assert graph.ancestors("c") == {"a", "b"}
        assert graph.descendants("a") == {"b", "c"}

    def test_order_is_deterministic(self) -> None:
        """Reproducible builds: ties break alphabetically, not by dict order."""
        for _ in range(5):
            graph = DAG()
            for node in ("z", "y", "x"):
                graph.add_edge("root", node)
            assert graph.topological_order() == ["root", "x", "y", "z"]

    def test_roots_and_leaves(self) -> None:
        graph = DAG()
        graph.add_edge("a", "b")
        assert graph.roots() == ["a"]
        assert graph.leaves() == ["b"]

    def test_mermaid_escapes_unsafe_ids(self) -> None:
        graph = DAG()
        graph.add_edge("ingest:my-source", "model:facts")
        diagram = graph.to_mermaid()
        assert "graph LR" in diagram
        assert "ingest_my_source" in diagram
