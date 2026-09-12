"""Catalog, schema and type-coercion tests."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from clara.catalog import Schema, TableRef
from clara.catalog.local import LocalCatalog
from clara.catalog.schema import (
    DataType,
    Field_,
    coerce_record,
    coerce_value,
    ddl_columns,
)
from clara.errors import ConflictError, NotFoundError, ValidationError


class TestTableRef:
    def test_parses_qualified_and_bare_names(self) -> None:
        assert TableRef.parse("raw.orders").fqn == "raw.orders"
        assert TableRef.parse("orders", "main").fqn == "main.orders"
        # A catalog prefix is dropped: the catalog is implied by the client.
        assert TableRef.parse("iceberg.raw.orders").fqn == "raw.orders"
        assert TableRef.parse('"raw"."orders"').fqn == "raw.orders"

    def test_rejects_unparseable(self) -> None:
        with pytest.raises(ValidationError):
            TableRef.parse("a.b.c.d")
        with pytest.raises(ValidationError):
            TableRef("", "orders")


class TestSchema:
    def test_rejects_duplicate_columns(self) -> None:
        with pytest.raises(ValueError, match="duplicate"):
            Schema(fields=[Field_("id"), Field_("ID")])

    def test_infers_types_from_records(self) -> None:
        schema = Schema.infer(
            [
                {"a": 1, "b": "x", "c": 1.5, "d": True, "e": None},
                {"a": 2, "b": "y", "c": 2.5, "d": False, "e": None},
            ]
        )
        assert schema.get("a").type is DataType.INT
        assert schema.get("b").type is DataType.STRING
        assert schema.get("c").type is DataType.DOUBLE
        assert schema.get("d").type is DataType.BOOLEAN
        # An all-null column cannot be inferred; string is the safe default.
        assert schema.get("e").type is DataType.STRING

    def test_widens_conflicting_numeric_types(self) -> None:
        schema = Schema.infer([{"n": 1}, {"n": 2.5}])
        assert schema.get("n").type is DataType.DOUBLE

    def test_falls_back_to_string_on_mixed_types(self) -> None:
        schema = Schema.infer([{"v": 1}, {"v": "text"}])
        assert schema.get("v").type is DataType.STRING

    def test_with_fields_is_additive_and_ignores_existing(self) -> None:
        base = Schema.from_simple({"id": "long"})
        grown = base.with_fields(Field_("name"), Field_("id"))
        assert grown.names == ["id", "name"]
        assert base.names == ["id"], "original schema must not be mutated"

    def test_round_trips_through_dict(self) -> None:
        original = Schema.from_simple(
            {"id": "long", "ts": "timestamptz"}, partition_by=["days(ts)"], primary_key=["id"]
        )
        restored = Schema.from_dict(original.to_dict())
        assert restored.names == original.names
        assert restored.partition_by == ["days(ts)"]
        assert restored.primary_key == ["id"]

    def test_renders_dialect_specific_ddl(self) -> None:
        schema = Schema.from_simple({"id": "long", "ts": "timestamptz"})
        assert "BIGINT" in ddl_columns(schema, "trino")
        assert "TIMESTAMP(6) WITH TIME ZONE" in ddl_columns(schema, "trino")
        assert "TIMESTAMPTZ" in ddl_columns(schema, "duckdb")


class TestCoercion:
    """Connectors emit JSON; the lakehouse needs typed values."""

    def test_parses_iso_timestamps(self) -> None:
        value = coerce_value(DataType.TIMESTAMPTZ, "2026-09-12T10:30:00Z")
        assert isinstance(value, dt.datetime)
        assert value.tzinfo is not None
        assert value.year == 2026 and value.hour == 10

    def test_strips_timezone_for_naive_column(self) -> None:
        value = coerce_value(DataType.TIMESTAMP, "2026-09-12T10:30:00Z")
        assert value.tzinfo is None

    def test_handles_epoch_seconds_and_milliseconds(self) -> None:
        seconds = coerce_value(DataType.TIMESTAMPTZ, 1_757_670_000)
        millis = coerce_value(DataType.TIMESTAMPTZ, 1_757_670_000_000)
        assert seconds.year == millis.year == 2025

    def test_tolerates_excess_fractional_digits(self) -> None:
        value = coerce_value(DataType.TIMESTAMPTZ, "2026-09-12T10:30:00.123456789Z")
        assert value.microsecond == 123456

    def test_coerces_scalars(self) -> None:
        assert coerce_value(DataType.LONG, "42") == 42
        assert coerce_value(DataType.DOUBLE, "3.5") == 3.5
        assert coerce_value(DataType.BOOLEAN, "yes") is True
        assert coerce_value(DataType.BOOLEAN, "0") is False
        assert coerce_value(DataType.DECIMAL, "1.5") == Decimal("1.500000000")
        assert coerce_value(DataType.DATE, "2026-09-12") == dt.date(2026, 9, 12)

    def test_serialises_nested_values_as_json(self) -> None:
        assert coerce_value(DataType.JSON, {"a": 1}) == '{"a": 1}'
        assert coerce_value(DataType.STRING, [1, 2]) == "[1, 2]"

    def test_passes_through_unconvertible_values(self) -> None:
        # A single bad cell must not crash the batch; it surfaces at write time.
        assert coerce_value(DataType.LONG, "not-a-number") == "not-a-number"

    def test_projects_record_onto_schema(self) -> None:
        schema = Schema.from_simple({"id": "long", "name": "string", "ts": "timestamptz"})
        record = coerce_record(schema, {"id": "7", "extra": "dropped"})
        assert set(record) == {"id", "name", "ts"}, "must project exactly the schema"
        assert record["id"] == 7
        assert record["name"] is None


class TestLocalCatalog:
    def test_creates_lists_and_drops_tables(self, catalog: LocalCatalog) -> None:
        ref = TableRef("raw", "orders")
        catalog.create_table(ref, Schema.from_simple({"id": "long"}))
        assert catalog.table_exists(ref)
        assert [r.fqn for r in catalog.list_tables()] == ["raw.orders"]
        assert catalog.list_namespaces() == ["raw"]

        catalog.drop_table(ref, purge=True)
        assert not catalog.table_exists(ref)

    def test_rejects_duplicate_table_unless_allowed(self, catalog: LocalCatalog) -> None:
        ref = TableRef("raw", "orders")
        schema = Schema.from_simple({"id": "long"})
        catalog.create_table(ref, schema)
        with pytest.raises(ConflictError):
            catalog.create_table(ref, schema)
        catalog.create_table(ref, schema, exists_ok=True)

    def test_rejects_empty_schema(self, catalog: LocalCatalog) -> None:
        with pytest.raises(Exception, match="no columns"):
            catalog.create_table(TableRef("raw", "empty"), Schema())

    def test_missing_table_raises_not_found(self, catalog: LocalCatalog) -> None:
        with pytest.raises(NotFoundError):
            catalog.load_table(TableRef("raw", "absent"))

    def test_append_and_scan_round_trip(
        self, catalog: LocalCatalog, sample_rows: list[dict]
    ) -> None:
        ref = TableRef("raw", "orders")
        catalog.create_table(
            ref,
            Schema.from_simple(
                {"id": "long", "customer": "string", "amount": "double", "country": "string"}
            ),
        )
        assert catalog.append(ref, sample_rows) == 3
        assert catalog.load_table(ref).row_count == 3

        scanned = catalog.scan(ref)
        assert len(scanned) == 3
        assert {r["customer"] for r in scanned} == {"acme", "globex"}

    def test_evolves_schema_additively(self, catalog: LocalCatalog) -> None:
        ref = TableRef("raw", "orders")
        catalog.create_table(ref, Schema.from_simple({"id": "long"}))
        evolved = catalog.evolve_schema(
            ref, Schema.from_simple({"id": "long", "added": "string"})
        )
        assert evolved.schema.names == ["id", "added"]

        # Evolution never removes a column: losing data silently is worse than
        # carrying an unused one.
        shrunk = catalog.evolve_schema(ref, Schema.from_simple({"id": "long"}))
        assert shrunk.schema.names == ["id", "added"]

    def test_upsert_is_idempotent(self, catalog: LocalCatalog) -> None:
        ref = TableRef("raw", "orders")
        catalog.create_table(
            ref, Schema.from_simple({"id": "long", "amount": "double"}),
        )
        rows = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}]
        catalog.append(ref, rows)

        # Re-applying the same keys must not duplicate, and must update values.
        catalog.upsert(ref, [{"id": 1, "amount": 99.0}], ["id"])
        scanned = {r["id"]: r["amount"] for r in catalog.scan(ref)}
        assert scanned == {1: 99.0, 2: 20.0}

    def test_views_store_sql_not_rows(self, catalog: LocalCatalog) -> None:
        ref = TableRef("analytics", "summary")
        catalog.create_view(ref, "SELECT 1 AS n", Schema.from_simple({"n": "long"}))
        info = catalog.load_table(ref)
        assert info.format == "view"
        assert info.properties["clara.view-sql"] == "SELECT 1 AS n"
        assert catalog.is_view(ref)

    def test_drop_namespace_requires_cascade_when_not_empty(
        self, catalog: LocalCatalog
    ) -> None:
        catalog.create_table(TableRef("raw", "orders"), Schema.from_simple({"id": "long"}))
        with pytest.raises(ConflictError):
            catalog.drop_namespace("raw")
        catalog.drop_namespace("raw", cascade=True)
        assert "raw" not in catalog.list_namespaces()

    def test_metadata_survives_reopen(self, tmp_path) -> None:  # noqa: ANN001
        root = tmp_path / "catalog"
        first = LocalCatalog(root=root)
        ref = TableRef("raw", "orders")
        first.create_table(ref, Schema.from_simple({"id": "long", "name": "string"}))
        first.append(ref, [{"id": 1, "name": "a"}])

        # A new process must see the same tables and rows.
        second = LocalCatalog(root=root)
        assert second.table_exists(ref)
        assert second.load_table(ref).row_count == 1
        assert second.load_table(ref).schema.names == ["id", "name"]

    def test_in_memory_mode_needs_no_files(self, memory_catalog: LocalCatalog) -> None:
        ref = TableRef("raw", "orders")
        memory_catalog.create_table(ref, Schema.from_simple({"id": "long"}))
        memory_catalog.append(ref, [{"id": 1}])
        assert memory_catalog.scan(ref) == [{"id": 1}]

    def test_ensure_table_creates_then_evolves(self, catalog: LocalCatalog) -> None:
        ref = TableRef("raw", "orders")
        catalog.ensure_table(ref, Schema.from_simple({"id": "long"}))
        catalog.ensure_table(ref, Schema.from_simple({"id": "long", "extra": "string"}))
        assert catalog.load_table(ref).schema.names == ["id", "extra"]
