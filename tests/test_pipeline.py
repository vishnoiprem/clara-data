"""End-to-end pipeline tests: connectors, spec, transform, execution.

The headline test is ``test_runs_end_to_end``: source → raw tables → models →
maintenance, asserting on real row counts.
"""

from __future__ import annotations

import pytest
import yaml

from clara.catalog import TableRef
from clara.catalog.local import LocalCatalog
from clara.connectors import (
    LakehouseDestination,
    SyncMode,
    SyncRunner,
    WriteMode,
    available_sources,
    build_source,
    configure_streams,
    source_specs,
)
from clara.connectors.protocol import AirbyteMessage, MessageType
from clara.engines.duckdb_engine import DuckDBEngine
from clara.engines.router import EngineRouter
from clara.errors import ConnectorError, ValidationError
from clara.orchestration import PipelineExecutor
from clara.orchestration.schedule import CronSchedule, is_due, next_run_at
from clara.spec import STARTER_SPEC, PlatformSpec, parse_spec, save_spec
from clara.spec.loader import interpolate, load_spec
from clara.transform.models import Materialization, Model
from clara.transform.runner import TransformRunner


class TestConnectorRegistry:
    def test_built_ins_are_registered(self) -> None:
        assert {"sample", "postgres", "http_file"} <= set(available_sources())

    def test_specs_expose_form_schemas(self) -> None:
        # The console renders these as forms, so every spec needs properties.
        for spec in source_specs():
            assert spec["config_schema"].get("properties") is not None
            assert spec["title"]

    def test_unknown_source_is_rejected(self) -> None:
        with pytest.raises(ConnectorError):
            build_source("not_a_connector", {})

    def test_missing_required_config_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="missing required"):
            build_source("postgres", {"host": "localhost"})  # no database/username

    def test_unknown_config_keys_are_rejected(self) -> None:
        with pytest.raises(ValidationError, match="unknown"):
            build_source("sample", {"nonsense": 1})

    def test_secrets_are_redacted(self) -> None:
        from clara.connectors.sources.postgres import PostgresSource

        redacted = PostgresSource.spec().redact(
            {"host": "db", "password": "hunter2", "username": "u"}
        )
        assert redacted["password"] == "***"
        assert redacted["host"] == "db"


class TestProtocol:
    def test_messages_round_trip(self) -> None:
        original = AirbyteMessage.record_message("orders", {"id": 1})
        restored = AirbyteMessage.from_json(original.to_json())
        assert restored.type is MessageType.RECORD
        assert restored.record.data == {"id": 1}

    def test_noise_lines_are_skipped(self) -> None:
        # Container connectors interleave plain stdout with protocol JSON.
        assert AirbyteMessage.from_json("not json at all") is None
        assert AirbyteMessage.from_json("") is None
        assert AirbyteMessage.from_json('{"broken":') is None

    def test_merge_without_a_key_downgrades_to_append(self) -> None:
        from clara.connectors.protocol import ConfiguredStream, StreamDescriptor

        configured = ConfiguredStream(
            stream=StreamDescriptor(name="s"), write_mode=WriteMode.MERGE
        )
        # Labelling an unkeyed write "merge" would silently duplicate rows.
        assert configured.write_mode is WriteMode.APPEND

    def test_json_schema_maps_to_clara_types(self) -> None:
        from clara.catalog.schema import DataType
        from clara.connectors.protocol import StreamDescriptor

        schema = StreamDescriptor(
            name="s",
            json_schema={
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "integer"},
                    "ts": {"type": "string", "format": "date-time"},
                    "meta": {"type": "object"},
                    "ok": {"type": "boolean"},
                },
            },
        ).to_schema()
        assert schema.get("id").type is DataType.LONG
        assert schema.get("ts").type is DataType.TIMESTAMPTZ
        assert schema.get("meta").type is DataType.JSON
        assert schema.get("ok").type is DataType.BOOLEAN
        assert schema.get("id").nullable is False


class TestSampleSource:
    def test_check_and_discover(self) -> None:
        source = build_source("sample", {"orders": 10})
        assert source.check().succeeded
        assert {s.name for s in source.discover()} == {"customers", "orders"}

    def test_is_deterministic_for_a_seed(self) -> None:
        def first_order(seed: int) -> dict:
            source = build_source("sample", {"orders": 5, "seed": seed})
            for message in source.read(source.configured_streams("orders")):
                if message.type is MessageType.RECORD:
                    return message.record.data
            raise AssertionError("no records")

        assert first_order(7) == first_order(7)
        assert first_order(7) != first_order(8)


class TestIngest:
    def test_syncs_into_the_lakehouse(
        self, catalog: LocalCatalog, engine: DuckDBEngine, meter
    ) -> None:  # noqa: ANN001
        source = build_source("sample", {"orders": 50, "seed": 3})
        runner = SyncRunner(
            source, LakehouseDestination(catalog, engine=engine), namespace="raw", meter=meter
        )
        result = runner.run(source.configured_streams())

        assert result.succeeded
        assert result.records == 55  # 50 orders + 5 customers
        assert catalog.load_table(TableRef("raw", "orders")).row_count == 50
        assert catalog.load_table(TableRef("raw", "customers")).row_count == 5

    def test_incremental_sync_moves_less_the_second_time(
        self, catalog: LocalCatalog, engine: DuckDBEngine
    ) -> None:
        source = build_source("sample", {"orders": 100, "seed": 3})
        destination = LakehouseDestination(catalog, engine=engine)
        runner = SyncRunner(source, destination, namespace="raw")
        streams = configure_streams(
            source,
            [{"name": "orders", "sync_mode": "incremental",
              "cursor_field": "ordered_at", "primary_key": "order_id"}],
        )

        first = runner.run(streams)
        second = runner.run(streams, state=first.state)

        assert first.records == 100
        assert second.records < first.records
        # And the table must not grow from replayed boundary rows.
        assert catalog.load_table(TableRef("raw", "orders")).row_count == 100

    def test_repeated_syncs_are_idempotent(
        self, catalog: LocalCatalog, engine: DuckDBEngine
    ) -> None:
        source = build_source("sample", {"orders": 40, "seed": 9})
        runner = SyncRunner(
            source, LakehouseDestination(catalog, engine=engine), namespace="raw"
        )
        streams = configure_streams(
            source,
            [{"name": "orders", "sync_mode": "incremental",
              "cursor_field": "ordered_at", "primary_key": "order_id"}],
        )
        state = None
        for _ in range(3):
            result = runner.run(streams, state=state)
            state = result.state
        assert catalog.load_table(TableRef("raw", "orders")).row_count == 40

    def test_full_refresh_replaces_rather_than_appends(
        self, catalog: LocalCatalog, engine: DuckDBEngine
    ) -> None:
        source = build_source("sample", {"orders": 30, "seed": 4})
        runner = SyncRunner(
            source, LakehouseDestination(catalog, engine=engine), namespace="raw"
        )
        streams = configure_streams(source, [{"name": "orders", "sync_mode": "full_refresh"}])
        runner.run(streams)
        runner.run(streams)
        assert catalog.load_table(TableRef("raw", "orders")).row_count == 30

    def test_schema_evolution_adds_new_source_columns(
        self, catalog: LocalCatalog, engine: DuckDBEngine
    ) -> None:
        from clara.connectors.protocol import ConfiguredStream, StreamDescriptor

        destination = LakehouseDestination(catalog, engine=engine)
        stream = ConfiguredStream(
            stream=StreamDescriptor(
                name="evolving",
                json_schema={"type": "object", "properties": {"id": {"type": "integer"}}},
            )
        )
        destination.prepare(stream, "raw")
        destination.write(stream, "raw", [{"id": 1}])
        # The source starts emitting a new field; the table must widen, not fail.
        written = destination.write(stream, "raw", [{"id": 2, "added": "x"}])

        assert written.schema_changed
        assert "added" in catalog.load_table(TableRef("raw", "evolving")).schema

    def test_selecting_an_unknown_stream_fails_clearly(self) -> None:
        source = build_source("sample", {})
        with pytest.raises(ConnectorError, match="not found"):
            configure_streams(source, [{"name": "no_such_stream"}])

    def test_incremental_defaults_to_merge_when_keyed(self) -> None:
        source = build_source("sample", {})
        streams = configure_streams(
            source,
            [{"name": "orders", "sync_mode": "incremental", "primary_key": "order_id"}],
        )
        assert streams[0].write_mode is WriteMode.MERGE

    def test_unsupported_incremental_falls_back(self) -> None:
        source = build_source("sample", {})
        streams = configure_streams(source, [{"name": "customers", "sync_mode": "incremental"}])
        assert streams[0].sync_mode is SyncMode.FULL_REFRESH

    def test_preview_reads_without_writing(self, catalog: LocalCatalog) -> None:
        from clara.connectors import ConsoleDestination

        source = build_source("sample", {"orders": 100})
        runner = SyncRunner(source, ConsoleDestination(limit=0), namespace="preview")
        rows = runner.preview("orders", limit=3)
        assert len(rows) == 3
        assert catalog.list_tables() == []


class TestTransform:
    def test_compiles_source_and_ref_templating(self) -> None:
        upstream = Model(name="base", sql="SELECT 1 AS n")
        model = Model(
            name="derived",
            sql="SELECT * FROM {{ ref('base') }} JOIN {{ source('raw','orders') }} USING (n)",
        )
        compiled = model.compile(models={"base": upstream})
        assert '"analytics"."base"' in compiled
        assert '"raw"."orders"' in compiled

    def test_detects_dependencies(self) -> None:
        model = Model(name="m", sql="SELECT * FROM {{ ref('a') }}, {{ ref('b') }}")
        assert set(model.model_refs()) == {"a", "b"}

    def test_ephemeral_models_inline_as_subqueries(self) -> None:
        ephemeral = Model(
            name="cte", sql="SELECT 1 AS n", materialization=Materialization.EPHEMERAL
        )
        model = Model(name="m", sql="SELECT * FROM {{ ref('cte') }}")
        assert "(SELECT 1 AS n)" in model.compile(models={"cte": ephemeral})

    def test_incremental_blocks_are_conditional(self) -> None:
        model = Model(
            name="m",
            sql="SELECT * FROM t {% if is_incremental() %} WHERE ts > (SELECT max(ts) FROM {{ this }}) {% endif %}",
            materialization=Materialization.INCREMENTAL,
            unique_key=["id"],
        )
        assert "WHERE" not in model.compile(incremental=False)
        assert "WHERE" in model.compile(incremental=True)

    def test_variables_resolve_with_defaults(self) -> None:
        model = Model(name="m", sql="SELECT {{ var('limit', 10) }} AS n")
        assert "10" in model.compile()
        assert "99" in model.compile(variables={"limit": 99})

    def test_undefined_variable_without_default_is_an_error(self) -> None:
        model = Model(name="m", sql="SELECT {{ var('missing') }}")
        with pytest.raises(ValidationError, match="undefined variable"):
            model.compile()

    def test_builds_models_in_dependency_order(
        self, catalog: LocalCatalog, engine: DuckDBEngine, router: EngineRouter
    ) -> None:
        source = build_source("sample", {"orders": 60, "seed": 2})
        SyncRunner(
            source, LakehouseDestination(catalog, engine=engine), namespace="raw"
        ).run(source.configured_streams())
        engine.sync_catalog(force=True)

        models = [
            Model(
                name="facts",
                sql="SELECT order_id, customer_id, amount FROM {{ source('raw','orders') }}",
            ),
            Model(
                name="totals",
                sql="SELECT customer_id, sum(amount) AS revenue FROM {{ ref('facts') }} GROUP BY 1",
            ),
        ]
        runner = TransformRunner(models, router=router, catalog=catalog)
        assert runner.build_order() == ["facts", "totals"]

        result = runner.run()
        assert result.succeeded, [r.error for r in result.results]
        assert catalog.load_table(TableRef("analytics", "facts")).row_count == 60
        assert catalog.table_exists(TableRef("analytics", "totals"))

    def test_downstream_is_skipped_when_upstream_fails(
        self, catalog: LocalCatalog, router: EngineRouter
    ) -> None:
        models = [
            Model(name="broken", sql="SELECT * FROM does_not_exist_at_all"),
            Model(name="child", sql="SELECT * FROM {{ ref('broken') }}"),
        ]
        result = TransformRunner(models, router=router, catalog=catalog).run()

        assert not result.succeeded
        child = next(r for r in result.results if r.model == "child")
        assert child.skipped and "upstream failed" in child.error

    def test_data_tests_fail_the_model(
        self, catalog: LocalCatalog, engine: DuckDBEngine, router: EngineRouter
    ) -> None:
        model = Model(
            name="dupes",
            sql="SELECT 1 AS id UNION ALL SELECT 1 AS id",
            tests=[{"type": "unique", "column": "id"}],
        )
        result = TransformRunner([model], router=router, catalog=catalog).run()
        assert not result.succeeded
        assert "unique(id)" in result.results[0].test_failures[0]

    def test_passing_tests_do_not_fail_the_model(
        self, catalog: LocalCatalog, router: EngineRouter
    ) -> None:
        model = Model(
            name="clean",
            sql="SELECT 1 AS id UNION ALL SELECT 2 AS id",
            tests=[{"type": "unique", "column": "id"}, {"type": "not_null", "column": "id"}],
        )
        result = TransformRunner([model], router=router, catalog=catalog).run()
        assert result.succeeded, result.results[0].test_failures

    def test_selecting_a_model_includes_its_ancestors(
        self, catalog: LocalCatalog, router: EngineRouter
    ) -> None:
        models = [
            Model(name="a", sql="SELECT 1 AS n"),
            Model(name="b", sql="SELECT * FROM {{ ref('a') }}"),
        ]
        runner = TransformRunner(models, router=router, catalog=catalog)
        assert runner.build_order(["b"]) == ["a", "b"]

    def test_duplicate_model_names_are_rejected(
        self, catalog: LocalCatalog, router: EngineRouter
    ) -> None:
        with pytest.raises(ValidationError, match="duplicate"):
            TransformRunner(
                [Model(name="m", sql="SELECT 1"), Model(name="m", sql="SELECT 2")],
                router=router,
                catalog=catalog,
            )


class TestSchedule:
    def test_parses_standard_cron(self) -> None:
        schedule = CronSchedule.parse("0 2 * * *")
        assert schedule.hours == frozenset({2})
        assert schedule.minutes == frozenset({0})

    def test_supports_aliases_and_friendly_forms(self) -> None:
        assert CronSchedule.parse("@daily").hours == frozenset({0})
        assert CronSchedule.parse("every 15 minutes").minutes == frozenset({0, 15, 30, 45})
        assert CronSchedule.parse("every 2 hours").hours == frozenset(range(0, 24, 2))

    def test_supports_steps_ranges_and_names(self) -> None:
        assert CronSchedule.parse("*/30 * * * *").minutes == frozenset({0, 30})
        assert CronSchedule.parse("0 9-17 * * *").hours == frozenset(range(9, 18))
        assert CronSchedule.parse("0 0 * jan mon").months == frozenset({1})

    def test_rejects_malformed_expressions(self) -> None:
        for bad in ("", "0 2 * *", "99 * * * *", "0 2 * * xyz", "0 5-2 * * *"):
            with pytest.raises(ValidationError):
                CronSchedule.parse(bad)

    def test_next_firing_is_in_the_future(self) -> None:
        from clara.time_utils import utcnow

        assert next_run_at("*/5 * * * *") > utcnow()

    def test_sunday_accepts_both_zero_and_seven(self) -> None:
        assert CronSchedule.parse("0 0 * * 7").days_of_week == frozenset({0})

    def test_upcoming_firings_are_ordered(self) -> None:
        firings = CronSchedule.parse("0 * * * *").upcoming(4)
        assert firings == sorted(firings)
        assert len(firings) == 4

    def test_is_due_after_the_window_passes(self) -> None:
        from datetime import timedelta

        from clara.time_utils import utcnow

        assert is_due("*/5 * * * *", utcnow() - timedelta(hours=1))
        assert not is_due("0 0 1 1 *", utcnow() - timedelta(minutes=1))

    def test_describes_itself_readably(self) -> None:
        assert "02:00" in CronSchedule.parse("0 2 * * *").describe()


class TestSpec:
    def test_starter_spec_is_valid(self) -> None:
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        assert spec.project == "retail_demo"
        assert len(spec.sources) == 1
        assert len(spec.models) == 3

    def test_derives_the_end_to_end_dag(self) -> None:
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        batches = spec.dag().batches()
        # Ingest must precede the model reading it, which must precede its child.
        assert batches[0] == ["ingest:retail"]
        assert batches[1] == ["model:order_facts"]
        assert set(batches[2]) == {"model:category_mix", "model:daily_revenue"}
        assert batches[-1] == ["maintenance"]

    def test_rejects_duplicate_names(self) -> None:
        with pytest.raises(ValidationError, match="duplicate source"):
            parse_spec(
                {
                    "sources": [
                        {"name": "s", "connector": "sample"},
                        {"name": "s", "connector": "sample"},
                    ]
                }
            )

    def test_rejects_unknown_dependency(self) -> None:
        with pytest.raises(ValidationError, match="unknown model"):
            parse_spec(
                {"models": [{"name": "m", "sql": "SELECT 1", "depends_on": ["ghost"]}]}
            )

    def test_rejects_bad_schedule(self) -> None:
        with pytest.raises(ValidationError):
            parse_spec({"schedule": "not a cron"})

    def test_rejects_unsupported_version(self) -> None:
        with pytest.raises(ValidationError, match="unsupported spec version"):
            parse_spec({"version": 99})

    def test_rejects_undefined_default_warehouse(self) -> None:
        with pytest.raises(ValidationError, match="not defined"):
            parse_spec(
                {"defaults": {"warehouse": "ghost"}, "warehouses": [{"name": "default"}]}
            )

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            parse_spec({"nonsense_key": True})

    def test_interpolates_environment_variables(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CLARA_TEST_SECRET", "s3cret")
        assert interpolate({"k": "${CLARA_TEST_SECRET}"}) == {"k": "s3cret"}
        assert interpolate("${CLARA_ABSENT:-fallback}") == "fallback"
        # Unset with no default is left alone so `validate` works anywhere.
        assert interpolate("${CLARA_ABSENT}") == "${CLARA_ABSENT}"

    def test_round_trips_through_yaml(self, tmp_path) -> None:  # noqa: ANN001
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        path = save_spec(spec, tmp_path / "clara.yaml")
        reloaded = load_spec(path)
        assert reloaded.project == spec.project
        assert [m.name for m in reloaded.models] == [m.name for m in spec.models]

    def test_console_draft_becomes_a_valid_spec(self) -> None:
        """The console posts a draft; it must validate without hand-editing."""
        from clara.control_plane.schemas import (
            ModelDraft,
            PipelineDraft,
            SourceDraft,
            StreamSelectionPayload,
        )

        draft = PipelineDraft(
            project="from_console",
            sources=[
                SourceDraft(
                    name="sample",
                    connector="sample",
                    config={"orders": 10},
                    streams=[
                        StreamSelectionPayload(
                            name="orders",
                            sync_mode="incremental",
                            cursor_field="ordered_at",
                            primary_key="order_id",
                        )
                    ],
                )
            ],
            models=[ModelDraft(name="m", sql="SELECT * FROM {{ source('raw','orders') }}")],
            schedule="0 3 * * *",
        )
        spec = parse_spec(draft.to_spec_payload())
        assert spec.project == "from_console"
        assert spec.sources[0].streams[0].sync_mode == "incremental"


class TestExecutor:
    def test_runs_end_to_end(
        self, catalog: LocalCatalog, router: EngineRouter, engine: DuckDBEngine, meter
    ) -> None:  # noqa: ANN001
        """The headline path: source → raw tables → models → maintenance."""
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        executor = PipelineExecutor(
            spec, catalog=catalog, router=router, meter=meter, tenant_id="ten_test"
        )
        run = executor.run()

        assert run.status.value == "succeeded", run.error
        assert run.records_synced == 2005  # 2000 orders + 5 customers
        assert run.models_built == 3

        # Raw tables landed.
        assert catalog.load_table(TableRef("raw", "orders")).row_count == 2000
        assert catalog.load_table(TableRef("raw", "customers")).row_count == 5
        # Models built, including a genuine view.
        assert catalog.load_table(TableRef("analytics", "order_facts")).row_count == 2000
        assert catalog.load_table(TableRef("analytics", "category_mix")).format == "view"

        # And the results are queryable.
        revenue = engine.execute(
            "SELECT sum(revenue) FROM analytics.daily_revenue"
        ).scalar()
        assert revenue > 0

    def test_second_run_is_incremental_and_stable(
        self, catalog: LocalCatalog, router: EngineRouter, meter
    ) -> None:  # noqa: ANN001
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        executor = PipelineExecutor(
            spec, catalog=catalog, router=router, meter=meter, tenant_id="ten_test"
        )
        first = executor.run()
        second = executor.run(state=first.state)

        assert second.status.value == "succeeded", second.error
        assert second.records_synced < first.records_synced
        assert catalog.load_table(TableRef("raw", "orders")).row_count == 2000

    def test_failed_ingest_skips_dependent_models(
        self, catalog: LocalCatalog, router: EngineRouter
    ) -> None:
        spec = parse_spec(
            {
                "project": "broken",
                "sources": [
                    {"name": "bad", "connector": "http_file",
                     "config": {"url": "http://127.0.0.1:9/never"}}
                ],
                "models": [
                    {"name": "downstream", "sql": "SELECT * FROM {{ source('raw','bad') }}"}
                ],
                "maintenance": {"enabled": False},
            }
        )
        run = PipelineExecutor(spec, catalog=catalog, router=router).run()

        assert run.status.value == "failed"
        ingest = run.task("ingest:bad")
        assert ingest.status.value == "failed"
        # The model must not run against a table that never loaded.
        assert run.task("model:downstream").status.value == "skipped"

    def test_skip_flags_narrow_the_run(
        self, catalog: LocalCatalog, router: EngineRouter, meter
    ) -> None:  # noqa: ANN001
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        executor = PipelineExecutor(spec, catalog=catalog, router=router, meter=meter)

        ingest_only = executor.run(skip_transform=True)
        assert ingest_only.records_synced > 0
        assert ingest_only.models_built == 0

        transform_only = executor.run(skip_ingest=True)
        assert transform_only.models_built == 3

    def test_plan_describes_the_work(
        self, catalog: LocalCatalog, router: EngineRouter
    ) -> None:
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        plan = PipelineExecutor(spec, catalog=catalog, router=router).plan()
        assert plan["sources"] == 1
        assert plan["models"] == 3
        assert "graph LR" in plan["lineage_mermaid"]

    def test_progress_callback_fires(
        self, catalog: LocalCatalog, router: EngineRouter, meter
    ) -> None:  # noqa: ANN001
        seen: list[str] = []
        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        PipelineExecutor(
            spec,
            catalog=catalog,
            router=router,
            meter=meter,
            on_progress=lambda run: seen.append(run.status.value),
        ).run()
        assert seen, "the console relies on progress callbacks to stream a run"

    def test_metering_records_the_run(
        self, catalog: LocalCatalog, router: EngineRouter, meter, usage_store
    ) -> None:  # noqa: ANN001
        from clara.metering import Meter

        spec = parse_spec(yaml.safe_load(STARTER_SPEC))
        PipelineExecutor(
            spec, catalog=catalog, router=router, meter=meter, tenant_id="ten_test"
        ).run()

        summary = usage_store.summarize("ten_test")
        assert summary.quantity(Meter.INGEST) > 0
        assert summary.quantity(Meter.ORCHESTRATION) > 0
