"""The declarative platform specification (``clara.yaml``).

This file *is* the "data-engineer-less platform" requirement. A business
describes what it wants — these sources, these tables, this schedule — and Clara
derives everything else: table schemas, dependency order, warehouse sizing,
incremental strategy, maintenance jobs.

The same document is what the web console reads and writes, so a pipeline built
by clicking through the UI is a file a consultant can review, diff and commit.
No hidden state, no export step, no divergence between the UI and the API.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from clara.engines.warehouse import WarehouseSize
from clara.ids import slugify


class StreamSelection(BaseModel):
    """One stream to pull from a source."""

    name: str
    sync_mode: Literal["full_refresh", "incremental"] = "full_refresh"
    #: Defaults from the sync mode: incremental merges, full refresh replaces.
    write_mode: Literal["append", "overwrite", "merge"] | None = None
    cursor_field: str | list[str] | None = None
    primary_key: str | list[str] | None = None
    #: Destination table name; defaults to the stream name.
    table: str | None = None

    model_config = {"extra": "forbid"}

    def to_selection(self) -> dict[str, Any]:
        """Render as the dict ``configure_streams`` consumes."""
        payload: dict[str, Any] = {"name": self.name, "sync_mode": self.sync_mode}
        if self.write_mode:
            payload["write_mode"] = self.write_mode
        if self.cursor_field:
            payload["cursor_field"] = self.cursor_field
        if self.primary_key:
            payload["primary_key"] = self.primary_key
        if self.table:
            payload["table"] = self.table
        return payload


class SourceSpec(BaseModel):
    """A configured data source."""

    name: str
    connector: str
    config: dict[str, Any] = Field(default_factory=dict)
    #: Landing namespace. Defaults to the project's raw namespace.
    namespace: str | None = None
    #: Streams to sync. Empty means every discovered stream.
    streams: list[StreamSelection] = Field(default_factory=list)
    enabled: bool = True

    model_config = {"extra": "forbid"}

    @field_validator("name")
    @classmethod
    def _slug(cls, value: str) -> str:
        return slugify(value)

    @property
    def task_name(self) -> str:
        return f"ingest:{self.name}"


class TestSpec(BaseModel):
    """A data quality assertion on a model."""

    type: Literal["not_null", "unique", "accepted_values", "sql"]
    column: str | None = None
    values: list[Any] = Field(default_factory=list)
    sql: str | None = None
    name: str | None = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _check(self) -> TestSpec:
        if self.type in ("not_null", "unique", "accepted_values") and not self.column:
            raise ValueError(f"test '{self.type}' requires a column")
        if self.type == "accepted_values" and not self.values:
            raise ValueError("test 'accepted_values' requires values")
        if self.type == "sql" and not self.sql:
            raise ValueError("test 'sql' requires a sql expression returning offending rows")
        return self

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True, exclude_defaults=False)


class ModelSpec(BaseModel):
    """A SQL transformation."""

    name: str
    sql: str
    materialization: Literal["table", "view", "incremental", "ephemeral"] = "table"
    namespace: str | None = None
    description: str = ""
    unique_key: str | list[str] = Field(default_factory=list)
    partition_by: str | list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    tests: list[TestSpec] = Field(default_factory=list)

    model_config = {"extra": "forbid"}

    @field_validator("name")
    @classmethod
    def _slug(cls, value: str) -> str:
        return slugify(value)

    @field_validator("sql")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("model sql cannot be empty")
        return value

    @property
    def task_name(self) -> str:
        return f"model:{self.name}"

    def to_model(self, default_namespace: str = "analytics"):  # noqa: ANN201
        """Convert into a runnable ``clara.transform.Model``."""
        from clara.transform.models import Materialization, Model

        return Model(
            name=self.name,
            sql=self.sql,
            materialization=Materialization(self.materialization),
            namespace=self.namespace or default_namespace,
            description=self.description,
            unique_key=_as_list(self.unique_key),
            partition_by=_as_list(self.partition_by),
            tags=list(self.tags),
            depends_on=list(self.depends_on),
            tests=[t.to_dict() for t in self.tests],
        )


class WarehouseSpec(BaseModel):
    """A compute pool declaration."""

    name: str = "default"
    size: WarehouseSize = WarehouseSize.XS
    engine: Literal["auto", "duckdb", "trino"] = "auto"
    auto_suspend_seconds: int = 60
    auto_resume: bool = True
    spot: bool = False
    min_clusters: int = 1
    max_clusters: int = 1

    model_config = {"extra": "forbid"}

    def to_warehouse(self, workspace_id: str | None = None):  # noqa: ANN201
        from clara.engines.warehouse import Warehouse

        return Warehouse(
            name=self.name,
            size=self.size,
            engine=self.engine,
            workspace_id=workspace_id,
            auto_suspend_seconds=self.auto_suspend_seconds,
            auto_resume=self.auto_resume,
            spot=self.spot,
            min_clusters=self.min_clusters,
            max_clusters=self.max_clusters,
        )


class MaintenanceSpec(BaseModel):
    """Lakehouse housekeeping.

    On by default, because the two failure modes it prevents — thousands of
    small files and unbounded snapshot growth — are the reason self-managed
    lakehouses degrade over months. A business running Clara should never have
    to know these jobs exist.
    """

    enabled: bool = True
    #: Compact small files into larger ones.
    optimize: bool = True
    #: Drop snapshots older than the retention window.
    expire_snapshots: bool = True
    retain_snapshots: int = 5
    snapshot_retention: str = "7d"
    #: Delete files no snapshot references.
    remove_orphan_files: bool = False
    schedule: str = "@daily"

    model_config = {"extra": "forbid"}


class Defaults(BaseModel):
    """Project-wide defaults."""

    raw_namespace: str = "raw"
    analytics_namespace: str = "analytics"
    warehouse: str = "default"
    batch_size: int = 10_000
    #: Applied to every model unless overridden.
    materialization: Literal["table", "view", "incremental", "ephemeral"] = "table"

    model_config = {"extra": "forbid"}


class PlatformSpec(BaseModel):
    """A complete platform definition."""

    version: int = 1
    project: str = "clara"
    description: str = ""
    defaults: Defaults = Field(default_factory=Defaults)
    warehouses: list[WarehouseSpec] = Field(default_factory=lambda: [WarehouseSpec()])
    sources: list[SourceSpec] = Field(default_factory=list)
    models: list[ModelSpec] = Field(default_factory=list)
    maintenance: MaintenanceSpec = Field(default_factory=MaintenanceSpec)
    #: Cron expression for the whole pipeline. ``None`` means manual only.
    schedule: str | None = None
    #: Free-form labels, surfaced in the console and on usage attribution.
    tags: dict[str, str] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}

    @field_validator("version")
    @classmethod
    def _supported_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError(f"unsupported spec version: {value} (this build supports 1)")
        return value

    @field_validator("project")
    @classmethod
    def _slug(cls, value: str) -> str:
        return slugify(value)

    @model_validator(mode="after")
    def _check_consistency(self) -> PlatformSpec:
        """Catch the mistakes that would otherwise fail mid-run.

        Validating names, uniqueness and the schedule up front means an invalid
        spec is rejected at save time in the console, not at 2 a.m. in a
        scheduled run.
        """
        source_names = [s.name for s in self.sources]
        if len(set(source_names)) != len(source_names):
            raise ValueError(f"duplicate source names: {_dupes(source_names)}")

        model_names = [m.name for m in self.models]
        if len(set(model_names)) != len(model_names):
            raise ValueError(f"duplicate model names: {_dupes(model_names)}")

        warehouse_names = [w.name for w in self.warehouses]
        if len(set(warehouse_names)) != len(warehouse_names):
            raise ValueError(f"duplicate warehouse names: {_dupes(warehouse_names)}")

        if self.warehouses and self.defaults.warehouse not in warehouse_names:
            raise ValueError(
                f"defaults.warehouse '{self.defaults.warehouse}' is not defined; "
                f"declared warehouses: {', '.join(warehouse_names)}"
            )

        if self.schedule:
            from clara.orchestration.schedule import CronSchedule

            CronSchedule.parse(self.schedule)  # raises with a clear message

        # A model referencing a model that does not exist is almost always a
        # typo, and produces a confusing "table not found" much later.
        known = set(model_names)
        for model in self.models:
            for dependency in model.depends_on:
                if slugify(dependency) not in known:
                    raise ValueError(
                        f"model '{model.name}' depends_on unknown model '{dependency}'"
                    )
        return self

    # ------------------------------------------------------------- accessors

    def source(self, name: str) -> SourceSpec | None:
        return next((s for s in self.sources if s.name == slugify(name)), None)

    def model(self, name: str) -> ModelSpec | None:
        return next((m for m in self.models if m.name == slugify(name)), None)

    def warehouse(self, name: str | None = None) -> WarehouseSpec:
        target = name or self.defaults.warehouse
        found = next((w for w in self.warehouses if w.name == target), None)
        return found or WarehouseSpec(name=target)

    def enabled_sources(self) -> list[SourceSpec]:
        return [s for s in self.sources if s.enabled]

    def to_models(self) -> list[Any]:
        """Every model as a runnable ``Model``."""
        return [m.to_model(self.defaults.analytics_namespace) for m in self.models]

    def raw_namespace_for(self, source: SourceSpec) -> str:
        return source.namespace or self.defaults.raw_namespace

    # ------------------------------------------------------------------ graph

    def dag(self):  # noqa: ANN201
        """The end-to-end DAG: ingest tasks, then the models that read them.

        Model-to-source edges are derived from ``{{ source(...) }}`` calls, so a
        user never declares them — writing the SQL is the declaration.
        """
        from clara.orchestration.graph import DAG

        graph = DAG()
        # Map landed tables back to the ingest task that produces them.
        produced_by: dict[str, str] = {}
        for source in self.enabled_sources():
            graph.add_node(source.task_name)
            namespace = self.raw_namespace_for(source)
            if source.streams:
                for stream in source.streams:
                    table = slugify(stream.table or stream.name)
                    produced_by[f"{namespace}.{table}"] = source.task_name
            else:
                # Streams unknown until discovery; match on namespace alone.
                produced_by[f"{namespace}.*"] = source.task_name

        runnable = {m.name: m for m in self.models}
        for spec in self.models:
            graph.add_node(spec.task_name)
            model = spec.to_model(self.defaults.analytics_namespace)

            for upstream in model.model_refs():
                if upstream in runnable:
                    graph.add_edge(f"model:{upstream}", spec.task_name)

            for source_ref in model.source_refs():
                task = produced_by.get(source_ref.fqn) or produced_by.get(
                    f"{source_ref.namespace}.*"
                )
                if task:
                    graph.add_edge(task, spec.task_name)

        if self.maintenance.enabled and (self.sources or self.models):
            # Maintenance runs last: compacting before the writes finish is
            # wasted work.
            graph.add_node("maintenance")
            for leaf in [n for n in graph.nodes if n != "maintenance" and not graph.downstream(n)]:
                graph.add_edge(leaf, "maintenance")
        return graph

    # --------------------------------------------------------------- summary

    def summary(self) -> dict[str, Any]:
        """Compact description, shown by ``clara plan`` and the console."""
        graph = self.dag()
        return {
            "project": self.project,
            "description": self.description,
            "sources": [
                {
                    "name": s.name,
                    "connector": s.connector,
                    "namespace": self.raw_namespace_for(s),
                    "streams": [st.name for st in s.streams] or ["<all discovered>"],
                    "enabled": s.enabled,
                }
                for s in self.sources
            ],
            "models": [
                {
                    "name": m.name,
                    "materialization": m.materialization,
                    "namespace": m.namespace or self.defaults.analytics_namespace,
                    "tests": len(m.tests),
                }
                for m in self.models
            ],
            "warehouses": [w.model_dump(mode="json") for w in self.warehouses],
            "schedule": self.schedule,
            "maintenance": self.maintenance.model_dump(mode="json"),
            "execution_batches": graph.batches(),
            "lineage": graph.to_dict(),
        }


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def _dupes(values: list[str]) -> str:
    seen: set[str] = set()
    duplicates = {v for v in values if v in seen or seen.add(v)}  # type: ignore[func-returns-value]
    return ", ".join(sorted(duplicates))
