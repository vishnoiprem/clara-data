"""Model execution.

Builds a dependency graph from ``ref()`` calls, runs models in topological
order, and materialises each according to its configuration. Incremental models
merge when they have a key and append when they do not.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from clara.catalog.base import Catalog, TableRef
from clara.engines.base import Engine
from clara.engines.router import EngineRouter
from clara.errors import ValidationError
from clara.logging_setup import get_logger
from clara.orchestration.graph import DAG
from clara.time_utils import format_duration, utcnow
from clara.transform.models import Materialization, Model

log = get_logger(__name__)


@dataclass
class ModelResult:
    """Outcome of building one model."""

    model: str
    table: str
    materialization: str
    succeeded: bool = True
    rows: int | None = None
    duration_seconds: float = 0.0
    engine: str | None = None
    skipped: bool = False
    error: str | None = None
    test_failures: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "table": self.table,
            "materialization": self.materialization,
            "succeeded": self.succeeded,
            "skipped": self.skipped,
            "rows": self.rows,
            "duration_seconds": round(self.duration_seconds, 3),
            "duration": format_duration(self.duration_seconds),
            "engine": self.engine,
            "error": self.error,
            "test_failures": self.test_failures,
        }


@dataclass
class TransformResult:
    """Outcome of a full transform run."""

    results: list[ModelResult] = field(default_factory=list)
    duration_seconds: float = 0.0
    started_at: Any = None

    @property
    def succeeded(self) -> bool:
        return all(r.succeeded for r in self.results)

    @property
    def built(self) -> int:
        return sum(1 for r in self.results if r.succeeded and not r.skipped)

    def to_dict(self) -> dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "models_built": self.built,
            "models_total": len(self.results),
            "duration_seconds": round(self.duration_seconds, 3),
            "duration": format_duration(self.duration_seconds),
            "results": [r.to_dict() for r in self.results],
        }


class TransformRunner:
    """Compiles and executes a set of models."""

    def __init__(
        self,
        models: list[Model],
        *,
        router: EngineRouter | None = None,
        engine: Engine | None = None,
        catalog: Catalog | None = None,
        variables: dict[str, Any] | None = None,
        meter: Any | None = None,
    ) -> None:
        if router is None and engine is None:
            raise ValidationError("transform runner requires a router or an engine")
        self.router = router
        self.engine = engine
        self.catalog = catalog
        self.variables = dict(variables or {})
        self.meter = meter
        self.models = {m.name: m for m in models}
        if len(self.models) != len(models):
            raise ValidationError("duplicate model names")

    # ------------------------------------------------------------------- graph

    def dag(self) -> DAG:
        """Dependency graph over the model set."""
        graph = DAG()
        for name in self.models:
            graph.add_node(name)
        for name, model in self.models.items():
            for upstream in model.model_refs():
                # A ref to something outside this set is a source, not an edge.
                if upstream in self.models:
                    graph.add_edge(upstream, name)
        return graph

    def build_order(self, select: list[str] | None = None) -> list[str]:
        """Models in execution order, optionally restricted to a selection.

        Selecting a model implicitly selects its upstream dependencies —
        building a model against a stale parent produces a quietly wrong table.
        """
        graph = self.dag()
        order = graph.topological_order()
        if not select:
            return order
        wanted: set[str] = set()
        for name in select:
            if name not in self.models:
                raise ValidationError(f"unknown model: {name}", available=sorted(self.models))
            wanted.add(name)
            wanted.update(graph.ancestors(name))
        return [n for n in order if n in wanted]

    # --------------------------------------------------------------- execution

    def run(
        self,
        select: list[str] | None = None,
        *,
        full_refresh: bool = False,
        dry_run: bool = False,
    ) -> TransformResult:
        """Build models in dependency order.

        A failed model marks its downstream dependents skipped rather than
        running them against missing or stale inputs.
        """
        outcome = TransformResult(started_at=utcnow())
        started = time.perf_counter()
        graph = self.dag()
        failed: set[str] = set()

        for name in self.build_order(select):
            model = self.models[name]

            if model.materialization is Materialization.EPHEMERAL:
                outcome.results.append(
                    ModelResult(
                        model=name,
                        table="(ephemeral)",
                        materialization=model.materialization.value,
                        skipped=True,
                    )
                )
                continue

            blocked = failed & set(graph.ancestors(name))
            if blocked:
                outcome.results.append(
                    ModelResult(
                        model=name,
                        table=model.ref.fqn,
                        materialization=model.materialization.value,
                        succeeded=False,
                        skipped=True,
                        error=f"upstream failed: {', '.join(sorted(blocked))}",
                    )
                )
                failed.add(name)
                continue

            result = self._build(model, full_refresh=full_refresh, dry_run=dry_run)
            outcome.results.append(result)
            if not result.succeeded:
                failed.add(name)

        outcome.duration_seconds = time.perf_counter() - started
        log.info(
            "transform run complete",
            extra={
                "built": outcome.built,
                "total": len(outcome.results),
                "succeeded": outcome.succeeded,
            },
        )
        return outcome

    def _build(self, model: Model, *, full_refresh: bool, dry_run: bool) -> ModelResult:
        result = ModelResult(
            model=model.name,
            table=model.ref.fqn,
            materialization=model.materialization.value,
        )
        started = time.perf_counter()

        try:
            exists = self._table_exists(model.ref)
            incremental = (
                model.materialization is Materialization.INCREMENTAL
                and exists
                and not full_refresh
            )
            engine = self._engine_for(model, incremental=incremental)
            sql = model.compile(
                models=self.models,
                variables=self.variables,
                incremental=incremental,
                quote=engine.quote_ref,
            )

            if dry_run:
                result.engine = engine.name
                result.skipped = True
                result.duration_seconds = time.perf_counter() - started
                return result

            query_result = engine.materialize(
                model.ref,
                sql,
                mode=model.materialization.value,
                unique_key=model.unique_key,
                incremental=incremental,
            )

            result.engine = engine.name
            result.rows = self._row_count(model, engine)
            result.duration_seconds = time.perf_counter() - started
            self._meter(model, query_result, engine)

            if self.catalog is not None:
                # Newly created tables must appear in the catalog for the next
                # model — and for the router's size estimates.
                self._refresh_catalog(engine)

            result.test_failures = self._run_tests(model, engine)
            if result.test_failures:
                result.succeeded = False
                result.error = f"{len(result.test_failures)} test(s) failed"

        except Exception as exc:  # noqa: BLE001 - one model must not kill the run
            result.succeeded = False
            result.error = str(exc)
            result.duration_seconds = time.perf_counter() - started
            log.warning("model failed", extra={"model": model.name, "error": str(exc)})

        return result

    def _row_count(self, model: Model, engine: Engine) -> int | None:
        if model.materialization is Materialization.VIEW:
            return None
        try:
            return engine.count(model.ref)
        except Exception:  # noqa: BLE001 - row count is informational
            return None

    def _table_exists(self, ref: TableRef) -> bool:
        if self.catalog is not None:
            try:
                return self.catalog.table_exists(ref)
            except Exception:  # noqa: BLE001
                return False
        engine = self.engine or next(iter(self.router.engines.values()))  # type: ignore[union-attr]
        try:
            engine.execute(f"SELECT 1 FROM {engine.quote_ref(ref)} LIMIT 0")
            return True
        except Exception:  # noqa: BLE001
            return False

    def _engine_for(self, model: Model, *, incremental: bool) -> Engine:
        """Pick the engine for a model.

        Incremental merges need MERGE support, so they prefer a distributed
        engine even when the data would fit single-node.
        """
        if self.engine is not None:
            return self.engine
        assert self.router is not None
        sql = model.compile(models=self.models, variables=self.variables, incremental=incremental)
        if incremental and model.unique_key:
            merge_capable = [
                e for e in self.router.engines.values() if getattr(e, "merge", None)
            ]
            if merge_capable:
                return merge_capable[0]
        return self.router.route(sql, default_namespace=model.namespace).engine

    def _refresh_catalog(self, engine: Engine) -> None:
        sync = getattr(engine, "sync_catalog", None)
        if callable(sync):
            sync(force=True)
        if self.router is not None:
            self.router.invalidate_stats()

    def _meter(self, model: Model, query_result: Any, engine: Engine) -> None:
        if self.meter is None:
            return
        self.meter.record_task(
            seconds=query_result.stats.wall_seconds,
            task_name=f"model:{model.name}",
        )

    # ------------------------------------------------------------------- tests

    def _run_tests(self, model: Model, engine: Engine) -> list[str]:
        """Evaluate a model's data tests, returning failure descriptions.

        Four tests cover most real data quality problems: not_null, unique,
        accepted_values and a free-form SQL assertion. Running them
        automatically after each build is what lets a business trust a table
        without a data team reviewing it.
        """
        failures: list[str] = []
        target = engine.quote_ref(model.ref)

        for test in model.tests:
            kind = str(test.get("type", "")).lower()
            column = test.get("column")
            try:
                if kind == "not_null" and column:
                    bad = engine.execute(
                        f"SELECT count(*) FROM {target} WHERE {engine.quote(column)} IS NULL"
                    ).scalar()
                    if bad:
                        failures.append(f"not_null({column}): {bad:,} null rows")

                elif kind == "unique" and column:
                    bad = engine.execute(
                        f"SELECT count(*) FROM (SELECT {engine.quote(column)} FROM {target} "
                        f"GROUP BY 1 HAVING count(*) > 1) AS d"
                    ).scalar()
                    if bad:
                        failures.append(f"unique({column}): {bad:,} duplicated values")

                elif kind == "accepted_values" and column:
                    values = test.get("values") or []
                    if values:
                        literals = ", ".join(_quote_literal(v) for v in values)
                        bad = engine.execute(
                            f"SELECT count(*) FROM {target} "
                            f"WHERE {engine.quote(column)} NOT IN ({literals})"
                        ).scalar()
                        if bad:
                            failures.append(f"accepted_values({column}): {bad:,} unexpected values")

                elif kind in ("sql", "assert") and test.get("sql"):
                    # Convention: the assertion query returns offending rows.
                    bad = engine.execute(
                        f"SELECT count(*) FROM ({test['sql']}) AS _clara_test"
                    ).scalar()
                    if bad:
                        failures.append(f"{test.get('name', 'sql test')}: {bad:,} failing rows")

            except Exception as exc:  # noqa: BLE001
                failures.append(f"{kind or 'test'} errored: {exc}")

        if failures:
            log.warning("model tests failed", extra={"model": model.name, "failures": failures})
        return failures


def _quote_literal(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
