"""The pipeline executor.

Runs a ``PlatformSpec`` end to end: ingest every source, build every model in
dependency order, then run lakehouse maintenance. This is the single code path
behind ``clara run``, the console's "Run pipeline" button and the scheduler, so
all three behave identically.
"""

from __future__ import annotations

from typing import Any, Callable

from clara.catalog.base import Catalog, TableRef
from clara.connectors import (
    LakehouseDestination,
    SyncRunner,
    build_source,
    configure_streams,
)
from clara.engines.router import EngineRouter
from clara.errors import ClaraError
from clara.logging_setup import get_logger
from clara.orchestration.runs import Run, RunStatus, TaskKind, TaskRun
from clara.spec.models import PlatformSpec, SourceSpec
from clara.transform.runner import TransformRunner

log = get_logger(__name__)

#: Called with the ``Run`` after every task, so the console can stream progress.
ProgressCallback = Callable[[Run], None]


class PipelineExecutor:
    """Executes a platform spec."""

    def __init__(
        self,
        spec: PlatformSpec,
        *,
        catalog: Catalog,
        router: EngineRouter,
        meter: Any | None = None,
        tenant_id: str | None = None,
        workspace_id: str | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> None:
        self.spec = spec
        self.catalog = catalog
        self.router = router
        self.meter = meter
        self.tenant_id = tenant_id
        self.workspace_id = workspace_id
        self.on_progress = on_progress

    # ---------------------------------------------------------------- running

    def run(
        self,
        *,
        trigger: str = "manual",
        state: dict[str, Any] | None = None,
        select: list[str] | None = None,
        full_refresh: bool = False,
        skip_ingest: bool = False,
        skip_transform: bool = False,
        run_id: str | None = None,
    ) -> Run:
        """Execute the pipeline, returning the completed ``Run``.

        Connector state from the previous run is threaded in so incremental
        sources resume where they stopped; the new state is returned on the run
        for the caller to persist. ``run_id`` lets a caller that already handed
        out an identifier (the API, which returns before the run finishes) keep
        using it.
        """
        run = Run(
            pipeline=self.spec.project,
            trigger=trigger,
            tenant_id=self.tenant_id,
            workspace_id=self.workspace_id,
            state=dict(state or {}),
            **({"id": run_id} if run_id else {}),
        )
        run.start()
        log.info(
            "pipeline run started",
            extra={"run": run.id, "project": self.spec.project, "trigger": trigger},
        )

        failed_tasks: set[str] = set()

        if not skip_ingest:
            for source in self.spec.enabled_sources():
                if select and source.task_name not in select and source.name not in select:
                    continue
                task = run.add_task(
                    TaskRun(name=source.task_name, kind=TaskKind.INGEST)
                )
                self._run_ingest(run, task, source)
                if task.status is RunStatus.FAILED:
                    failed_tasks.add(task.name)
                self._notify(run)

        if not skip_transform and self.spec.models:
            self._run_transforms(run, select=select, full_refresh=full_refresh, blocked=failed_tasks)
            self._notify(run)

        if self.spec.maintenance.enabled and not failed_tasks:
            task = run.add_task(TaskRun(name="maintenance", kind=TaskKind.MAINTENANCE))
            self._run_maintenance(run, task)
            self._notify(run)

        run.finish()
        log.info(
            "pipeline run finished",
            extra={
                "run": run.id,
                "status": run.status.value,
                "records": run.records_synced,
                "models": run.models_built,
            },
        )
        self._notify(run)
        return run

    # ----------------------------------------------------------------- ingest

    def _run_ingest(self, run: Run, task: TaskRun, source: SourceSpec) -> None:
        task.start()
        namespace = self.spec.raw_namespace_for(source)

        try:
            connector = build_source(source.connector, source.config)
        except ClaraError as exc:
            task.fail(f"could not build source: {exc.message}")
            return
        except Exception as exc:  # noqa: BLE001
            task.fail(f"could not build source: {exc}")
            return

        try:
            selections = [s.to_selection() for s in source.streams]
            streams = (
                configure_streams(connector, selections)
                if selections
                else connector.configured_streams()
            )
            task.log(f"{len(streams)} stream(s): {', '.join(s.stream.name for s in streams)}")

            destination = LakehouseDestination(
                self.catalog, engine=self._merge_engine()
            )
            runner = SyncRunner(
                connector,
                destination,
                namespace=namespace,
                batch_size=self.spec.defaults.batch_size,
                meter=self.meter,
                pipeline_id=source.name,
            )

            result = runner.run(streams, state=run.state.get(source.name))
            for line in result.logs:
                task.log(line)

            run.state[source.name] = result.state
            # Newly landed tables must be visible to the models that read them.
            self._refresh_engines()

            if result.succeeded:
                task.succeed(
                    records=result.records,
                    bytes_moved=result.bytes_moved,
                    tables=[s.table for s in result.streams if s.table],
                    streams=[s.to_dict() for s in result.streams],
                )
            else:
                errors = "; ".join(f"{s.stream}: {s.error}" for s in result.streams if s.error)
                task.output.update(records=result.records, streams=[s.to_dict() for s in result.streams])
                task.fail(errors or "sync failed")
        except Exception as exc:  # noqa: BLE001 - a source failure is a task failure
            log.exception("ingest task failed", extra={"source": source.name})
            task.fail(str(exc))
        finally:
            connector.close()

    # -------------------------------------------------------------- transform

    def _run_transforms(
        self,
        run: Run,
        *,
        select: list[str] | None,
        full_refresh: bool,
        blocked: set[str],
    ) -> None:
        models = self.spec.to_models()
        runner = TransformRunner(
            models,
            router=self.router,
            catalog=self.catalog,
            meter=self.meter,
        )

        selected_models: list[str] | None = None
        if select:
            selected_models = [
                name.removeprefix("model:")
                for name in select
                if name.startswith("model:") or any(m.name == name for m in models)
            ] or None
            if selected_models is None:
                return

        # A model whose source ingest failed must not build on stale data.
        graph = self.spec.dag()
        for spec_model in self.spec.models:
            upstream_failures = blocked & graph.ancestors(spec_model.task_name)
            if not upstream_failures:
                continue
            task = run.add_task(
                TaskRun(name=spec_model.task_name, kind=TaskKind.TRANSFORM)
            )
            task.skip(f"upstream failed: {', '.join(sorted(upstream_failures))}")

        skipped = {t.name for t in run.tasks if t.status is RunStatus.SKIPPED}
        buildable = [
            m.name
            for m in self.spec.models
            if m.task_name not in skipped
            and (selected_models is None or m.name in selected_models)
        ]
        if not buildable:
            return

        result = runner.run(buildable, full_refresh=full_refresh)

        for model_result in result.results:
            task = run.add_task(
                TaskRun(name=f"model:{model_result.model}", kind=TaskKind.TRANSFORM)
            )
            task.started_at = run.started_at
            if model_result.skipped and model_result.succeeded:
                task.skip("ephemeral model — inlined into consumers")
            elif model_result.succeeded:
                task.succeed(
                    table=model_result.table,
                    rows=model_result.rows,
                    materialization=model_result.materialization,
                    engine=model_result.engine,
                )
            else:
                task.output.update(
                    table=model_result.table, test_failures=model_result.test_failures
                )
                task.fail(model_result.error or "model failed")

        self._refresh_engines()

    # ------------------------------------------------------------ maintenance

    def _run_maintenance(self, run: Run, task: TaskRun) -> None:
        """Compact files and expire snapshots.

        Best-effort: a lakehouse that skipped compaction is slower, not broken,
        so maintenance failures never fail a run.
        """
        task.start()
        config = self.spec.maintenance
        optimized: list[str] = []
        expired: dict[str, int] = {}

        try:
            tables = self.catalog.list_tables()
        except Exception as exc:  # noqa: BLE001
            task.succeed(skipped=True, reason=f"could not list tables: {exc}")
            return

        for ref in tables:
            if ref.name.startswith("_clara_stage_"):
                continue
            if config.optimize:
                if self._optimize(ref):
                    optimized.append(ref.fqn)
            if config.expire_snapshots:
                count = self._expire(ref, config.retain_snapshots)
                if count:
                    expired[ref.fqn] = count

        task.log(f"compacted {len(optimized)} table(s), expired snapshots on {len(expired)}")
        task.succeed(optimized=optimized, snapshots_expired=expired)

    def _optimize(self, ref: TableRef) -> bool:
        for engine in self.router.engines.values():
            optimize = getattr(engine, "optimize", None)
            if not callable(optimize):
                continue
            try:
                optimize(ref)
                return True
            except Exception as exc:  # noqa: BLE001
                log.debug("optimize skipped", extra={"table": ref.fqn, "error": str(exc)})
        return False

    def _expire(self, ref: TableRef, retain: int) -> int:
        expire = getattr(self.catalog, "expire_snapshots", None)
        if not callable(expire):
            return 0
        try:
            return int(expire(ref, retain_last=retain))
        except Exception as exc:  # noqa: BLE001
            log.debug("snapshot expiry skipped", extra={"table": ref.fqn, "error": str(exc)})
            return 0

    # ---------------------------------------------------------------- helpers

    def _merge_engine(self) -> Any:
        """An engine capable of MERGE, for incremental upserts."""
        for engine in self.router.engines.values():
            if getattr(engine, "merge", None):
                return engine
        return next(iter(self.router.engines.values()), None)

    def _refresh_engines(self) -> None:
        for engine in self.router.engines.values():
            sync = getattr(engine, "sync_catalog", None)
            if callable(sync):
                sync(force=True)
        self.router.invalidate_stats()

    def _notify(self, run: Run) -> None:
        if self.on_progress is None:
            return
        try:
            self.on_progress(run)
        except Exception as exc:  # noqa: BLE001 - a bad listener must not fail the run
            log.debug("progress callback failed", extra={"error": str(exc)})

    # ------------------------------------------------------------------ plan

    def plan(self) -> dict[str, Any]:
        """What a run would do, without doing it."""
        graph = self.spec.dag()
        return {
            "project": self.spec.project,
            "tasks": graph.nodes,
            "batches": graph.batches(),
            "lineage_mermaid": graph.to_mermaid(),
            "sources": len(self.spec.enabled_sources()),
            "models": len(self.spec.models),
            "engines": sorted(self.router.engines),
        }
