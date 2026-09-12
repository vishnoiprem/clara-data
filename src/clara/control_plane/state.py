"""Runtime state for the control plane.

One container owning the catalog, engines, meter and run history, built once at
startup. The API, the console and the scheduler all read from it, so there is a
single view of the platform rather than each request rebuilding connections.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from clara.catalog import Catalog, get_catalog
from clara.engines import EngineRouter, get_router
from clara.engines.warehouse import Warehouse
from clara.errors import ConflictError, NotFoundError
from clara.logging_setup import get_logger
from clara.metering import (
    BillingService,
    CreditLedger,
    InMemoryUsageStore,
    Plan,
    PricingEngine,
    QuotaEngine,
    UsageMeter,
    UsageStore,
    get_rate_card,
)
from clara.orchestration import PipelineExecutor, Run, RunStatus
from clara.providers import CloudProvider, get_provider
from clara.settings import Settings, get_settings
from clara.spec import PlatformSpec, find_spec_file, load_spec, save_spec

log = get_logger(__name__)

#: Single-tenant default. Multi-tenancy keys everything by tenant id already;
#: this is the identity used when no auth context supplies one.
DEFAULT_TENANT = "ten_local"
DEFAULT_WORKSPACE = "ws_local"


class RunRegistry:
    """In-memory run history with background execution.

    Runs execute on a small thread pool so the API returns immediately and the
    console polls for progress. Bounded history keeps memory flat in a
    long-lived process.
    """

    def __init__(self, max_history: int = 200, max_workers: int = 2) -> None:
        self._runs: dict[str, Run] = {}
        self._order: list[str] = []
        self._futures: dict[str, Future] = {}
        self._lock = threading.RLock()
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="clara-run")
        self.max_history = max_history

    def record(self, run: Run) -> Run:
        with self._lock:
            if run.id not in self._runs:
                self._order.append(run.id)
            self._runs[run.id] = run
            while len(self._order) > self.max_history:
                self._runs.pop(self._order.pop(0), None)
        return run

    def get(self, run_id: str) -> Run:
        with self._lock:
            run = self._runs.get(run_id)
        if run is None:
            raise NotFoundError(f"run not found: {run_id}")
        return run

    def list(self, limit: int = 50) -> list[Run]:
        with self._lock:
            return [self._runs[i] for i in reversed(self._order[-limit:]) if i in self._runs]

    @property
    def active(self) -> list[Run]:
        return [r for r in self.list(50) if not r.status.is_terminal]

    def submit(self, run_id: str, work: Callable[[], Run]) -> None:
        """Run work on the pool, keyed by a pre-allocated run id.

        ``run_id`` is excluded from the concurrency check: the caller has
        already registered it as pending, and counting it would make every
        submission conflict with itself.
        """
        with self._lock:
            active = [
                r.id
                for r in self._runs.values()
                if r.id != run_id and not r.status.is_terminal
            ]
            if active:
                # Concurrent runs of the same pipeline would race on the same
                # tables. One at a time is the safe default.
                raise ConflictError("a run is already in progress", active_runs=active)
            self._futures[run_id] = self._pool.submit(work)

    def discard(self, run_id: str) -> None:
        """Forget a run. Used to clean up a placeholder that never started."""
        with self._lock:
            self._runs.pop(run_id, None)
            if run_id in self._order:
                self._order.remove(run_id)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)


class PlatformState:
    """Everything the control plane needs at runtime."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.provider: CloudProvider = get_provider(self.settings)
        self.catalog: Catalog = get_catalog(self.settings)
        self.router: EngineRouter = get_router(self.settings, self.catalog)

        self.rate_card = get_rate_card(self.settings.billing.rate_card)
        self.plan = Plan(self.settings.billing.plan)
        self.usage_store: UsageStore = InMemoryUsageStore()
        self.meter = UsageMeter(
            self.provider,
            self.usage_store,
            tenant_id=DEFAULT_TENANT,
            workspace_id=DEFAULT_WORKSPACE,
            minimum_billable_seconds=self.rate_card.minimum_billable_seconds,
        )
        self.pricing = PricingEngine(self.rate_card)
        self.quotas = QuotaEngine(
            self.usage_store,
            self.provider,
            rate_card=self.rate_card,
            budget_cap_usd=self.settings.billing.budget_cap,
        )
        self.ledger = CreditLedger(self.rate_card)
        self.billing = BillingService(
            self.usage_store, rate_card=self.rate_card, ledger=self.ledger
        )

        self.runs = RunRegistry()
        self.warehouses: dict[str, Warehouse] = {}
        self._spec: PlatformSpec | None = None
        self.spec_path: Path | None = None
        self._lock = threading.RLock()

        self._load_spec()
        self._load_warehouses()

    # -------------------------------------------------------------------- spec

    def _load_spec(self) -> None:
        found = find_spec_file()
        if found is None:
            log.info("no clara.yaml found; console will start with an empty pipeline")
            return
        try:
            self._spec = load_spec(found)
            self.spec_path = found
            log.info(
                "loaded spec",
                extra={
                    "path": str(found),
                    "sources": len(self._spec.sources),
                    "models": len(self._spec.models),
                },
            )
        except Exception as exc:  # noqa: BLE001 - an invalid spec must not block startup
            log.warning("could not load spec", extra={"path": str(found), "error": str(exc)})

    @property
    def spec(self) -> PlatformSpec:
        """The current spec, defaulting to an empty one."""
        with self._lock:
            if self._spec is None:
                self._spec = PlatformSpec(project=self.settings.env)
            return self._spec

    def set_spec(self, spec: PlatformSpec, *, persist: bool = True) -> PlatformSpec:
        """Replace the spec and optionally write it to disk.

        The console saves through here, so a pipeline built in the UI becomes
        the same ``clara.yaml`` the CLI runs.
        """
        with self._lock:
            self._spec = spec
            if persist:
                self.spec_path = save_spec(spec, self.spec_path or "clara.yaml")
                log.info("saved spec", extra={"path": str(self.spec_path)})
            self._load_warehouses()
            return spec

    # -------------------------------------------------------------- warehouses

    def _load_warehouses(self) -> None:
        if self._spec is None:
            self.warehouses.setdefault(
                "default", Warehouse(name="default", workspace_id=DEFAULT_WORKSPACE)
            )
            return
        for declared in self._spec.warehouses:
            existing = self.warehouses.get(declared.name)
            if existing is None:
                self.warehouses[declared.name] = declared.to_warehouse(DEFAULT_WORKSPACE)
            else:
                # Preserve runtime state (uptime, activity) across a spec save.
                existing.size = declared.size
                existing.engine = declared.engine
                existing.auto_suspend_seconds = declared.auto_suspend_seconds
                existing.spot = declared.spot

    def warehouse(self, name: str | None = None) -> Warehouse:
        target = name or (self._spec.defaults.warehouse if self._spec else "default")
        found = self.warehouses.get(target)
        if found is None:
            if name:
                raise NotFoundError(f"warehouse not found: {name}")
            found = Warehouse(name="default", workspace_id=DEFAULT_WORKSPACE)
            self.warehouses["default"] = found
        return found

    def touch_warehouse(self, warehouse: Warehouse) -> None:
        """Mark activity, resuming the warehouse if it was suspended."""
        if not warehouse.is_running:
            warehouse.mark_started()
        else:
            warehouse.mark_activity()

    def suspend_idle_warehouses(self) -> list[str]:
        """Suspend warehouses past their idle timeout.

        Called by the background maintenance loop. Auto-suspend is the single
        biggest cost lever, so it runs unconditionally rather than as an option.
        """
        suspended = []
        for warehouse in self.warehouses.values():
            if warehouse.should_suspend():
                uptime = warehouse.uptime_seconds()
                warehouse.mark_suspended()
                # Bill the uptime that has not yet been charged to a query.
                self.meter.record_warehouse_uptime(warehouse, seconds=min(uptime, 60.0))
                suspended.append(warehouse.name)
                log.info("auto-suspended warehouse", extra={"warehouse": warehouse.name})
        return suspended

    # --------------------------------------------------------------- execution

    def executor(self, on_progress: Any | None = None) -> PipelineExecutor:
        return PipelineExecutor(
            self.spec,
            catalog=self.catalog,
            router=self.router,
            meter=self.meter,
            tenant_id=DEFAULT_TENANT,
            workspace_id=DEFAULT_WORKSPACE,
            on_progress=on_progress,
        )

    @property
    def state_file(self) -> Path:
        """Where connector checkpoints are persisted, per project."""
        return self.settings.path_in_state(f"state-{self.spec.project}.json")

    def last_state(self) -> dict[str, Any]:
        """Connector state from the most recent successful run.

        Checked in memory first, then on disk. The disk copy is what makes
        ``clara run`` incremental across separate invocations — without it every
        CLI run would be a full refresh, which is both slow and, on a
        production source, rude.
        """
        for run in self.runs.list(50):
            if run.status is RunStatus.SUCCEEDED and run.state:
                return dict(run.state)

        import json

        try:
            if self.state_file.is_file():
                return dict(json.loads(self.state_file.read_text()))
        except (OSError, ValueError) as exc:
            log.warning(
                "could not read saved connector state; treating as a full refresh",
                extra={"path": str(self.state_file), "error": str(exc)},
            )
        return {}

    def save_state(self, state: dict[str, Any]) -> None:
        """Persist connector checkpoints after a successful run."""
        if not state:
            return
        import json

        try:
            self.state_file.write_text(json.dumps(state, indent=2, default=str))
        except OSError as exc:  # pragma: no cover - disk failure
            log.warning("could not save connector state", extra={"error": str(exc)})

    def start_run(self, *, trigger: str = "manual", full_refresh: bool = False,
                  select: list[str] | None = None) -> Run:
        """Queue a pipeline run and return it immediately in PENDING state."""
        placeholder = Run(pipeline=self.spec.project, trigger=trigger, tenant_id=DEFAULT_TENANT)
        self.runs.record(placeholder)

        state = self.last_state()

        def work() -> Run:
            executor = self.executor(on_progress=self.runs.record)
            try:
                # Reuse the placeholder's id so there is exactly one run object
                # and the caller's polling handle stays valid throughout.
                run = executor.run(
                    trigger=trigger,
                    state=state,
                    full_refresh=full_refresh,
                    select=select,
                    run_id=placeholder.id,
                )
            except Exception as exc:  # noqa: BLE001 - record the failure, do not lose the run
                log.exception("run crashed")
                placeholder.status = RunStatus.FAILED
                placeholder.error = str(exc)
                placeholder.finish()
                self.runs.record(placeholder)
                return placeholder

            self.runs.record(run)
            if run.status is RunStatus.SUCCEEDED:
                self.save_state(run.state)
            return run

        try:
            self.runs.submit(placeholder.id, work)
        except ConflictError:
            # Do not leave a pending placeholder behind: it would block every
            # later run with a phantom conflict.
            self.runs.discard(placeholder.id)
            raise
        return placeholder

    # ------------------------------------------------------------------ health

    def health(self) -> dict[str, Any]:
        from clara.version import API_VERSION, __version__

        return {
            "status": "healthy",
            "version": __version__,
            "api_version": API_VERSION,
            "environment": self.settings.env,
            "provider": self.provider.name,
            "catalog": self.catalog.kind,
            "engines": sorted(self.router.engines),
            "plan": self.plan.value,
            "spec_loaded": self._spec is not None,
            "spec_path": str(self.spec_path) if self.spec_path else None,
            "active_runs": len(self.runs.active),
        }

    def shutdown(self) -> None:
        self.runs.shutdown()
        self.router.close()


_STATE: PlatformState | None = None
_STATE_LOCK = threading.Lock()


def get_state(settings: Settings | None = None) -> PlatformState:
    """Process-wide platform state."""
    global _STATE
    with _STATE_LOCK:
        if _STATE is None:
            _STATE = PlatformState(settings)
        return _STATE


def reset_state() -> None:
    """Drop platform state. Used by tests."""
    global _STATE
    with _STATE_LOCK:
        if _STATE is not None:
            _STATE.shutdown()
        _STATE = None
