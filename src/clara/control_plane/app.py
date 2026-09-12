"""The FastAPI application.

Serves the REST API and the web console from one process, so ``clara serve`` is
the entire deployment for a small installation.
"""

from __future__ import annotations

import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from clara import branding
from clara.control_plane import security
from clara.control_plane.routes import router
from clara.control_plane.state import get_state, reset_state
from clara.errors import ClaraError
from clara.logging_setup import configure_logging, get_logger
from clara.settings import Settings, get_settings
from clara.version import API_VERSION, __version__

log = get_logger(__name__)

WEB_DIR = Path(__file__).parent / "web"

DESCRIPTION = """
Open, multi-cloud lakehouse platform.

* **Apache Iceberg** tables on any S3-compatible object storage
* **Trino** for scale-out SQL, **DuckDB** for single-node — routed automatically
* **Connectors** compatible with the Airbyte protocol
* **Transformations** authored in SQL, dbt-compatible
* **Transparent cost-plus pricing** — infrastructure at cost, plus a declining
  platform fee you can verify against your own cloud bill

The web console at `/` is built entirely on this API.
"""


class MaintenanceLoop:
    """Background loop for auto-suspend and storage metering.

    Two jobs that must happen on a timer rather than per request: suspending
    idle warehouses (the largest cost saving on the platform) and sampling
    storage size (which is billed per GB-hour, so it has to be observed
    periodically rather than computed at month end).
    """

    def __init__(self, interval_seconds: float = 30.0) -> None:
        self.interval = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="clara-maintenance", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        last_storage_sample = 0.0
        while not self._stop.wait(self.interval):
            try:
                state = get_state()
                state.suspend_idle_warehouses()

                # Sample storage hourly; more often would multiply GB-month
                # rows without improving billing accuracy.
                now = time.monotonic()
                if now - last_storage_sample >= 3600:
                    self._sample_storage(state)
                    last_storage_sample = now
            except Exception as exc:  # noqa: BLE001 - the loop must never die
                log.warning("maintenance loop error", extra={"error": str(exc)})

    @staticmethod
    def _sample_storage(state: Any) -> None:
        total = 0
        for ref in state.catalog.list_tables():
            try:
                total += int(state.catalog.load_table(ref).size_bytes or 0)
            except Exception:  # noqa: BLE001
                continue
        if total:
            state.meter.record_storage_snapshot(total, hours=1.0)


_maintenance = MaintenanceLoop()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start and stop platform state alongside the server."""
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.log_json)

    secret = security.bootstrap(settings)
    state = get_state(settings)

    log.info(
        "clara control plane starting",
        extra={
            "version": __version__,
            "environment": settings.env,
            "provider": state.provider.name,
            "engines": ",".join(sorted(state.router.engines)),
            "auth": "disabled" if settings.auth_disabled else "api-key",
        },
    )
    if secret:
        # Printed, not only logged: an operator with no key cannot use the API,
        # and this is the one moment the plaintext exists.
        banner = (
            f"\n{'=' * 68}\n"
            f"  Clara API key (shown once — store it now):\n\n    {secret}\n\n"
            f"  Use it as:  Authorization: Bearer {secret[:16]}...\n"
            f"  Or set CLARA_BOOTSTRAP_API_KEY to pin your own.\n"
            f"{'=' * 68}\n"
        )
        print(banner)

    _maintenance.start()
    try:
        yield
    finally:
        _maintenance.stop()
        reset_state()
        log.info("clara control plane stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the application."""
    cfg = settings or get_settings()

    app = FastAPI(
        title="Clara Data",
        description=DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )

    # ------------------------------------------------------------- error shape
    @app.exception_handler(ClaraError)
    async def clara_error_handler(request: Request, exc: ClaraError) -> JSONResponse:
        """Map the internal error taxonomy onto HTTP, uniformly."""
        if exc.status >= 500:
            # 'message' is a reserved LogRecord attribute: passing it in extra
            # makes logging raise, which would turn every 5xx into a crash
            # inside the error handler itself.
            log.error(
                "request failed",
                extra={"path": request.url.path, "code": exc.code, "detail": exc.message},
            )
        return JSONResponse(status_code=exc.status, content={"error": exc.to_dict()})

    @app.exception_handler(Exception)
    async def unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error", extra={"path": request.url.path})
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "internal_error", "message": str(exc)}},
        )

    app.include_router(router)

    # ----------------------------------------------------------------- console
    if WEB_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

        @app.get("/", include_in_schema=False)
        async def console() -> FileResponse:
            return FileResponse(str(WEB_DIR / "index.html"))

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, Any]:
        """Unauthenticated liveness probe for load balancers and containers."""
        return {"status": "ok", "version": __version__, "api": API_VERSION}

    @app.get("/api", include_in_schema=False)
    async def api_root() -> dict[str, Any]:
        return {
            "name": "Clara Data",
            "version": __version__,
            "api_version": API_VERSION,
            "docs": "/api/docs",
            "console": "/",
            "brand": {name: c.hex for name, c in branding.PALETTE.items()},
        }

    log.debug("app created", extra={"environment": cfg.env})
    return app


#: Module-level app for ``uvicorn clara.control_plane.app:app``.
app = create_app()
