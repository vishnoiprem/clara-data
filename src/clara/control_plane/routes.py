"""The REST API.

Grouped by concern rather than split across many modules, because the routes are
thin: each one validates, delegates to a layer that already has tests, and
serialises. The console is written entirely against these endpoints, so anything
the UI can do is scriptable.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, Query

from clara import branding
from clara.catalog.base import TableRef
from clara.connectors import build_source, source_specs
from clara.control_plane.schemas import (
    ConnectorConfigRequest,
    CreditPurchaseRequest,
    EstimateRequest,
    PipelineDraft,
    PreviewRequest,
    QueryRequest,
    RunRequest,
    SqlPreviewRequest,
)
from clara.control_plane.security import require_auth
from clara.control_plane.state import PlatformState, get_state
from clara.engines.warehouse import recommend_size
from clara.errors import ClaraError, ValidationError
from clara.logging_setup import get_logger
from clara.metering import Plan, competitor_comparison
from clara.metering.events import Meter
from clara.providers import available_providers, build_provider, compare_providers
from clara.spec import parse_spec
from clara.version import API_VERSION

log = get_logger(__name__)

router = APIRouter(prefix=f"/api/{API_VERSION}", dependencies=[Depends(require_auth)])


def state() -> PlatformState:
    return get_state()


# ------------------------------------------------------------------- platform


@router.get("/health", tags=["platform"])
def health(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Liveness and configuration summary."""
    return st.health()


@router.get("/theme", tags=["platform"])
def theme() -> dict[str, Any]:
    """Brand colour tokens, so any UI renders Clara consistently."""
    return {"tokens": branding.theme_tokens(), "series": [c.hex for c in branding.SERIES]}


@router.get("/providers", tags=["platform"])
def providers(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Available infrastructure providers and the active one."""
    return {
        "active": st.provider.describe(),
        "available": available_providers(),
    }


@router.get("/providers/compare", tags=["platform"])
def providers_compare(st: PlatformState = Depends(state)) -> list[dict[str, Any]]:
    """Cost of the same warehouse across every provider, cheapest first."""
    return compare_providers(st.settings)


# ----------------------------------------------------------------- connectors


@router.get("/connectors", tags=["connectors"])
def connectors() -> dict[str, Any]:
    """Every registered source connector and its configuration schema.

    The console renders these schemas as forms, which is why a user can connect
    a database without reading documentation.
    """
    return {"sources": source_specs()}


@router.post("/connectors/{name}/check", tags=["connectors"])
def connector_check(name: str, payload: ConnectorConfigRequest) -> dict[str, Any]:
    """Test credentials and reachability."""
    source = build_source(name, payload.config)
    try:
        return source.check().to_dict()
    finally:
        source.close()


@router.post("/connectors/{name}/discover", tags=["connectors"])
def connector_discover(name: str, payload: ConnectorConfigRequest) -> dict[str, Any]:
    """List the streams a source exposes, with inferred schemas."""
    source = build_source(name, payload.config)
    try:
        streams = source.discover()
        return {
            "streams": [
                {
                    **descriptor.to_dict(),
                    "supports_incremental": descriptor.supports_incremental,
                    "columns": [f.to_dict() for f in descriptor.to_schema().fields],
                }
                for descriptor in streams
            ]
        }
    finally:
        source.close()


@router.post("/connectors/{name}/preview", tags=["connectors"])
def connector_preview(name: str, payload: PreviewRequest) -> dict[str, Any]:
    """Read a few rows without writing anything."""
    from clara.connectors import ConsoleDestination, SyncRunner

    source = build_source(name, payload.config)
    try:
        runner = SyncRunner(source, ConsoleDestination(limit=0), namespace="preview")
        rows = runner.preview(payload.stream, limit=payload.limit)
        columns = list(rows[0].keys()) if rows else []
        return {"stream": payload.stream, "columns": columns, "rows": rows, "row_count": len(rows)}
    finally:
        source.close()


# --------------------------------------------------------------------- catalog


@router.get("/catalog/namespaces", tags=["catalog"])
def namespaces(st: PlatformState = Depends(state)) -> dict[str, Any]:
    return {"namespaces": st.catalog.list_namespaces()}


@router.get("/catalog/tables", tags=["catalog"])
def tables(
    namespace: str | None = Query(default=None), st: PlatformState = Depends(state)
) -> dict[str, Any]:
    """Every table, with row counts and sizes."""
    result = []
    for ref in st.catalog.list_tables(namespace):
        if ref.name.startswith("_clara_stage_"):
            continue
        try:
            info = st.catalog.load_table(ref)
        except ClaraError:
            continue
        result.append(
            {
                "namespace": ref.namespace,
                "name": ref.name,
                "fqn": ref.fqn,
                "format": info.format,
                "row_count": info.row_count,
                "size_bytes": info.size_bytes,
                "columns": len(info.schema),
            }
        )
    return {"tables": result}


@router.get("/catalog/tables/{namespace}/{name}", tags=["catalog"])
def table_detail(namespace: str, name: str, st: PlatformState = Depends(state)) -> dict[str, Any]:
    return st.catalog.load_table(TableRef(namespace, name)).to_dict()


@router.get("/catalog/tables/{namespace}/{name}/preview", tags=["catalog"])
def table_preview(
    namespace: str,
    name: str,
    limit: int = Query(default=50, ge=1, le=1_000),
    st: PlatformState = Depends(state),
) -> dict[str, Any]:
    """First rows of a table, for the data browser."""
    ref = TableRef(namespace, name)
    engine = next(iter(st.router.engines.values()))
    result = engine.preview(ref, limit=limit)
    return result.to_dict(max_rows=limit)


# ----------------------------------------------------------------------- query


@router.post("/query", tags=["query"])
def run_query(payload: QueryRequest, st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Execute SQL, with routing, quota enforcement and cost attribution.

    Every query goes through the same path as the console's SQL editor: the
    router picks an engine, the quota engine authorises it, and the meter
    records what it consumed.
    """
    warehouse = st.warehouse(payload.warehouse)
    namespace = st.spec.defaults.analytics_namespace

    decision = st.router.route(payload.sql, warehouse=warehouse, default_namespace=namespace)
    estimate = decision.engine.estimate(payload.sql)

    if payload.explain_only:
        return {
            "routing": decision.to_dict(),
            "estimate": estimate.to_dict(),
            "plan": decision.engine.explain(payload.sql),
            "cost": st.pricing.estimate_query_cost(
                ccu_minutes=_estimated_ccu_minutes(estimate, warehouse),
                provider=st.provider,
                plan=st.plan,
                spot=warehouse.spot,
            ),
        }

    st.quotas.authorize_query(
        st.meter.tenant_id,
        st.plan,
        estimated_ccu_minutes=_estimated_ccu_minutes(estimate, warehouse),
        warehouse_size=warehouse.size,
    )

    st.touch_warehouse(warehouse)
    result = decision.engine.execute(payload.sql, max_rows=payload.max_rows)
    result.stats.engine = decision.engine.name
    warehouse.mark_activity()

    event = st.meter.record_query(
        result.stats,
        warehouse,
        routing_reason=decision.reason,
    )

    payload_out = result.to_dict(max_rows=payload.max_rows)
    payload_out["routing"] = decision.to_dict()
    payload_out["warehouse"] = warehouse.name
    payload_out["cost"] = {
        "ccu_minutes": round(event.quantity, 6) if event else 0.0,
        "infrastructure_usd": round(event.infra_cost_usd, 8) if event else 0.0,
    }
    return payload_out


@router.post("/query/preview", tags=["query"])
def preview_sql(payload: SqlPreviewRequest, st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Run a model's templated SQL and return sample rows.

    Accepts ``{{ source(...) }}`` and ``{{ ref(...) }}``, compiling them against
    existing tables. This is the "see your transformation work before saving it"
    step in the console.
    """
    from clara.transform.models import Model

    engine = next(iter(st.router.engines.values()))
    probe = Model(
        name="_clara_preview",
        sql=payload.sql,
        namespace=payload.analytics_namespace,
    )
    existing = {m.name: m for m in st.spec.to_models()}
    compiled = probe.compile(models=existing, incremental=False, quote=engine.quote_ref)

    wrapped = f"SELECT * FROM ({compiled}) AS _clara_preview LIMIT {payload.limit}"
    result = engine.execute(wrapped, max_rows=payload.limit)
    return {
        "compiled_sql": compiled,
        "engine": engine.name,
        **result.to_dict(max_rows=payload.limit),
    }


@router.post("/query/recommend-size", tags=["query"])
def recommend_warehouse_size(
    payload: QueryRequest, st: PlatformState = Depends(state)
) -> dict[str, Any]:
    """Recommend a warehouse size for a query — the autosizing endpoint."""
    tables_referenced = st.router.referenced_tables(payload.sql)
    estimated = st.router.estimate_bytes(tables_referenced)
    size = recommend_size(estimated)
    return {
        "recommended_size": size.value,
        "estimated_scan_bytes": estimated,
        "tables": [t.fqn for t in tables_referenced],
    }


# ------------------------------------------------------------------ warehouses


@router.get("/warehouses", tags=["warehouses"])
def warehouses(st: PlatformState = Depends(state)) -> dict[str, Any]:
    credit_price = 1.0
    return {
        "warehouses": [
            {
                **w.to_dict(),
                "cost_per_hour": w.cost_per_hour(st.provider, credit_price),
            }
            for w in st.warehouses.values()
        ]
    }


@router.post("/warehouses/{name}/suspend", tags=["warehouses"])
def suspend_warehouse(name: str, st: PlatformState = Depends(state)) -> dict[str, Any]:
    warehouse = st.warehouse(name)
    uptime = warehouse.uptime_seconds()
    warehouse.mark_suspended()
    st.meter.record_warehouse_uptime(warehouse, seconds=min(uptime, 60.0))
    return warehouse.to_dict()


@router.post("/warehouses/{name}/resume", tags=["warehouses"])
def resume_warehouse(name: str, st: PlatformState = Depends(state)) -> dict[str, Any]:
    warehouse = st.warehouse(name)
    st.touch_warehouse(warehouse)
    return warehouse.to_dict()


# ------------------------------------------------------------------------ spec


@router.get("/spec", tags=["pipeline"])
def get_spec(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """The current platform specification."""
    return {
        "spec": st.spec.model_dump(mode="json"),
        "summary": st.spec.summary(),
        "path": str(st.spec_path) if st.spec_path else None,
    }


@router.put("/spec", tags=["pipeline"])
def put_spec(
    payload: dict[str, Any] = Body(...), st: PlatformState = Depends(state)
) -> dict[str, Any]:
    """Replace the specification and persist it to ``clara.yaml``."""
    spec = parse_spec(payload)
    st.set_spec(spec)
    return {"spec": spec.model_dump(mode="json"), "summary": spec.summary()}


@router.post("/spec/validate", tags=["pipeline"])
def validate_spec(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Validate a draft without saving it."""
    try:
        spec = parse_spec(payload)
    except ValidationError as exc:
        return {"valid": False, "errors": exc.details.get("errors", []), "message": exc.message}
    return {"valid": True, "summary": spec.summary()}


@router.post("/pipeline/build", tags=["pipeline"])
def build_pipeline(draft: PipelineDraft, st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Turn the console wizard's draft into a saved, runnable pipeline.

    This is the endpoint the "Create pipeline" flow posts to. It validates the
    whole thing, writes ``clara.yaml``, and returns the execution plan so the
    UI can show what will run before anything does.
    """
    spec = parse_spec(draft.to_spec_payload())
    st.set_spec(spec)
    return {
        "spec": spec.model_dump(mode="json"),
        "summary": spec.summary(),
        "path": str(st.spec_path) if st.spec_path else None,
        "plan": st.executor().plan(),
    }


@router.get("/pipeline/plan", tags=["pipeline"])
def pipeline_plan(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """What a run would do: tasks, batches and lineage."""
    return st.executor().plan()


# ------------------------------------------------------------------------ runs


@router.post("/runs", tags=["runs"], status_code=202)
def create_run(payload: RunRequest, st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Trigger a pipeline run. Returns immediately; poll the run for progress."""
    if not st.spec.sources and not st.spec.models:
        raise ValidationError("nothing to run: the pipeline has no sources or models")
    run = st.start_run(
        trigger=payload.trigger,
        full_refresh=payload.full_refresh,
        select=payload.select or None,
    )
    return run.to_dict(include_tasks=False)


@router.get("/runs", tags=["runs"])
def list_runs(
    limit: int = Query(default=25, ge=1, le=200), st: PlatformState = Depends(state)
) -> dict[str, Any]:
    return {"runs": [r.to_dict(include_tasks=False) for r in st.runs.list(limit)]}


@router.get("/runs/{run_id}", tags=["runs"])
def get_run(run_id: str, st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Full run detail including per-task status — the console polls this."""
    return st.runs.get(run_id).to_dict()


@router.post("/runs/{run_id}/cancel", tags=["runs"])
def cancel_run(run_id: str, st: PlatformState = Depends(state)) -> dict[str, Any]:
    run = st.runs.get(run_id)
    run.cancel()
    return run.to_dict(include_tasks=False)


# ----------------------------------------------------------------- usage/bill


@router.get("/usage", tags=["billing"])
def usage(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Current-period usage summary."""
    summary = st.usage_store.summarize(st.meter.tenant_id)
    return summary.to_dict()


@router.get("/usage/quotas", tags=["billing"])
def quotas(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Limits and consumption against them."""
    return st.quotas.describe(st.plan, st.meter.tenant_id)


@router.get("/billing/forecast", tags=["billing"])
def forecast(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Month-to-date spend and a straight-line projection."""
    return st.billing.forecast(st.meter.tenant_id, st.plan)


@router.get("/billing/invoice", tags=["billing"])
def invoice(
    plan: str | None = Query(default=None),
    commitment_months: int = Query(default=0, ge=0),
    st: PlatformState = Depends(state),
) -> dict[str, Any]:
    """Generate the current period's invoice."""
    resolved = Plan(plan) if plan else st.plan
    generated = st.billing.generate(
        st.meter.tenant_id, resolved, commitment_term_months=commitment_months
    )
    return {**generated.to_dict(), "text": generated.render_text()}


@router.get("/billing/plans", tags=["billing"])
def plans(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """The rate card: plans, fee bands, limits and discounts."""
    summary = st.usage_store.summarize(st.meter.tenant_id)
    return {
        "rate_card": st.rate_card.describe(),
        "comparison_on_current_usage": st.pricing.compare_plans(summary),
    }


@router.post("/billing/credits", tags=["billing"])
def purchase_credits(
    payload: CreditPurchaseRequest, st: PlatformState = Depends(state)
) -> dict[str, Any]:
    """Buy prepaid credits, applying the volume bonus."""
    grant = st.ledger.purchase(
        st.meter.tenant_id,
        payload.amount_usd,
        term_months=payload.term_months,
        note=payload.note,
    )
    return {"grant": grant.to_dict(), "balance_usd": st.ledger.balance(st.meter.tenant_id)}


@router.post("/billing/estimate", tags=["billing"])
def estimate(payload: EstimateRequest, st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Compare Clara's cost against the incumbents for a given workload."""
    provider = (
        build_provider(st.settings, payload.provider) if payload.provider else st.provider
    )
    return competitor_comparison(
        provider,
        Plan(payload.plan),
        monthly_ccu_hours=payload.monthly_ccu_hours,
        rate_card=st.rate_card,
    )


# ------------------------------------------------------------------ dashboard


@router.get("/dashboard", tags=["platform"])
def dashboard(st: PlatformState = Depends(state)) -> dict[str, Any]:
    """Everything the console's home screen needs, in one round trip."""
    summary = st.usage_store.summarize(st.meter.tenant_id)
    runs = st.runs.list(5)
    table_rows = tables(namespace=None, st=st)["tables"]

    return {
        "health": st.health(),
        "spec": st.spec.summary(),
        "tables": table_rows,
        "table_count": len(table_rows),
        "total_rows": sum(t["row_count"] or 0 for t in table_rows),
        "total_bytes": sum(t["size_bytes"] or 0 for t in table_rows),
        "runs": [r.to_dict(include_tasks=False) for r in runs],
        "usage": {
            "compute_ccu_minutes": round(summary.quantity(Meter.COMPUTE), 2),
            "ingest_gb": round(summary.quantity(Meter.INGEST), 4),
            "storage_gb_month": round(summary.quantity(Meter.STORAGE), 4),
            "infra_cost_usd": round(summary.total_infra_cost_usd, 4),
        },
        "forecast": st.billing.forecast(st.meter.tenant_id, st.plan),
        "warehouses": [w.to_dict() for w in st.warehouses.values()],
        "theme": branding.theme_tokens(),
    }


def _estimated_ccu_minutes(estimate: Any, warehouse: Any) -> float:
    """Convert an engine estimate into CCU-minutes for the quota check.

    Deliberately conservative: with no estimate available it assumes a short
    query rather than zero, so quota checks are not trivially bypassed by an
    engine that cannot plan.
    """
    seconds = max(getattr(estimate, "wall_seconds", 0.0), 1.0)
    return (seconds / 60.0) * warehouse.ccu_per_minute


__all__ = ["router"]
