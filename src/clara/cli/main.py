"""The ``clara`` command line.

Mirrors the console: every command here maps to the same layers the API calls,
so a pipeline can be built in the UI and run from CI, or vice versa.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from clara import branding
from clara.errors import ClaraError
from clara.logging_setup import configure_logging
from clara.settings import get_settings
from clara.version import __version__

app = typer.Typer(
    name="clara",
    help="Open, multi-cloud lakehouse platform.",
    no_args_is_help=True,
    add_completion=False,
)
console = Console()

# Brand colours, as Rich styles.
BLUE = branding.BLUE.hex
GREEN = branding.SUCCESS.hex
YELLOW = branding.ACCENT.hex
RED = branding.DANGER.hex

pipeline_app = typer.Typer(help="Build and inspect pipelines.", no_args_is_help=True)
table_app = typer.Typer(help="Inspect lakehouse tables.", no_args_is_help=True)
cost_app = typer.Typer(help="Usage, pricing and invoices.", no_args_is_help=True)
provider_app = typer.Typer(help="Infrastructure providers.", no_args_is_help=True)
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(table_app, name="table")
app.add_typer(cost_app, name="cost")
app.add_typer(provider_app, name="provider")


# ------------------------------------------------------------------- helpers


def _state() -> Any:
    """Load platform state, reporting configuration errors cleanly."""
    from clara.control_plane.state import get_state

    try:
        return get_state()
    except ClaraError as exc:
        console.print(f"[{RED}]✗ {exc.message}[/]")
        raise typer.Exit(1) from exc


def _fail(exc: ClaraError) -> None:
    console.print(f"[{RED}]✗ {exc.message}[/]")
    if exc.details:
        console.print(f"  [dim]{json.dumps(exc.details, default=str)}[/dim]")
    raise typer.Exit(1)


def _status_style(status: str) -> str:
    return branding.status_color(status).hex


def _table(*columns: str, title: str | None = None) -> Table:
    table = Table(title=title, header_style=f"bold {BLUE}", border_style="dim", expand=False)
    for column in columns:
        table.add_column(column)
    return table


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging."),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Warnings and errors only."),
) -> None:
    """Configure logging before any command runs."""
    level = "DEBUG" if verbose else ("WARNING" if quiet else "INFO")
    configure_logging(level, force=True)


@app.command()
def version() -> None:
    """Print version and configuration."""
    settings = get_settings()
    console.print(
        Panel(
            f"[bold {BLUE}]Clara Data[/] [dim]v{__version__}[/]\n\n"
            f"environment  {settings.env}\n"
            f"provider     {settings.provider}\n"
            f"catalog      {settings.catalog.kind}\n"
            f"plan         {settings.billing.plan}",
            border_style=BLUE,
            expand=False,
        )
    )


# ---------------------------------------------------------------------- init


@app.command()
def init(
    path: Path = typer.Option(Path("clara.yaml"), "--path", help="Where to write the spec."),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing spec."),
) -> None:
    """Create a starter ``clara.yaml`` — a complete working pipeline."""
    from clara.spec import STARTER_SPEC

    if path.exists() and not force:
        console.print(f"[{YELLOW}]! {path} already exists. Use --force to overwrite.[/]")
        raise typer.Exit(1)

    path.write_text(STARTER_SPEC)
    console.print(f"[{GREEN}]✓[/] wrote [bold]{path}[/]")
    console.print(
        f"\nNext:\n"
        f"  [{BLUE}]clara plan[/]    inspect what will run\n"
        f"  [{BLUE}]clara run[/]     ingest, transform and test\n"
        f"  [{BLUE}]clara serve[/]   open the web console\n"
    )


@app.command()
def validate(
    path: Path | None = typer.Argument(None, help="Spec file. Defaults to discovery."),
) -> None:
    """Validate a spec without running it."""
    from clara.spec import load_spec

    try:
        spec = load_spec(path)
    except ClaraError as exc:
        _fail(exc)
        return

    console.print(f"[{GREEN}]✓[/] {spec.project} is valid — "
                  f"{len(spec.sources)} source(s), {len(spec.models)} model(s)")


@app.command()
def plan() -> None:
    """Show the execution plan: tasks, order and lineage."""
    state = _state()
    try:
        plan_data = state.executor().plan()
    except ClaraError as exc:
        _fail(exc)
        return

    console.print(f"\n[bold {BLUE}]{plan_data['project']}[/] — execution plan\n")
    for index, batch in enumerate(plan_data["batches"], start=1):
        console.print(f"  [dim]step {index}[/] " + "  ".join(f"[{BLUE}]{n}[/]" for n in batch))
    console.print(
        f"\n[dim]{plan_data['sources']} source(s), {plan_data['models']} model(s), "
        f"engines: {', '.join(plan_data['engines'])}[/dim]\n"
    )


# ----------------------------------------------------------------------- run


@app.command()
def run(
    select: list[str] = typer.Option(None, "--select", "-s", help="Limit to tasks or models."),
    full_refresh: bool = typer.Option(False, "--full-refresh", help="Rebuild incrementals."),
    skip_ingest: bool = typer.Option(False, "--skip-ingest", help="Transform only."),
    skip_transform: bool = typer.Option(False, "--skip-transform", help="Ingest only."),
    json_output: bool = typer.Option(False, "--json", help="Emit the run as JSON."),
) -> None:
    """Run the pipeline end to end."""
    state = _state()
    executor = state.executor()

    try:
        result = executor.run(
            trigger="cli",
            state=state.last_state(),
            select=list(select) if select else None,
            full_refresh=full_refresh,
            skip_ingest=skip_ingest,
            skip_transform=skip_transform,
        )
    except ClaraError as exc:
        _fail(exc)
        return

    state.runs.record(result)
    if result.status.value == "succeeded":
        state.save_state(result.state)

    if json_output:
        console.print_json(json.dumps(result.to_dict(), default=str))
        raise typer.Exit(0 if result.status.value == "succeeded" else 1)

    table = _table("task", "kind", "status", "duration", "detail")
    for task in result.tasks:
        payload = task.to_dict()
        detail = task.error or _describe_output(task.output)
        table.add_row(
            payload["name"],
            payload["kind"],
            f"[{_status_style(payload['status'])}]{payload['status']}[/]",
            payload["duration"],
            detail or "",
        )
    console.print(table)

    colour = GREEN if result.status.value == "succeeded" else RED
    console.print(
        f"\n[{colour}]{result.status.value}[/] — "
        f"{result.records_synced:,} rows synced, {result.models_built} model(s) built "
        f"in {result.to_dict()['duration']}\n"
    )
    if result.status.value != "succeeded":
        raise typer.Exit(1)


def _describe_output(output: dict[str, Any]) -> str:
    if not output:
        return ""
    if output.get("records") is not None:
        tables = ", ".join(output.get("tables") or [])
        return f"{output['records']:,} rows → {tables}"
    if output.get("rows") is not None:
        return f"{output['rows']:,} rows → {output.get('table', '')}"
    if output.get("optimized"):
        return f"compacted {len(output['optimized'])} table(s)"
    return ""


# --------------------------------------------------------------------- query


@app.command()
def query(
    sql: str = typer.Argument(..., help="SQL to execute."),
    limit: int = typer.Option(50, "--limit", "-n", help="Max rows to display."),
    explain: bool = typer.Option(False, "--explain", help="Show the plan and cost instead."),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Run SQL against the lakehouse, routed automatically."""
    state = _state()
    warehouse = state.warehouse()

    try:
        decision = state.router.route(sql, warehouse=warehouse)
        if explain:
            console.print(Panel(decision.engine.explain(sql), title=f"plan ({decision.engine.name})",
                                border_style=BLUE))
            return

        state.touch_warehouse(warehouse)
        result = decision.engine.execute(sql, max_rows=limit)
        result.stats.engine = decision.engine.name
        event = state.meter.record_query(result.stats, warehouse, routing_reason=decision.reason)
    except ClaraError as exc:
        _fail(exc)
        return

    if json_output:
        console.print_json(json.dumps(result.to_dict(max_rows=limit), default=str))
        return

    if result.is_dml:
        console.print(f"[{GREEN}]✓[/] statement completed in {result.stats.to_dict()['duration']}")
        return

    table = _table(*result.columns)
    for row in result.rows[:limit]:
        table.add_row(*["" if v is None else str(v) for v in row])
    console.print(table)

    cost = f", ${event.infra_cost_usd:.6f} infra" if event else ""
    console.print(
        f"[dim]{len(result.rows):,} rows · {result.stats.to_dict()['duration']} · "
        f"{decision.engine.name} ({decision.reason}){cost}[/dim]"
    )


# --------------------------------------------------------------------- serve


@app.command()
def serve(
    host: str = typer.Option(None, "--host", help="Bind address."),
    port: int = typer.Option(None, "--port", "-p", help="Port."),
    reload: bool = typer.Option(False, "--reload", help="Auto-reload on code changes."),
    open_browser: bool = typer.Option(True, "--open/--no-open", help="Open the console."),
) -> None:
    """Start the API and web console."""
    import uvicorn

    settings = get_settings()
    bind_host = host or settings.api_host
    bind_port = port or settings.api_port
    display_host = "localhost" if bind_host in ("0.0.0.0", "::") else bind_host
    url = f"http://{display_host}:{bind_port}"

    console.print(
        Panel(
            f"[bold {BLUE}]Clara console[/]  {url}\n"
            f"[dim]API docs[/]      {url}/api/docs\n"
            f"[dim]provider[/]      {settings.provider}\n"
            f"[dim]auth[/]          {'disabled (local)' if settings.auth_disabled else 'API key'}",
            border_style=BLUE,
            expand=False,
        )
    )

    if open_browser and not reload:
        import threading
        import webbrowser

        threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    uvicorn.run(
        "clara.control_plane.app:app" if reload else _build_app(),
        host=bind_host,
        port=bind_port,
        reload=reload,
        log_level=settings.log_level.lower(),
        access_log=False,
    )


def _build_app() -> Any:
    from clara.control_plane.app import create_app

    return create_app()


# ------------------------------------------------------------------ pipeline


@pipeline_app.command("show")
def pipeline_show() -> None:
    """Print the current spec as YAML."""
    import yaml

    state = _state()
    payload = state.spec.model_dump(mode="json", exclude_none=True)
    console.print(Syntax(yaml.safe_dump(payload, sort_keys=False), "yaml", theme="ansi_dark"))


@pipeline_app.command("sources")
def pipeline_sources() -> None:
    """List available source connectors."""
    from clara.connectors import source_specs

    table = _table("connector", "title", "incremental", "required config")
    for spec in source_specs():
        required = ", ".join(spec["config_schema"].get("required", [])) or "—"
        table.add_row(
            spec["name"],
            spec["title"],
            f"[{GREEN}]yes[/]" if spec["supports_incremental"] else "[dim]no[/dim]",
            required,
        )
    console.print(table)


@pipeline_app.command("test")
def pipeline_test(
    source: str = typer.Argument(..., help="Connector name."),
    config: str = typer.Option("{}", "--config", "-c", help="JSON configuration."),
) -> None:
    """Test a connector's configuration and list its streams."""
    from clara.connectors import build_source

    try:
        connector = build_source(source, json.loads(config))
        result = connector.check()
    except ClaraError as exc:
        _fail(exc)
        return
    except json.JSONDecodeError as exc:
        console.print(f"[{RED}]✗ --config is not valid JSON: {exc}[/]")
        raise typer.Exit(1) from exc

    if not result.succeeded:
        console.print(f"[{RED}]✗ {result.message}[/]")
        raise typer.Exit(1)

    console.print(f"[{GREEN}]✓[/] {result.message}")
    table = _table("stream", "columns", "incremental", "cursor", "key")
    for descriptor in connector.discover():
        schema = descriptor.to_schema()
        table.add_row(
            descriptor.name,
            str(len(schema)),
            f"[{GREEN}]yes[/]" if descriptor.supports_incremental else "[dim]no[/dim]",
            ", ".join(descriptor.default_cursor_field) or "—",
            ", ".join(k[0] for k in descriptor.source_defined_primary_key) or "—",
        )
    console.print(table)
    connector.close()


# --------------------------------------------------------------------- table


@table_app.command("list")
def table_list(
    namespace: str = typer.Option(None, "--namespace", "-n"),
) -> None:
    """List lakehouse tables."""
    state = _state()
    table = _table("table", "format", "rows", "size", "columns")
    for ref in state.catalog.list_tables(namespace):
        if ref.name.startswith("_clara_stage_"):
            continue
        info = state.catalog.load_table(ref)
        table.add_row(
            ref.fqn,
            info.format,
            f"{info.row_count:,}" if info.row_count is not None else "—",
            _bytes(info.size_bytes),
            str(len(info.schema)),
        )
    console.print(table)


@table_app.command("show")
def table_show(name: str = typer.Argument(..., help="namespace.table")) -> None:
    """Show a table's schema."""
    from clara.catalog.base import TableRef

    state = _state()
    try:
        info = state.catalog.load_table(TableRef.parse(name))
    except ClaraError as exc:
        _fail(exc)
        return

    console.print(f"\n[bold {BLUE}]{info.ref.fqn}[/] [dim]{info.format}[/]")
    console.print(
        f"[dim]{info.row_count if info.row_count is not None else '—'} rows · "
        f"{_bytes(info.size_bytes)} · {info.location or 'no location'}[/dim]\n"
    )
    table = _table("column", "type", "nullable")
    for field in info.schema.fields:
        table.add_row(field.name, field.type.value, "yes" if field.nullable else "no")
    console.print(table)


def _bytes(value: int | None) -> str:
    if not value:
        return "0 B"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


# ---------------------------------------------------------------------- cost


@cost_app.command("usage")
def cost_usage() -> None:
    """Show current-period usage by meter."""
    state = _state()
    summary = state.usage_store.summarize(state.meter.tenant_id)
    payload = summary.to_dict()

    table = _table("meter", "quantity", "infra cost (USD)")
    for meter, quantity in payload["quantities"].items():
        table.add_row(meter, f"{quantity:,.4f}", f"${payload['infra_cost_usd'].get(meter, 0):,.6f}")
    console.print(table)
    console.print(f"\n[dim]total infrastructure cost: "
                  f"${payload['total_infra_cost_usd']:,.4f}[/dim]\n")


@cost_app.command("invoice")
def cost_invoice(
    plan: str = typer.Option(None, "--plan", help="Price against a specific plan."),
    commitment: int = typer.Option(0, "--commitment", help="Commitment term in months."),
) -> None:
    """Generate the current period's invoice."""
    from clara.metering import Plan

    state = _state()
    resolved = Plan(plan) if plan else state.plan
    invoice = state.billing.generate(
        state.meter.tenant_id, resolved, commitment_term_months=commitment
    )
    console.print(invoice.render_text())


@cost_app.command("forecast")
def cost_forecast() -> None:
    """Project this month's spend from usage so far."""
    state = _state()
    data = state.billing.forecast(state.meter.tenant_id, state.plan)
    console.print(
        Panel(
            f"month to date     [bold]${data['to_date_usd']:,.2f}[/]\n"
            f"projected total   [bold {YELLOW}]${data['projected_usd']:,.2f}[/]\n"
            f"infrastructure    ${data['infrastructure_to_date_usd']:,.2f}\n"
            f"platform fee      ${data['platform_fee_to_date_usd']:,.2f}\n\n"
            f"[dim]{data['projection_method']}[/dim]",
            title="forecast",
            border_style=BLUE,
            expand=False,
        )
    )


@cost_app.command("compare")
def cost_compare(
    ccu_hours: float = typer.Option(1000.0, "--ccu-hours", help="Monthly CCU-hours."),
    plan: str = typer.Option("team", "--plan"),
    provider: str = typer.Option(None, "--provider"),
) -> None:
    """Compare Clara's cost against Snowflake, Databricks and BigQuery."""
    from clara.metering import Plan, competitor_comparison
    from clara.providers import build_provider

    state = _state()
    target = build_provider(state.settings, provider) if provider else state.provider
    data = competitor_comparison(
        target, Plan(plan), monthly_ccu_hours=ccu_hours, rate_card=state.rate_card
    )

    console.print(
        f"\n[bold {BLUE}]Clara on {data['provider']}[/] "
        f"({plan} plan, {ccu_hours:,.0f} CCU-hours/month)\n"
        f"  infrastructure  ${data['clara']['monthly_infrastructure_usd']:,.2f}\n"
        f"  platform fee    ${data['clara']['monthly_platform_fee_usd']:,.2f}\n"
        f"  [bold]total           ${data['clara']['monthly_total_usd']:,.2f}[/]  "
        f"(${data['clara']['all_in_usd_per_ccu_hour']:.4f}/CCU-hour)\n"
    )
    table = _table("platform", "$/CCU-hour", "$/month", "Clara cheaper by")
    for row in data["competitors"]:
        table.add_row(
            row["platform"],
            f"${row['usd_per_ccu_hour']:.3f}",
            f"${row['monthly_usd']:,.0f}",
            f"[{GREEN}]{row['clara_is_cheaper_by']}×[/]" if row["clara_is_cheaper_by"] else "—",
        )
    console.print(table)
    console.print(f"\n[dim]{data['assumptions']}[/dim]\n")


@cost_app.command("quotas")
def cost_quotas() -> None:
    """Show plan limits and consumption."""
    state = _state()
    data = state.quotas.describe(state.plan, state.meter.tenant_id)

    console.print(f"\n[bold {BLUE}]{data['plan']}[/] on {data['provider']}")
    if data["derived_from_provider_free_tier"]:
        console.print(f"[dim]limits derived from the provider's free tier: "
                      f"{data['provider_free_tier_notes']}[/dim]")

    table = _table("limit", "used", "of", "unit")
    for check in data["checks"]:
        limit = "unlimited" if check["limit"] is None else f"{check['limit']:,.1f}"
        style = RED if not check["allowed"] else (
            YELLOW if check["utilization"] > 0.8 else GREEN
        )
        table.add_row(
            check["limit_name"],
            f"[{style}]{check['used']:,.1f}[/]",
            limit,
            check["unit"],
        )
    console.print(table)


# ------------------------------------------------------------------ provider


@provider_app.command("list")
def provider_list() -> None:
    """Compare infrastructure providers by cost."""
    from clara.providers import compare_providers

    state = _state()
    table = _table("provider", "medium wh/hour", "$/GB-month", "$/GB egress", "free CCU-min/month")
    for row in compare_providers(state.settings):
        free = row["free_tier"]["ccu_minutes_month"]
        table.add_row(
            row["display_name"],
            f"${row['medium_warehouse_hour']:,.4f}",
            f"${row['rates']['storage_gb_month']:.4f}",
            f"${row['rates']['egress_gb']:.3f}",
            "unlimited" if free == float("inf") else f"{free:,.0f}",
        )
    console.print(table)


@provider_app.command("show")
def provider_show() -> None:
    """Describe the active provider."""
    state = _state()
    console.print_json(json.dumps(state.provider.describe(), default=str))


def cli() -> None:
    """Entry point used by the console script."""
    try:
        app()
    except ClaraError as exc:  # pragma: no cover - safety net
        console.print(f"[{RED}]✗ {exc.message}[/]")
        sys.exit(1)


if __name__ == "__main__":
    cli()
