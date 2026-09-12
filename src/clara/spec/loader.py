"""Loading and saving ``clara.yaml``.

Handles environment interpolation and turns Pydantic's validation errors into
messages that point at the offending line in the user's file.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError as PydanticValidationError

from clara.errors import NotFoundError, ValidationError
from clara.logging_setup import get_logger
from clara.spec.models import PlatformSpec

log = get_logger(__name__)

#: Default filenames searched by ``clara`` commands, in order.
SPEC_FILENAMES = ("clara.yaml", "clara.yml", ".clara/clara.yaml")

#: ``${VAR}`` or ``${VAR:-fallback}``. Secrets live in the environment, never
#: in the spec file, so a spec is safe to commit.
_ENV_RE = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")


def interpolate(value: Any, *, strict: bool = False) -> Any:
    """Recursively substitute ``${VAR}`` references from the environment.

    With ``strict=False`` an unset variable without a default is left as-is, so
    ``clara validate`` can check a spec's structure on a machine that has none
    of the production credentials.
    """
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name = match.group("name")
            default = match.group("default")
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            if strict:
                raise ValidationError(
                    f"environment variable ${{{name}}} is not set", variable=name
                )
            return match.group(0)

        return _ENV_RE.sub(replace, value)

    if isinstance(value, dict):
        return {k: interpolate(v, strict=strict) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate(v, strict=strict) for v in value]
    return value


def find_spec_file(start: Path | str = ".") -> Path | None:
    """Search upward from ``start`` for a spec file, like git finds ``.git``."""
    current = Path(start).expanduser().resolve()
    if current.is_file():
        return current

    for directory in (current, *current.parents):
        for name in SPEC_FILENAMES:
            candidate = directory / name
            if candidate.is_file():
                return candidate
    return None


def load_spec(
    path: Path | str | None = None, *, strict_env: bool = False
) -> PlatformSpec:
    """Load and validate a spec file."""
    resolved = Path(path) if path else find_spec_file()
    if resolved is None:
        raise NotFoundError(
            "no clara.yaml found. Run 'clara init' to create one.",
            searched=list(SPEC_FILENAMES),
        )
    if not resolved.is_file():
        raise NotFoundError(f"spec file not found: {resolved}")

    try:
        raw = yaml.safe_load(resolved.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ValidationError(f"{resolved}: invalid YAML — {exc}") from exc

    if not isinstance(raw, dict):
        raise ValidationError(f"{resolved}: top level must be a mapping")

    spec = parse_spec(interpolate(raw, strict=strict_env), origin=str(resolved))
    log.debug(
        "loaded spec",
        extra={"path": str(resolved), "sources": len(spec.sources), "models": len(spec.models)},
    )
    return spec


def parse_spec(payload: dict[str, Any], *, origin: str = "spec") -> PlatformSpec:
    """Validate a parsed mapping, reporting errors by field path."""
    try:
        return PlatformSpec.model_validate(payload)
    except PydanticValidationError as exc:
        raise ValidationError(
            f"{origin} is invalid:\n{_format_errors(exc)}",
            errors=_error_list(exc),
        ) from exc


def save_spec(spec: PlatformSpec, path: Path | str = "clara.yaml") -> Path:
    """Write a spec back to YAML.

    Used when the console saves a pipeline. Round-tripping through the same
    model the loader validates guarantees the console cannot produce a file the
    CLI would reject.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = spec.model_dump(mode="json", exclude_defaults=False, exclude_none=True)
    header = (
        "# Clara platform specification\n"
        "# Describes sources, tables, transformations and schedule.\n"
        "# Edit here or in the Clara console — both read and write this file.\n"
        "# Secrets belong in environment variables, referenced as ${VAR}.\n"
    )
    target.write_text(header + yaml.safe_dump(payload, sort_keys=False, width=100))
    return target


def _error_list(exc: PydanticValidationError) -> list[dict[str, Any]]:
    return [
        {"field": ".".join(str(p) for p in error["loc"]), "message": error["msg"]}
        for error in exc.errors()
    ]


def _format_errors(exc: PydanticValidationError) -> str:
    lines = []
    for error in _error_list(exc):
        location = error["field"] or "(root)"
        lines.append(f"  - {location}: {error['message']}")
    return "\n".join(lines)


#: The spec written by ``clara init`` — a complete, working pipeline that runs
#: with no credentials, so a new user sees data land within a minute.
STARTER_SPEC = """\
version: 1
project: retail_demo
description: End-to-end demo — sample source, raw tables, analytics models.

defaults:
  raw_namespace: raw
  analytics_namespace: analytics
  warehouse: default

warehouses:
  - name: default
    size: xs
    engine: auto
    # Suspend after a minute idle. Idle compute is the biggest avoidable cost.
    auto_suspend_seconds: 60

sources:
  - name: retail
    connector: sample
    config:
      orders: 2000
      days: 60
    streams:
      - name: customers
      - name: orders
        sync_mode: incremental
        cursor_field: ordered_at
        primary_key: order_id

models:
  - name: order_facts
    description: Cleaned order grain with customer attributes joined on.
    materialization: table
    sql: |
      SELECT
        o.order_id,
        o.customer_id,
        c.name        AS customer_name,
        c.country,
        o.sku,
        o.product,
        o.category,
        o.quantity,
        o.unit_price,
        o.amount,
        CAST(o.ordered_at AS DATE) AS order_date
      FROM {{ source('raw', 'orders') }} o
      LEFT JOIN {{ source('raw', 'customers') }} c
        ON o.customer_id = c.customer_id
    tests:
      - type: not_null
        column: order_id
      - type: unique
        column: order_id

  - name: daily_revenue
    description: Revenue by day and country — the table a dashboard reads.
    materialization: table
    sql: |
      SELECT
        order_date,
        country,
        count(*)              AS orders,
        sum(quantity)         AS units,
        round(sum(amount), 2) AS revenue
      FROM {{ ref('order_facts') }}
      GROUP BY order_date, country
    tests:
      - type: not_null
        column: order_date

  - name: category_mix
    description: Revenue share by product category.
    materialization: view
    sql: |
      SELECT
        category,
        round(sum(amount), 2) AS revenue,
        round(100.0 * sum(amount) / sum(sum(amount)) OVER (), 1) AS pct_of_revenue
      FROM {{ ref('order_facts') }}
      GROUP BY category

maintenance:
  enabled: true
  optimize: true
  expire_snapshots: true
  retain_snapshots: 5

# Run every night at 02:00 UTC. Remove to keep the pipeline manual-only.
schedule: "0 2 * * *"
"""
