"""API request and response models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ConnectorConfigRequest(BaseModel):
    """Configuration for a connector operation."""

    config: dict[str, Any] = Field(default_factory=dict)


class PreviewRequest(ConnectorConfigRequest):
    """Preview rows from one stream before committing to a pipeline."""

    stream: str
    limit: int = Field(default=20, ge=1, le=500)


class QueryRequest(BaseModel):
    """An ad-hoc SQL query."""

    sql: str
    warehouse: str | None = None
    max_rows: int = Field(default=1_000, ge=1, le=50_000)
    #: Return the plan and cost estimate without executing.
    explain_only: bool = False


class StreamSelectionPayload(BaseModel):
    """One stream chosen in the console wizard."""

    name: str
    sync_mode: Literal["full_refresh", "incremental"] = "full_refresh"
    write_mode: Literal["append", "overwrite", "merge"] | None = None
    cursor_field: str | None = None
    primary_key: str | None = None
    table: str | None = None


class SourceDraft(BaseModel):
    """A source as configured in the wizard."""

    name: str
    connector: str
    config: dict[str, Any] = Field(default_factory=dict)
    namespace: str | None = None
    streams: list[StreamSelectionPayload] = Field(default_factory=list)


class ModelDraft(BaseModel):
    """A transformation as authored in the wizard."""

    name: str
    sql: str
    materialization: Literal["table", "view", "incremental", "ephemeral"] = "table"
    description: str = ""
    unique_key: str | None = None
    tests: list[dict[str, Any]] = Field(default_factory=list)


class PipelineDraft(BaseModel):
    """The full wizard payload: sources, models and a schedule.

    The console posts this; the server converts it into a ``PlatformSpec``,
    validates it, saves ``clara.yaml`` and can run it. One round trip from
    "built in the UI" to "committed, runnable pipeline".
    """

    project: str = "my_pipeline"
    description: str = ""
    sources: list[SourceDraft] = Field(default_factory=list)
    models: list[ModelDraft] = Field(default_factory=list)
    schedule: str | None = None
    warehouse_size: str = "xs"
    raw_namespace: str = "raw"
    analytics_namespace: str = "analytics"

    def to_spec_payload(self) -> dict[str, Any]:
        """Render as the mapping ``parse_spec`` validates."""
        return {
            "version": 1,
            "project": self.project,
            "description": self.description,
            "defaults": {
                "raw_namespace": self.raw_namespace,
                "analytics_namespace": self.analytics_namespace,
                "warehouse": "default",
            },
            "warehouses": [{"name": "default", "size": self.warehouse_size}],
            "sources": [
                {
                    "name": s.name,
                    "connector": s.connector,
                    "config": s.config,
                    **({"namespace": s.namespace} if s.namespace else {}),
                    "streams": [
                        {k: v for k, v in stream.model_dump().items() if v is not None}
                        for stream in s.streams
                    ],
                }
                for s in self.sources
            ],
            "models": [
                {
                    "name": m.name,
                    "sql": m.sql,
                    "materialization": m.materialization,
                    "description": m.description,
                    **({"unique_key": m.unique_key} if m.unique_key else {}),
                    "tests": m.tests,
                }
                for m in self.models
            ],
            **({"schedule": self.schedule} if self.schedule else {}),
        }


class RunRequest(BaseModel):
    """Trigger a pipeline run."""

    full_refresh: bool = False
    #: Restrict to specific tasks or models. Empty means everything.
    select: list[str] = Field(default_factory=list)
    trigger: str = "api"


class SqlPreviewRequest(BaseModel):
    """Run a model's SQL against the lakehouse without materialising it.

    This is what makes the transform step of the wizard usable: a user sees the
    first rows their SQL produces before the table exists.
    """

    sql: str
    limit: int = Field(default=20, ge=1, le=200)
    raw_namespace: str = "raw"
    analytics_namespace: str = "analytics"


class CreditPurchaseRequest(BaseModel):
    amount_usd: float = Field(gt=0)
    term_months: int = Field(default=0, ge=0, le=60)
    note: str = ""


class EstimateRequest(BaseModel):
    """Cost estimate for a hypothetical workload."""

    monthly_ccu_hours: float = Field(default=1_000.0, gt=0)
    plan: str = "team"
    provider: str | None = None
