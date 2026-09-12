"""Runtime configuration.

One settings object, populated from environment variables (``CLARA_*``) with
sane local-dev defaults. Nested sections keep provider/engine/billing concerns
separable, and ``get_settings()`` caches a single instance per process.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ProviderName = Literal["local", "aws", "azure", "gcp", "tencent", "alibaba", "generic"]
CatalogKind = Literal["memory", "sqlite", "rest", "glue"]
EngineName = Literal["auto", "duckdb", "trino"]


class StorageSettings(BaseModel):
    """Object-storage target.

    Deliberately modelled as S3-compatible: AWS S3, MinIO, Tencent COS, Alibaba
    OSS, Backblaze B2 and Cloudflare R2 all speak this protocol, which is what
    makes Clara portable across cheap providers.
    """

    bucket: str = "clara-lakehouse"
    prefix: str = "warehouse"
    endpoint: str | None = None
    region: str = "us-east-1"
    access_key: str | None = None
    secret_key: str | None = None
    path_style: bool = True
    #: Used when provider == "local": a filesystem directory instead of a bucket.
    local_root: Path = Path(".clara/storage")

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix.strip('/')}"


class CatalogSettings(BaseModel):
    """Iceberg catalog location."""

    kind: CatalogKind = "sqlite"
    uri: str = "sqlite:///./.clara/iceberg_catalog.db"
    warehouse: str | None = None
    name: str = "clara"
    #: Bearer token for a REST catalog (Lakekeeper, Polaris, Nessie, Unity OSS).
    token: str | None = None


class TrinoSettings(BaseModel):
    """Scale-out engine coordinates."""

    host: str = "localhost"
    port: int = 8080
    user: str = "clara"
    password: str | None = None
    catalog: str = "iceberg"
    schema_: str = Field(default="main", alias="schema")
    http_scheme: Literal["http", "https"] = "http"

    model_config = {"populate_by_name": True}


class DuckDBSettings(BaseModel):
    """Single-node engine coordinates."""

    #: ``:memory:`` is valid and is what the test suite uses.
    path: str = ".clara/duckdb/local.duckdb"
    memory_limit: str = "4GB"
    threads: int = 4


class BillingSettings(BaseModel):
    """Metering and pricing behaviour."""

    rate_card: str = "default"
    plan: str = "trial"
    currency: str = "USD"
    #: Hard monthly spend cap; operations that would breach it are refused.
    budget_cap: float | None = None
    #: Emit usage events even when nothing is being charged (useful in dev).
    meter_when_free: bool = True


class Settings(BaseSettings):
    """Top-level Clara configuration."""

    model_config = SettingsConfigDict(
        env_prefix="CLARA_",
        env_file=("clara.env", ".env.local"),
        env_nested_delimiter="__",
        extra="ignore",
        case_sensitive=False,
    )

    env: Literal["local", "dev", "staging", "prod"] = "local"
    log_level: str = "INFO"
    log_json: bool = False

    # control plane
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    database_url: str = "sqlite:///./.clara/control_plane.db"
    bootstrap_api_key: str | None = None
    #: When true the API accepts unauthenticated calls. Local convenience only;
    #: refused at startup for non-local environments.
    auth_disabled: bool = False

    # platform
    provider: ProviderName = "local"
    default_engine: EngineName = "auto"
    state_dir: Path = Path(".clara")

    storage: StorageSettings = Field(default_factory=StorageSettings)
    catalog: CatalogSettings = Field(default_factory=CatalogSettings)
    trino: TrinoSettings = Field(default_factory=TrinoSettings)
    duckdb: DuckDBSettings = Field(default_factory=DuckDBSettings)
    billing: BillingSettings = Field(default_factory=BillingSettings)

    @field_validator("log_level")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()

    def model_post_init(self, _context: object) -> None:
        # Flat CLARA_STORAGE_* / CLARA_TRINO_* style variables are friendlier in
        # docker-compose than pydantic's nested CLARA_STORAGE__BUCKET form, so
        # support both.
        self._apply_flat_env()
        if self.auth_disabled and self.env != "local":
            raise ValueError("CLARA_AUTH_DISABLED is only permitted when CLARA_ENV=local")
        if self.catalog.warehouse is None:
            self.catalog.warehouse = self.storage.uri

    def _apply_flat_env(self) -> None:
        # Pydantic models are not hashable, so the mapping is keyed by env var
        # name and the (model, field) target is the value.
        mapping: dict[str, tuple[BaseModel, str]] = {
            "CLARA_STORAGE_BUCKET": (self.storage, "bucket"),
            "CLARA_STORAGE_PREFIX": (self.storage, "prefix"),
            "CLARA_STORAGE_ENDPOINT": (self.storage, "endpoint"),
            "CLARA_STORAGE_REGION": (self.storage, "region"),
            "CLARA_STORAGE_ACCESS_KEY": (self.storage, "access_key"),
            "CLARA_STORAGE_SECRET_KEY": (self.storage, "secret_key"),
            "CLARA_STORAGE_PATH_STYLE": (self.storage, "path_style"),
            "CLARA_CATALOG_KIND": (self.catalog, "kind"),
            "CLARA_CATALOG_URI": (self.catalog, "uri"),
            "CLARA_CATALOG_WAREHOUSE": (self.catalog, "warehouse"),
            "CLARA_CATALOG_TOKEN": (self.catalog, "token"),
            "CLARA_TRINO_HOST": (self.trino, "host"),
            "CLARA_TRINO_PORT": (self.trino, "port"),
            "CLARA_TRINO_USER": (self.trino, "user"),
            "CLARA_TRINO_PASSWORD": (self.trino, "password"),
            "CLARA_TRINO_CATALOG": (self.trino, "catalog"),
            "CLARA_TRINO_SCHEMA": (self.trino, "schema_"),
            "CLARA_DUCKDB_PATH": (self.duckdb, "path"),
            "CLARA_RATE_CARD": (self.billing, "rate_card"),
            "CLARA_PLAN": (self.billing, "plan"),
            "CLARA_CURRENCY": (self.billing, "currency"),
            "CLARA_BUDGET_CAP": (self.billing, "budget_cap"),
        }
        for env_name, (model, field) in mapping.items():
            raw = os.environ.get(env_name)
            if raw in (None, ""):
                continue
            annotation = type(model).model_fields[field].annotation
            setattr(model, field, _coerce(raw, annotation))

    # ------------------------------------------------------------------ helpers

    def path_in_state(self, *parts: str) -> Path:
        """Resolve a path under the state directory, creating parent dirs."""
        target = self.state_dir.joinpath(*parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    @property
    def is_local(self) -> bool:
        return self.env == "local"


def _coerce(raw: str, annotation: object) -> object:
    """Best-effort scalar coercion for flat environment overrides."""
    text = str(annotation)
    if "bool" in text:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    if "int" in text:
        return int(raw)
    if "float" in text:
        return float(raw)
    return raw


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton."""
    return Settings()


def reset_settings() -> None:
    """Clear the cache. Tests use this after mutating the environment."""
    get_settings.cache_clear()