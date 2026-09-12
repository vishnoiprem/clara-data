"""The catalog contract.

A catalog answers "what tables exist, what shape are they, and where does their
data live". Clara ships two implementations — ``IcebergCatalog`` for real
deployments and ``LocalCatalog`` for dev/test — behind this one interface, so
nothing above this layer knows which is in use.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from clara.catalog.schema import Schema
from clara.errors import ValidationError


@dataclass(frozen=True)
class TableRef:
    """A fully-qualified table name."""

    namespace: str
    name: str

    def __post_init__(self) -> None:
        if not self.namespace or not self.name:
            raise ValidationError("table reference requires both namespace and name")

    @property
    def fqn(self) -> str:
        return f"{self.namespace}.{self.name}"

    @classmethod
    def parse(cls, value: str | TableRef, default_namespace: str = "main") -> TableRef:
        """Parse ``"ns.table"``, or ``"table"`` against a default namespace.

        A three-part ``catalog.ns.table`` is accepted and the catalog component
        dropped — the catalog is implied by the client you are talking to.
        """
        if isinstance(value, TableRef):
            return value
        parts = [p.strip('"` ') for p in str(value).split(".") if p.strip('"` ')]
        if len(parts) == 1:
            return cls(default_namespace, parts[0])
        if len(parts) == 2:
            return cls(parts[0], parts[1])
        if len(parts) == 3:
            return cls(parts[1], parts[2])
        raise ValidationError(f"cannot parse table reference: {value!r}")

    def __str__(self) -> str:
        return self.fqn


@dataclass
class TableInfo:
    """Catalog metadata for one table."""

    ref: TableRef
    schema: Schema
    location: str | None = None
    format: str = "iceberg"
    properties: dict[str, str] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    #: Populated on demand — statistics can be expensive to compute.
    row_count: int | None = None
    size_bytes: int | None = None
    snapshot_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        from clara.time_utils import to_iso

        return {
            "namespace": self.ref.namespace,
            "name": self.ref.name,
            "fqn": self.ref.fqn,
            "format": self.format,
            "location": self.location,
            "schema": self.schema.to_dict(),
            "properties": self.properties,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "snapshot_id": self.snapshot_id,
            "created_at": to_iso(self.created_at) if self.created_at else None,
            "updated_at": to_iso(self.updated_at) if self.updated_at else None,
        }


class Catalog(ABC):
    """Abstract table catalog."""

    #: Identifier used in logs and API responses.
    kind: str = "abstract"

    # ------------------------------------------------------------- namespaces

    @abstractmethod
    def list_namespaces(self) -> list[str]: ...

    @abstractmethod
    def create_namespace(self, namespace: str, exists_ok: bool = True) -> None: ...

    @abstractmethod
    def drop_namespace(self, namespace: str, cascade: bool = False) -> None: ...

    def namespace_exists(self, namespace: str) -> bool:
        return namespace in self.list_namespaces()

    # ----------------------------------------------------------------- tables

    @abstractmethod
    def list_tables(self, namespace: str | None = None) -> list[TableRef]: ...

    @abstractmethod
    def create_table(
        self,
        ref: TableRef,
        schema: Schema,
        *,
        location: str | None = None,
        properties: dict[str, str] | None = None,
        exists_ok: bool = False,
    ) -> TableInfo: ...

    @abstractmethod
    def load_table(self, ref: TableRef) -> TableInfo: ...

    @abstractmethod
    def drop_table(self, ref: TableRef, purge: bool = False) -> None: ...

    @abstractmethod
    def table_exists(self, ref: TableRef) -> bool: ...

    # ------------------------------------------------------------- evolution

    @abstractmethod
    def evolve_schema(self, ref: TableRef, schema: Schema) -> TableInfo:
        """Additively reconcile a table's schema towards ``schema``.

        Adding columns is safe and automatic; narrowing or dropping is not
        attempted — connectors change shape all the time and silently dropping
        a column loses data.
        """

    # ------------------------------------------------------------------ data

    @abstractmethod
    def append(self, ref: TableRef, records: list[dict[str, Any]]) -> int:
        """Append records, returning the number written."""

    @abstractmethod
    def scan(self, ref: TableRef, limit: int | None = None) -> list[dict[str, Any]]:
        """Read records back. Intended for previews and tests, not analytics —
        analytics goes through the query engines."""

    # --------------------------------------------------------------- helpers

    def ensure_table(
        self,
        ref: TableRef,
        schema: Schema,
        *,
        location: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> TableInfo:
        """Create-or-evolve. The ingest path calls this for every sync."""
        self.create_namespace(ref.namespace, exists_ok=True)
        if self.table_exists(ref):
            return self.evolve_schema(ref, schema)
        return self.create_table(ref, schema, location=location, properties=properties)

    def close(self) -> None:
        """Release resources. Safe to call more than once."""

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"<{type(self).__name__} kind={self.kind}>"