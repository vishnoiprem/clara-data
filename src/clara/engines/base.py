"""The query engine contract.

Clara runs two engines behind one interface: DuckDB for single-node work and
Trino for scale-out. Everything above this layer — the API, the transform
runner, the CLI — is engine-agnostic, which is what lets the router silently
send a small query to DuckDB (cheap, milliseconds) and a large one to Trino.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from clara.catalog.base import TableRef
from clara.time_utils import format_duration


@dataclass
class QueryStats:
    """Execution facts. These are the inputs to metering, so they are recorded
    for every statement, successful or not."""

    engine: str = "unknown"
    wall_seconds: float = 0.0
    #: Aggregate CPU time across workers. Drives the CCU charge on Trino.
    cpu_seconds: float = 0.0
    bytes_scanned: int = 0
    rows_produced: int = 0
    rows_scanned: int = 0
    #: Bytes shuffled between workers — the cost signal Clara surfaces when
    #: recommending a partition change.
    bytes_shuffled: int = 0
    peak_memory_bytes: int = 0
    #: Warehouse-seconds consumed; the metered quantity for a warehouse query.
    warehouse_seconds: float = 0.0
    queued_seconds: float = 0.0
    #: True when served from the result cache — charged nothing.
    cache_hit: bool = False
    engine_query_id: str | None = None

    def ccu_minutes(self, ccu_per_minute: float) -> float:
        """CCU-minutes to bill for this statement."""
        if self.cache_hit:
            return 0.0
        seconds = self.warehouse_seconds or self.wall_seconds
        return (seconds / 60.0) * ccu_per_minute

    def to_dict(self) -> dict[str, Any]:
        return {
            "engine": self.engine,
            "wall_seconds": round(self.wall_seconds, 4),
            "cpu_seconds": round(self.cpu_seconds, 4),
            "bytes_scanned": self.bytes_scanned,
            "rows_produced": self.rows_produced,
            "rows_scanned": self.rows_scanned,
            "bytes_shuffled": self.bytes_shuffled,
            "peak_memory_bytes": self.peak_memory_bytes,
            "queued_seconds": round(self.queued_seconds, 4),
            "cache_hit": self.cache_hit,
            "engine_query_id": self.engine_query_id,
            "duration": format_duration(self.wall_seconds),
        }


@dataclass
class QueryResult:
    """A result set plus its execution stats."""

    columns: list[str] = field(default_factory=list)
    rows: list[tuple[Any, ...]] = field(default_factory=list)
    stats: QueryStats = field(default_factory=QueryStats)
    #: True when the statement produced no result set (DDL, INSERT).
    is_dml: bool = False
    affected_rows: int | None = None

    def dicts(self) -> list[dict[str, Any]]:
        """Rows as dictionaries — the API response shape."""
        return [dict(zip(self.columns, row, strict=False)) for row in self.rows]

    def scalar(self) -> Any:
        """First column of the first row, or None. For ``SELECT count(*)``."""
        return self.rows[0][0] if self.rows and self.rows[0] else None

    def column(self, name: str) -> list[Any]:
        index = self.columns.index(name)
        return [row[index] for row in self.rows]

    def to_dict(self, max_rows: int | None = None) -> dict[str, Any]:
        rows = self.rows[:max_rows] if max_rows is not None else self.rows
        return {
            "columns": self.columns,
            "rows": [list(r) for r in rows],
            "row_count": len(self.rows),
            "truncated": max_rows is not None and len(self.rows) > max_rows,
            "is_dml": self.is_dml,
            "affected_rows": self.affected_rows,
            "stats": self.stats.to_dict(),
        }

    def __len__(self) -> int:
        return len(self.rows)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.dicts())


@dataclass(frozen=True)
class EngineCapabilities:
    """What an engine can do. The router and transform layer branch on these
    rather than on engine names."""

    #: Scales across nodes.
    distributed: bool = False
    #: Supports MERGE / UPDATE / DELETE on Iceberg tables.
    supports_merge: bool = False
    #: Can read Iceberg tables directly.
    supports_iceberg: bool = True
    #: Can join across catalogs/sources in one query (federation).
    supports_federation: bool = False
    #: Caches identical result sets.
    supports_result_cache: bool = False
    #: Practical ceiling on data scanned per query, in bytes.
    max_scan_bytes: int = 2**62
    #: Typical fixed startup cost, used by the router's cost model.
    startup_seconds: float = 0.0


class Engine(ABC):
    """A SQL execution engine."""

    #: Stable identifier used in config, stats and routing decisions.
    name: str = "abstract"
    capabilities: EngineCapabilities = EngineCapabilities()

    # ------------------------------------------------------------- execution

    @abstractmethod
    def execute(
        self,
        sql: str,
        *,
        parameters: dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_seconds: float | None = None,
    ) -> QueryResult:
        """Run one statement and return its result."""

    def execute_many(self, statements: list[str]) -> list[QueryResult]:
        """Run statements in order, stopping at the first failure."""
        return [self.execute(statement) for statement in statements]

    @abstractmethod
    def explain(self, sql: str) -> str:
        """Return the engine's plan as text."""

    def estimate(self, sql: str) -> QueryStats:
        """Pre-execution estimate, used for cost preview.

        Default implementation returns an empty estimate; engines that can plan
        without executing override it. Cost preview is what stops a customer
        accidentally running a $400 query.
        """
        return QueryStats(engine=self.name)

    # ------------------------------------------------------------ table ops

    def ensure_namespace(self, namespace: str) -> None:
        """Create a schema if it does not exist.

        Called before materialising into a namespace. A model targeting a brand
        new namespace is the common case on a first run, and failing there
        would make the platform unusable out of the box.
        """
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {self.quote(namespace)}")

    def materialize(
        self,
        ref: TableRef,
        sql: str,
        *,
        mode: str = "table",
        unique_key: list[str] | None = None,
        incremental: bool = False,
    ) -> QueryResult:
        """Persist a query's result as a table or view.

        Engines override this when their write path differs. Trino writing to
        Iceberg registers the table in the catalog as a side effect of the DDL;
        DuckDB over a local catalog has to write through the catalog explicitly.
        Keeping the difference behind one method means the transform layer does
        not branch on engine type.
        """
        self.ensure_namespace(ref.namespace)
        if mode == "view":
            return self.create_view(ref, sql)
        if incremental:
            if unique_key and hasattr(self, "merge"):
                columns = self.execute(
                    f"SELECT * FROM ({sql}) AS _clara_probe WHERE 1 = 0"
                ).columns
                return self.merge(ref, sql, unique_key, columns)  # type: ignore[attr-defined]
            return self.execute(f"INSERT INTO {self.quote_ref(ref)} {sql}")
        return self.create_table_as(ref, sql)

    def create_table_as(self, ref: TableRef, sql: str, *, replace: bool = True) -> QueryResult:
        """Materialise a query as a table — the workhorse of the transform layer."""
        verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
        return self.execute(f"{verb} {self.quote_ref(ref)} AS {sql}")

    def create_view(self, ref: TableRef, sql: str, *, replace: bool = True) -> QueryResult:
        verb = "CREATE OR REPLACE VIEW" if replace else "CREATE VIEW"
        return self.execute(f"{verb} {self.quote_ref(ref)} AS {sql}")

    def drop_table(self, ref: TableRef, *, if_exists: bool = True) -> QueryResult:
        guard = "IF EXISTS " if if_exists else ""
        return self.execute(f"DROP TABLE {guard}{self.quote_ref(ref)}")

    def count(self, ref: TableRef) -> int:
        return int(self.execute(f"SELECT count(*) FROM {self.quote_ref(ref)}").scalar() or 0)

    def preview(self, ref: TableRef, limit: int = 100) -> QueryResult:
        return self.execute(f"SELECT * FROM {self.quote_ref(ref)} LIMIT {int(limit)}")

    # -------------------------------------------------------------- identity

    def quote(self, identifier: str) -> str:
        """Quote one identifier for this dialect."""
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def quote_ref(self, ref: TableRef) -> str:
        """Fully-qualified, quoted table reference for this engine."""
        return f"{self.quote(ref.namespace)}.{self.quote(ref.name)}"

    # ------------------------------------------------------------- lifecycle

    @abstractmethod
    def test_connection(self) -> bool:
        """True if the engine is reachable and usable."""

    def close(self) -> None:
        """Release connections. Safe to call repeatedly."""

    def __enter__(self) -> Engine:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"<{type(self).__name__} name={self.name}>"
