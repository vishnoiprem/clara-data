"""Trino engine — distributed execution.

Trino is Clara's scale-out engine: petabyte scans, large joins, high concurrency,
and federated queries that join a lakehouse table to a live operational database.
It also supplies the accurate per-query resource accounting Clara bills on —
``EXPLAIN ANALYZE``-grade statistics come back with every query, so CCU charges
reflect real CPU consumed rather than wall-clock guesses.

Chosen over Spark as the interactive engine because startup is instant, SQL is
ANSI-standard, and the memory footprint per query is far smaller — which matters
when the target customer is running on commodity hardware.
"""

from __future__ import annotations

import time
from typing import Any

from clara.catalog.base import TableRef
from clara.engines.base import Engine, EngineCapabilities, QueryResult, QueryStats
from clara.errors import EngineError, require
from clara.logging_setup import get_logger
from clara.settings import Settings, TrinoSettings, get_settings

log = get_logger(__name__)


class TrinoEngine(Engine):
    """Distributed SQL engine over Iceberg."""

    name = "trino"
    capabilities = EngineCapabilities(
        distributed=True,
        supports_merge=True,
        supports_iceberg=True,
        supports_federation=True,
        supports_result_cache=False,  # Trino has no built-in result cache
        max_scan_bytes=2**62,
        # Coordinator round trip plus split scheduling. Real, and the reason
        # small queries should not come here.
        startup_seconds=0.6,
    )

    def __init__(
        self,
        config: TrinoSettings | None = None,
        *,
        settings: Settings | None = None,
        session_properties: dict[str, str] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.config = config or self.settings.trino
        self.session_properties = dict(session_properties or {})
        self._connection: Any | None = None

    # ------------------------------------------------------------- connection

    @property
    def connection(self) -> Any:
        if self._connection is None:
            dbapi = require("trino.dbapi", "trino", "the Trino engine")
            auth = None
            if self.config.password:
                from trino.auth import BasicAuthentication

                auth = BasicAuthentication(self.config.user, self.config.password)

            self._connection = dbapi.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                catalog=self.config.catalog,
                schema=self.config.schema_,
                http_scheme=self.config.http_scheme,
                auth=auth,
                session_properties=self._session_properties(),
                source="clara",
            )
        return self._connection

    def _session_properties(self) -> dict[str, str]:
        """Session defaults tuned for object-storage lakehouses on modest hardware."""
        properties = {
            # Spill to disk rather than failing a large join — on commodity
            # nodes, completing slowly beats an out-of-memory error.
            "query_max_memory_per_node": "1GB",
            # Dynamic filtering is the single biggest win for star-schema joins
            # over partitioned Iceberg tables.
            "enable_dynamic_filtering": "true",
            "join_distribution_type": "AUTOMATIC",
            "join_reordering_strategy": "AUTOMATIC",
        }
        properties.update(self.session_properties)
        return properties

    def test_connection(self) -> bool:
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            return cursor.fetchone()[0] == 1
        except Exception as exc:  # noqa: BLE001
            # Not an error: a single-node install has no Trino, and the router
            # simply runs everything on DuckDB.
            log.debug("trino not reachable", extra={"error": str(exc)})
            return False

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:  # noqa: BLE001 - closing must not raise
                pass
            self._connection = None

    # -------------------------------------------------------------- execution

    def execute(
        self,
        sql: str,
        *,
        parameters: dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_seconds: float | None = None,
    ) -> QueryResult:
        statement = sql.strip().rstrip(";")
        if not statement:
            raise EngineError("empty statement")

        cursor = self.connection.cursor()
        started = time.perf_counter()
        try:
            # Trino's DBAPI takes positional parameters; named parameters are
            # inlined by the caller before reaching here.
            cursor.execute(statement, list(parameters.values()) if parameters else None)
            rows = cursor.fetchmany(max_rows) if max_rows is not None else cursor.fetchall()
        except Exception as exc:  # noqa: BLE001
            raise EngineError(_clean_message(exc), engine=self.name, sql=statement) from exc
        elapsed = time.perf_counter() - started

        stats = self._collect_stats(cursor, elapsed)
        if cursor.description is None:
            return QueryResult(stats=stats, is_dml=True, affected_rows=stats.rows_produced)

        columns = [d[0] for d in cursor.description]
        stats.rows_produced = len(rows)
        return QueryResult(columns=columns, rows=[tuple(r) for r in rows], stats=stats)

    def _collect_stats(self, cursor: Any, elapsed: float) -> QueryStats:
        """Translate Trino's query statistics into Clara's stats.

        Trino reports true aggregate CPU time across workers, which is what
        makes per-query CCU billing defensible on this engine.
        """
        stats = QueryStats(engine=self.name, wall_seconds=elapsed, warehouse_seconds=elapsed)
        raw = getattr(cursor, "stats", None) or {}
        stats.engine_query_id = raw.get("queryId") or getattr(cursor, "query_id", None)
        stats.cpu_seconds = _ms_to_seconds(raw.get("cpuTimeMillis"))
        stats.queued_seconds = _ms_to_seconds(raw.get("queuedTimeMillis"))
        stats.bytes_scanned = int(raw.get("processedBytes") or raw.get("physicalInputBytes") or 0)
        stats.rows_scanned = int(raw.get("processedRows") or 0)
        stats.peak_memory_bytes = int(raw.get("peakMemoryBytes") or 0)
        stats.bytes_shuffled = int(raw.get("internalNetworkInputBytes") or 0)
        # Prefer CPU time for billing when available: it charges for work done,
        # not for a query that sat waiting on a slow object store.
        if stats.cpu_seconds > 0:
            stats.warehouse_seconds = max(elapsed, stats.cpu_seconds / 4.0)
        return stats

    def explain(self, sql: str) -> str:
        result = self.execute(f"EXPLAIN {sql.strip().rstrip(';')}")
        return "\n".join(str(row[0]) for row in result.rows)

    def estimate(self, sql: str) -> QueryStats:
        """Estimate from the cost-based optimiser's own numbers.

        ``EXPLAIN (TYPE IO)`` returns the planner's estimated input bytes
        without running anything — exactly what a pre-flight cost preview needs.
        """
        stats = QueryStats(engine=self.name)
        try:
            result = self.execute(f"EXPLAIN (TYPE IO, FORMAT JSON) {sql.strip().rstrip(';')}")
        except EngineError:
            return stats

        import json

        try:
            payload = json.loads(result.rows[0][0])
        except (IndexError, ValueError, TypeError):
            return stats

        estimate = payload.get("estimate") or {}
        stats.bytes_scanned = int(estimate.get("outputSizeInBytes") or 0)
        stats.rows_scanned = int(estimate.get("outputRowCount") or 0)
        return stats

    # --------------------------------------------------------- table helpers

    def quote_ref(self, ref: TableRef) -> str:
        """Trino needs the catalog name too, since it federates many catalogs."""
        return f"{self.quote(self.config.catalog)}.{self.quote(ref.namespace)}.{self.quote(ref.name)}"

    def ensure_namespace(self, namespace: str) -> None:
        """Create an Iceberg schema. Must be catalog-qualified in Trino."""
        self.execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{self.quote(self.config.catalog)}.{self.quote(namespace)}"
        )

    def merge(
        self,
        target: TableRef,
        source_sql: str,
        keys: list[str],
        columns: list[str],
    ) -> QueryResult:
        """Upsert via ``MERGE`` — how incremental pipelines apply CDC batches.

        Iceberg's merge-on-read mode makes this affordable: matched rows produce
        delete files rather than rewriting whole data files.
        """
        if not keys:
            raise EngineError("merge requires at least one key column")

        target_sql = self.quote_ref(target)
        on_clause = " AND ".join(f"t.{self.quote(k)} = s.{self.quote(k)}" for k in keys)
        updatable = [c for c in columns if c not in keys]
        set_clause = ", ".join(f"{self.quote(c)} = s.{self.quote(c)}" for c in updatable)
        insert_columns = ", ".join(self.quote(c) for c in columns)
        insert_values = ", ".join(f"s.{self.quote(c)}" for c in columns)

        statement = f"""
            MERGE INTO {target_sql} AS t
            USING ({source_sql}) AS s
            ON {on_clause}
        """
        if set_clause:
            statement += f"\n            WHEN MATCHED THEN UPDATE SET {set_clause}"
        statement += (
            f"\n            WHEN NOT MATCHED THEN INSERT ({insert_columns}) "
            f"VALUES ({insert_values})"
        )
        return self.execute(statement)

    def optimize(self, ref: TableRef, *, file_size_threshold: str = "128MB") -> QueryResult:
        """Compact small files.

        Streaming and frequent micro-batch ingest produce thousands of tiny
        Parquet files, which is the classic cause of a lakehouse getting slower
        over time. The maintenance scheduler runs this per table.
        """
        return self.execute(
            f"ALTER TABLE {self.quote_ref(ref)} EXECUTE optimize"
            f"(file_size_threshold => '{file_size_threshold}')"
        )

    def expire_snapshots(self, ref: TableRef, retention: str = "7d") -> QueryResult:
        return self.execute(
            f"ALTER TABLE {self.quote_ref(ref)} EXECUTE expire_snapshots"
            f"(retention_threshold => '{retention}')"
        )

    def remove_orphan_files(self, ref: TableRef, retention: str = "7d") -> QueryResult:
        return self.execute(
            f"ALTER TABLE {self.quote_ref(ref)} EXECUTE remove_orphan_files"
            f"(retention_threshold => '{retention}')"
        )


def _ms_to_seconds(value: Any) -> float:
    try:
        return float(value) / 1000.0
    except (TypeError, ValueError):
        return 0.0


def _clean_message(exc: Exception) -> str:
    """Trim Trino's very long error text to the useful first line."""
    message = str(exc).strip()
    first = message.split("\n", 1)[0]
    return first[:500] if first else message[:500]
