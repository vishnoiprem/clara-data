"""DuckDB engine — single-node execution.

DuckDB is Clara's answer to the fact that most analytical queries are small. It
starts in milliseconds, needs no cluster, and on anything under a few hundred
gigabytes it beats a distributed engine outright because there is no shuffle and
no coordination. Routing small queries here rather than to Trino is the single
largest cost saving in the platform.

Two modes:

* **Iceberg mode** — reads Iceberg tables from object storage via DuckDB's
  ``iceberg`` extension. Used in production for interactive queries.
* **Local mode** — exposes ``LocalCatalog`` tables as views over their Parquet
  or JSONL files. Used by ``clara init``, CI and the test suite, with no cloud
  and no catalog service.
"""

from __future__ import annotations

import time
from typing import Any

from clara.catalog.base import Catalog, TableRef
from clara.catalog.local import LocalCatalog
from clara.catalog.schema import Schema, from_arrow_schema, sql_type
from clara.engines.base import Engine, EngineCapabilities, QueryResult, QueryStats
from clara.errors import EngineError, require
from clara.logging_setup import get_logger
from clara.settings import DuckDBSettings, Settings, get_settings

log = get_logger(__name__)


class DuckDBEngine(Engine):
    """Embedded single-node SQL engine."""

    name = "duckdb"
    capabilities = EngineCapabilities(
        distributed=False,
        supports_merge=True,
        supports_iceberg=True,
        supports_federation=True,  # can attach Postgres/MySQL/SQLite directly
        supports_result_cache=False,
        # Above roughly this much scanned data a distributed engine wins; the
        # router uses this as its handover threshold.
        max_scan_bytes=200 * 1024**3,
        startup_seconds=0.05,
    )

    def __init__(
        self,
        config: DuckDBSettings | None = None,
        *,
        catalog: Catalog | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.config = config or self.settings.duckdb
        self.catalog = catalog
        self._connection: Any | None = None
        self._synced_tables: set[str] = set()

    # ------------------------------------------------------------- connection

    @property
    def connection(self) -> Any:
        """Lazily opened DuckDB connection, configured and extension-loaded."""
        if self._connection is None:
            duckdb = require("duckdb", "duckdb", "the DuckDB engine")
            path = self.config.path
            if path not in (":memory:", ""):
                resolved = self.settings.path_in_state(path) if not path.startswith("/") else path
                path = str(resolved)
            else:
                path = ":memory:"

            self._connection = duckdb.connect(path)
            self._configure(self._connection)
        return self._connection

    def _configure(self, connection: Any) -> None:
        connection.execute(f"SET memory_limit='{self.config.memory_limit}'")
        connection.execute(f"SET threads={self.config.threads}")
        # Trino-compatible behaviour so the same SQL runs on both engines.
        connection.execute("SET timezone='UTC'")

        for extension in ("httpfs", "iceberg"):
            try:
                connection.execute(f"INSTALL {extension}")
                connection.execute(f"LOAD {extension}")
            except Exception as exc:  # noqa: BLE001 - extensions are optional
                log.debug(
                    "duckdb extension unavailable",
                    extra={"extension": extension, "error": str(exc)},
                )

        # Credentials for object storage, when the provider supplies them.
        from clara.providers import get_provider

        secret_sql = get_provider(self.settings).duckdb_secret_sql()
        if secret_sql:
            try:
                connection.execute(secret_sql)
            except Exception as exc:  # noqa: BLE001
                log.warning("could not create duckdb storage secret", extra={"error": str(exc)})

    def test_connection(self) -> bool:
        try:
            return self.connection.execute("SELECT 1").fetchone()[0] == 1
        except Exception as exc:  # noqa: BLE001
            log.warning("duckdb connection failed", extra={"error": str(exc)})
            return False

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._synced_tables.clear()

    # ---------------------------------------------------------------- catalog

    def sync_catalog(self, namespace: str | None = None, *, force: bool = False) -> int:
        """Expose catalog tables to SQL, returning how many were registered.

        For a ``LocalCatalog`` each table becomes a view over its data files.
        Called automatically before a query references an unknown table, so
        callers never have to think about it.
        """
        if self.catalog is None:
            return 0
        registered = 0
        refs = self.catalog.list_tables(namespace)

        # Tables before views: a view's SQL references tables, so registering it
        # first would fail on a missing relation.
        def is_view(ref: TableRef) -> bool:
            checker = getattr(self.catalog, "is_view", None)
            return bool(checker(ref)) if callable(checker) else False

        for ref in sorted(refs, key=is_view):
            if not force and ref.fqn in self._synced_tables:
                continue
            try:
                self._register_table(ref)
                registered += 1
            except Exception as exc:  # noqa: BLE001 - one bad table must not block the rest
                log.warning(
                    "could not register table", extra={"table": ref.fqn, "error": str(exc)}
                )
        return registered

    def _register_table(self, ref: TableRef) -> None:
        assert self.catalog is not None
        info = self.catalog.load_table(ref)
        connection = self.connection
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self.quote(ref.namespace)}")

        if info.format == "view":
            view_sql = info.properties.get("clara.view-sql")
            if not view_sql:
                raise EngineError(f"view {ref.fqn} has no stored SQL")
            connection.execute(f"CREATE OR REPLACE VIEW {self.quote_ref(ref)} AS {view_sql}")
            self._synced_tables.add(ref.fqn)
            return
        if isinstance(self.catalog, LocalCatalog):
            source = self._local_source_sql(ref, info.schema)
        elif info.format == "iceberg" and info.location:
            source = f"SELECT * FROM iceberg_scan('{info.location}', allow_moved_paths => true)"
        else:
            raise EngineError(f"cannot expose {ref.fqn} ({info.format}) to DuckDB")

        connection.execute(f"CREATE OR REPLACE VIEW {self.quote_ref(ref)} AS {source}")
        self._synced_tables.add(ref.fqn)

    def _local_source_sql(self, ref: TableRef, schema: Schema) -> str:
        """SQL that reads a local table's files — or an empty typed relation."""
        assert isinstance(self.catalog, LocalCatalog)
        try:
            pattern = self.catalog.data_glob(ref)
        except Exception:  # noqa: BLE001 - in-memory catalog has no files
            pattern = None

        files = self.catalog.data_files(ref) if pattern else []
        if not files:
            # An empty table still has to be queryable with the right column
            # types, otherwise the first pipeline run fails before any data lands.
            columns = ", ".join(
                f"CAST(NULL AS {sql_type(f.type, 'duckdb', f.precision, f.scale)}) AS {self.quote(f.name)}"
                for f in schema.fields
            )
            return f"SELECT {columns} WHERE 1 = 0"

        reader = "read_parquet" if pattern.endswith(".parquet") else "read_json_auto"
        return f"SELECT * FROM {reader}('{pattern}')"

    def materialize(
        self,
        ref: TableRef,
        sql: str,
        *,
        mode: str = "table",
        unique_key: list[str] | None = None,
        incremental: bool = False,
    ) -> QueryResult:
        """Materialise a model.

        Over a ``LocalCatalog`` the result is written *through the catalog*
        rather than into DuckDB's own storage. Without this, model outputs would
        live only inside the DuckDB file — invisible to the catalog, to other
        engines, and to the next process that opens an in-memory database.
        """
        if not isinstance(self.catalog, LocalCatalog):
            return super().materialize(
                ref, sql, mode=mode, unique_key=unique_key, incremental=incremental
            )

        self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self.quote(ref.namespace)}")
        started = time.perf_counter()

        if mode == "view":
            # Probe for the column types without computing any rows.
            probe = self.connection.execute(
                f"SELECT * FROM ({sql}) AS _clara_probe WHERE 1 = 0"
            ).fetch_arrow_table()
            self.catalog.create_view(ref, sql, from_arrow_schema(probe.schema))
            self.connection.execute(f"CREATE OR REPLACE VIEW {self.quote_ref(ref)} AS {sql}")
            self._synced_tables.add(ref.fqn)
            return QueryResult(
                stats=QueryStats(
                    engine=self.name,
                    wall_seconds=time.perf_counter() - started,
                    warehouse_seconds=time.perf_counter() - started,
                ),
                is_dml=True,
            )

        arrow = self.connection.execute(sql).fetch_arrow_table()
        schema = from_arrow_schema(arrow.schema)
        schema.primary_key = list(unique_key or [])
        rows = arrow.to_pylist()

        if incremental and self.catalog.table_exists(ref):
            self.catalog.evolve_schema(ref, schema)
            if unique_key:
                # No MERGE on files: replace the keys present in this batch so a
                # re-run is idempotent rather than duplicating rows.
                rows = self._apply_upsert(ref, rows, unique_key)
                written = len(rows)
            else:
                written = self.catalog.append(ref, rows)
        else:
            if self.catalog.table_exists(ref):
                self.catalog.drop_table(ref, purge=True)
            self.catalog.create_table(ref, schema)
            written = self.catalog.append(ref, rows)

        self._register_table(ref)
        elapsed = time.perf_counter() - started
        return QueryResult(
            stats=QueryStats(
                engine=self.name,
                wall_seconds=elapsed,
                warehouse_seconds=elapsed,
                rows_produced=written,
            ),
            is_dml=True,
            affected_rows=written,
        )

    def _apply_upsert(
        self, ref: TableRef, rows: list[dict[str, Any]], unique_key: list[str]
    ) -> list[dict[str, Any]]:
        """Emulate an upsert by rewriting the table without the affected keys."""
        assert isinstance(self.catalog, LocalCatalog)

        def key_of(record: dict[str, Any]) -> tuple:
            return tuple(record.get(k) for k in unique_key)

        incoming = {key_of(r): r for r in rows}
        existing = [r for r in self.catalog.scan(ref) if key_of(r) not in incoming]
        merged = [*existing, *incoming.values()]

        schema = self.catalog.load_table(ref).schema
        self.catalog.drop_table(ref, purge=True)
        self.catalog.create_table(ref, schema)
        self.catalog.append(ref, merged)
        return merged

    def register_arrow(self, name: str, table: Any) -> None:
        """Register an Arrow table as a queryable relation. Used by connectors
        to transform in flight without a round trip through storage."""
        self.connection.register(name, table)

    # ------------------------------------------------------------- execution

    def execute(
        self,
        sql: str,
        *,
        parameters: dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_seconds: float | None = None,
    ) -> QueryResult:
        connection = self.connection
        statement = sql.strip().rstrip(";")
        if not statement:
            raise EngineError("empty statement")

        started = time.perf_counter()
        try:
            cursor = self._run(connection, statement, parameters)
        except Exception as exc:  # noqa: BLE001
            # A missing table is usually just an unsynced catalog, so retry once.
            if self.catalog is not None and _is_missing_table(exc):
                self.sync_catalog(force=True)
                try:
                    cursor = self._run(connection, statement, parameters)
                except Exception as retry_exc:  # noqa: BLE001
                    raise EngineError(str(retry_exc), engine=self.name, sql=statement) from retry_exc
            else:
                raise EngineError(str(exc), engine=self.name, sql=statement) from exc

        elapsed = time.perf_counter() - started
        return self._to_result(cursor, elapsed, max_rows)

    @staticmethod
    def _run(connection: Any, statement: str, parameters: dict[str, Any] | None) -> Any:
        if parameters:
            # DuckDB uses $name placeholders for dict parameters.
            return connection.execute(statement, parameters)
        return connection.execute(statement)

    def _to_result(self, cursor: Any, elapsed: float, max_rows: int | None) -> QueryResult:
        stats = QueryStats(
            engine=self.name,
            wall_seconds=elapsed,
            # Single-node: wall time is the resource consumed, scaled by threads.
            cpu_seconds=elapsed * self.config.threads,
            warehouse_seconds=elapsed,
        )

        if cursor.description is None:
            return QueryResult(stats=stats, is_dml=True)

        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchmany(max_rows) if max_rows is not None else cursor.fetchall()
        stats.rows_produced = len(rows)

        # A single-column single-row integer result from a DML statement is
        # DuckDB's "rows changed" report.
        affected = None
        if len(columns) == 1 and columns[0].lower() in {"count", "changes"} and len(rows) == 1:
            affected = int(rows[0][0])

        return QueryResult(
            columns=columns,
            rows=[tuple(r) for r in rows],
            stats=stats,
            affected_rows=affected,
        )

    def explain(self, sql: str) -> str:
        result = self.connection.execute(f"EXPLAIN {sql.strip().rstrip(';')}").fetchall()
        return "\n".join(str(row[-1]) for row in result)

    def estimate(self, sql: str) -> QueryStats:
        """Estimate via ``EXPLAIN``, reading the planner's cardinality guess."""
        stats = QueryStats(engine=self.name)
        try:
            plan = self.explain(sql)
        except Exception:  # noqa: BLE001 - estimation must never fail a request
            return stats

        import re

        estimates = [int(m) for m in re.findall(r"EC[:=]\s*(\d+)", plan)]
        if estimates:
            stats.rows_scanned = max(estimates)
            # Rough per-row width; good enough to distinguish a small query
            # from a large one, which is all the router needs.
            stats.bytes_scanned = stats.rows_scanned * 120
        return stats

    # --------------------------------------------------------------- dialect

    def quote(self, identifier: str) -> str:
        return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _is_missing_table(exc: Exception) -> bool:
    """Whether a DuckDB error looks like an unregistered catalog table.

    Used to decide whether re-syncing the catalog and retrying is worthwhile,
    rather than resyncing on every failure.
    """
    message = str(exc).lower()
    return any(
        phrase in message
        for phrase in ("does not exist", "not found", "no such table", "catalog error")
    )
