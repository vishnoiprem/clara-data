"""PostgreSQL source.

The most common source in a mid-sized company: the production database. Two
properties matter for cost and safety —

* **Server-side cursors.** Rows stream in fixed batches instead of being
  materialised in memory, so a 200-million-row table syncs in a container with
  a few hundred megabytes of RAM rather than needing a large machine.
* **Cursor-based incremental sync.** After the first load, only rows with a
  cursor value greater than the last checkpoint are read. On a large table this
  is the difference between a nightly full scan of the production primary and a
  few seconds of work.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from clara.connectors.base import CheckResult, ConnectorSpec, Source
from clara.connectors.protocol import (
    AirbyteMessage,
    ConfiguredStream,
    StreamDescriptor,
    SyncMode,
)
from clara.errors import ConnectorError, require
from clara.logging_setup import get_logger

log = get_logger(__name__)

#: Rows fetched per round trip. Large enough to amortise latency, small enough
#: to keep memory flat.
FETCH_SIZE = 5_000

#: Column types usable as an incremental cursor — monotonic, indexable.
_CURSOR_TYPES = {
    "timestamp with time zone",
    "timestamp without time zone",
    "date",
    "bigint",
    "integer",
    "smallint",
    "numeric",
}

_PG_TO_JSON: dict[str, dict[str, Any]] = {
    "boolean": {"type": "boolean"},
    "smallint": {"type": "integer"},
    "integer": {"type": "integer"},
    "bigint": {"type": "integer"},
    "real": {"type": "number"},
    "double precision": {"type": "number"},
    "numeric": {"type": "number", "airbyte_type": "big_number"},
    "date": {"type": "string", "format": "date"},
    "time without time zone": {"type": "string", "format": "time"},
    "timestamp without time zone": {
        "type": "string",
        "format": "date-time",
        "airbyte_type": "timestamp_without_timezone",
    },
    "timestamp with time zone": {"type": "string", "format": "date-time"},
    "json": {"type": "object"},
    "jsonb": {"type": "object"},
    "ARRAY": {"type": "array"},
    "bytea": {"type": "string"},
    "uuid": {"type": "string"},
}


class PostgresSource(Source):
    """Reads tables from PostgreSQL."""

    name = "postgres"

    @classmethod
    def spec(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name=cls.name,
            title="PostgreSQL",
            supports_incremental=True,
            secret_fields=("password",),
            documentation_url="https://github.com/clara-data/clara/tree/main/docs/connectors/postgres.md",
            config_schema={
                "type": "object",
                "required": ["host", "database", "username"],
                "properties": {
                    "host": {"type": "string", "title": "Host"},
                    "port": {"type": "integer", "title": "Port", "default": 5432},
                    "database": {"type": "string", "title": "Database"},
                    "username": {"type": "string", "title": "Username"},
                    "password": {"type": "string", "title": "Password", "airbyte_secret": True},
                    "schemas": {
                        "type": "array",
                        "title": "Schemas",
                        "items": {"type": "string"},
                        "default": ["public"],
                    },
                    "ssl_mode": {
                        "type": "string",
                        "title": "SSL mode",
                        "default": "prefer",
                        "enum": ["disable", "allow", "prefer", "require", "verify-full"],
                    },
                },
            },
        )

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._connection: Any | None = None

    # ------------------------------------------------------------- connection

    @property
    def connection(self) -> Any:
        if self._connection is None:
            psycopg = require("psycopg", "postgres", "the PostgreSQL source")
            try:
                self._connection = psycopg.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 5432),
                    dbname=self.config["database"],
                    user=self.config["username"],
                    password=self.config.get("password"),
                    sslmode=self.config.get("ssl_mode", "prefer"),
                    # Reading a replica should never block production writes.
                    options="-c default_transaction_read_only=on",
                    connect_timeout=15,
                )
            except Exception as exc:  # noqa: BLE001
                raise ConnectorError(
                    f"postgres connection failed: {exc}",
                    host=self.config["host"],
                    database=self.config["database"],
                ) from exc
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    # ------------------------------------------------------------------ checks

    def check(self) -> CheckResult:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
            streams = len(self.discover())
            return CheckResult(
                succeeded=True,
                message=version.split(" on ")[0],
                detected_streams=streams,
            )
        except Exception as exc:  # noqa: BLE001 - check must report, not raise
            return CheckResult(succeeded=False, message=str(exc))

    def discover(self) -> list[StreamDescriptor]:
        """Read table and column metadata from ``information_schema``."""
        schemas = self.config.get("schemas") or ["public"]
        query = """
            SELECT c.table_schema, c.table_name, c.column_name,
                   c.data_type, c.is_nullable
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE c.table_schema = ANY(%s) AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (list(schemas),))
            rows = cursor.fetchall()

        primary_keys = self._primary_keys(schemas)
        grouped: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
        for schema, table, column, data_type, nullable in rows:
            grouped.setdefault((schema, table), []).append((column, data_type, nullable))

        streams: list[StreamDescriptor] = []
        for (schema, table), columns in grouped.items():
            properties: dict[str, Any] = {}
            required: list[str] = []
            cursor_candidates: list[str] = []
            for column, data_type, nullable in columns:
                properties[column] = dict(_PG_TO_JSON.get(data_type, {"type": "string"}))
                if nullable == "NO":
                    required.append(column)
                if data_type in _CURSOR_TYPES:
                    cursor_candidates.append(column)

            # Prefer a conventional audit column as the default cursor.
            default_cursor = next(
                (c for c in ("updated_at", "modified_at", "created_at", "id") if c in cursor_candidates),
                cursor_candidates[0] if cursor_candidates else None,
            )
            keys = primary_keys.get((schema, table), [])
            streams.append(
                StreamDescriptor(
                    name=f"{schema}_{table}" if schema != "public" else table,
                    namespace=schema,
                    json_schema={
                        "type": "object",
                        "properties": properties,
                        "required": required,
                    },
                    supported_sync_modes=(
                        [SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL]
                        if default_cursor
                        else [SyncMode.FULL_REFRESH]
                    ),
                    default_cursor_field=[default_cursor] if default_cursor else [],
                    source_defined_primary_key=[[k] for k in keys],
                )
            )
        return streams

    def _primary_keys(self, schemas: list[str]) -> dict[tuple[str, str], list[str]]:
        query = """
            SELECT tc.table_schema, tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = ANY(%s)
            ORDER BY kcu.ordinal_position
        """
        keys: dict[tuple[str, str], list[str]] = {}
        with self.connection.cursor() as cursor:
            cursor.execute(query, (list(schemas),))
            for schema, table, column in cursor.fetchall():
                keys.setdefault((schema, table), []).append(column)
        return keys

    # ------------------------------------------------------------------- read

    def read(
        self,
        streams: list[ConfiguredStream],
        state: dict[str, Any] | None = None,
    ) -> Iterator[AirbyteMessage]:
        state = dict(state or {})

        for configured in streams:
            descriptor = configured.stream
            schema_name = descriptor.namespace or "public"
            table_name = descriptor.name.removeprefix(f"{schema_name}_")
            qualified = f'"{schema_name}"."{table_name}"'

            cursor_field = configured.cursor_field[0] if configured.cursor_field else None
            incremental = configured.sync_mode is SyncMode.INCREMENTAL and cursor_field
            last_value = (state.get(descriptor.name) or {}).get("cursor") if incremental else None

            sql = f"SELECT * FROM {qualified}"
            params: list[Any] = []
            if incremental and last_value is not None:
                # Inclusive comparison would replay the boundary row; exclusive
                # would drop rows sharing a timestamp. Inclusive plus dedup on
                # the primary key downstream is the safe choice.
                sql += f' WHERE "{cursor_field}" >= %s'
                params.append(last_value)
            if incremental:
                sql += f' ORDER BY "{cursor_field}" ASC'

            yield AirbyteMessage.log_message(
                "INFO",
                f"reading {qualified}"
                + (f" incrementally from {last_value}" if last_value else " (full refresh)"),
            )

            max_cursor = last_value
            count = 0
            # A named cursor is a server-side cursor: rows are not buffered
            # client-side, which is what keeps memory flat on huge tables.
            with self.connection.cursor(name=f"clara_{descriptor.name}") as cursor:
                cursor.itersize = FETCH_SIZE
                cursor.execute(sql, params or None)
                columns = [d.name for d in cursor.description]

                for row in cursor:
                    record = dict(zip(columns, row, strict=False))
                    if cursor_field and record.get(cursor_field) is not None:
                        value = record[cursor_field]
                        if max_cursor is None or _gt(value, max_cursor):
                            max_cursor = value
                    count += 1
                    yield AirbyteMessage.record_message(
                        descriptor.name, record, namespace=schema_name
                    )

                    # Periodic checkpoints so a failure part-way through a large
                    # table does not discard hours of work.
                    if incremental and count % (FETCH_SIZE * 10) == 0:
                        state[descriptor.name] = {"cursor": _serialize(max_cursor)}
                        yield AirbyteMessage.state_message(dict(state), descriptor.name)

            if incremental and max_cursor is not None:
                state[descriptor.name] = {"cursor": _serialize(max_cursor)}
            yield AirbyteMessage.state_message(dict(state), descriptor.name)
            yield AirbyteMessage.log_message("INFO", f"read {count:,} rows from {qualified}")


def _gt(left: Any, right: Any) -> bool:
    """Comparison that tolerates the mixed types a cursor column can hold."""
    try:
        return bool(left > right)
    except TypeError:
        return str(left) > str(right)


def _serialize(value: Any) -> Any:
    """Make a cursor value JSON-safe for the state checkpoint."""
    import datetime
    import decimal

    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    return value
