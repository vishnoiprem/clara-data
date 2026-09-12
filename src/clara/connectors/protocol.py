"""The connector wire protocol.

Clara's ingest protocol is deliberately message-compatible with Airbyte's. That
choice buys the ecosystem for free: any of the hundreds of existing Airbyte
source images can be driven by Clara's runner without writing an adapter, while
native Python connectors (faster, no container overhead) implement the same
interface in-process.

A connector emits a stream of newline-delimited JSON messages. The important
ones are RECORD (data), STATE (a resumable checkpoint) and LOG/TRACE
(diagnostics).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from clara.time_utils import utcnow


class MessageType(str, Enum):
    RECORD = "RECORD"
    STATE = "STATE"
    LOG = "LOG"
    SPEC = "SPEC"
    CONNECTION_STATUS = "CONNECTION_STATUS"
    CATALOG = "CATALOG"
    TRACE = "TRACE"


class SyncMode(str, Enum):
    """How a stream is read."""

    #: Read everything, every time. Correct, simple, expensive.
    FULL_REFRESH = "full_refresh"
    #: Read only what changed since the last checkpoint.
    INCREMENTAL = "incremental"


class WriteMode(str, Enum):
    """How a stream is written to the destination."""

    #: Add rows; never modify existing ones.
    APPEND = "append"
    #: Replace the table contents atomically.
    OVERWRITE = "overwrite"
    #: Upsert on the primary key.
    MERGE = "merge"


@dataclass
class AirbyteRecord:
    """One data row."""

    stream: str
    data: dict[str, Any]
    emitted_at: int = field(default_factory=lambda: int(utcnow().timestamp() * 1000))
    namespace: str | None = None


@dataclass
class AirbyteState:
    """A resumable checkpoint.

    State is what makes incremental sync safe: it is emitted *after* the records
    it covers have been handed to the destination, so a crash replays rather
    than skips.
    """

    stream: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class AirbyteMessage:
    """One protocol message."""

    type: MessageType
    record: AirbyteRecord | None = None
    state: AirbyteState | None = None
    log: dict[str, Any] | None = None
    spec: dict[str, Any] | None = None
    connection_status: dict[str, Any] | None = None
    catalog: dict[str, Any] | None = None
    trace: dict[str, Any] | None = None

    # ------------------------------------------------------------ constructors

    @classmethod
    def record_message(
        cls, stream: str, data: dict[str, Any], namespace: str | None = None
    ) -> AirbyteMessage:
        return cls(
            type=MessageType.RECORD,
            record=AirbyteRecord(stream=stream, data=data, namespace=namespace),
        )

    @classmethod
    def state_message(cls, data: dict[str, Any], stream: str | None = None) -> AirbyteMessage:
        return cls(type=MessageType.STATE, state=AirbyteState(stream=stream, data=data))

    @classmethod
    def log_message(cls, level: str, message: str) -> AirbyteMessage:
        return cls(type=MessageType.LOG, log={"level": level.upper(), "message": message})

    @classmethod
    def status_message(cls, succeeded: bool, message: str = "") -> AirbyteMessage:
        return cls(
            type=MessageType.CONNECTION_STATUS,
            connection_status={
                "status": "SUCCEEDED" if succeeded else "FAILED",
                "message": message,
            },
        )

    # ---------------------------------------------------------- serialisation

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type.value}
        if self.record is not None:
            payload["record"] = {
                "stream": self.record.stream,
                "data": self.record.data,
                "emitted_at": self.record.emitted_at,
                **({"namespace": self.record.namespace} if self.record.namespace else {}),
            }
        if self.state is not None:
            payload["state"] = {
                "data": self.state.data,
                **({"stream": self.state.stream} if self.state.stream else {}),
            }
        for name in ("log", "spec", "connection_status", "catalog", "trace"):
            value = getattr(self, name)
            if value is not None:
                payload[name] = value
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, separators=(",", ":"))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AirbyteMessage:
        message_type = MessageType(payload["type"])
        record = None
        if raw := payload.get("record"):
            record = AirbyteRecord(
                stream=raw["stream"],
                data=raw.get("data", {}),
                emitted_at=raw.get("emitted_at", 0),
                namespace=raw.get("namespace"),
            )
        state = None
        if raw := payload.get("state"):
            state = AirbyteState(stream=raw.get("stream"), data=raw.get("data", {}))
        return cls(
            type=message_type,
            record=record,
            state=state,
            log=payload.get("log"),
            spec=payload.get("spec"),
            connection_status=payload.get("connectionStatus") or payload.get("connection_status"),
            catalog=payload.get("catalog"),
            trace=payload.get("trace"),
        )

    @classmethod
    def from_json(cls, line: str) -> AirbyteMessage | None:
        """Parse one protocol line, tolerating the noise real connectors emit.

        Container connectors interleave plain stdout with protocol JSON, so an
        unparseable line is skipped rather than failing the sync.
        """
        text = line.strip()
        if not text or not text.startswith("{"):
            return None
        try:
            return cls.from_dict(json.loads(text))
        except (json.JSONDecodeError, KeyError, ValueError):
            return None


@dataclass
class StreamDescriptor:
    """One discoverable stream (a table, endpoint or collection)."""

    name: str
    json_schema: dict[str, Any] = field(default_factory=dict)
    supported_sync_modes: list[SyncMode] = field(
        default_factory=lambda: [SyncMode.FULL_REFRESH]
    )
    #: Fields usable as an incremental cursor (e.g. ``updated_at``).
    default_cursor_field: list[str] = field(default_factory=list)
    source_defined_primary_key: list[list[str]] = field(default_factory=list)
    namespace: str | None = None

    @property
    def supports_incremental(self) -> bool:
        return SyncMode.INCREMENTAL in self.supported_sync_modes

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "json_schema": self.json_schema,
            "supported_sync_modes": [m.value for m in self.supported_sync_modes],
            "default_cursor_field": self.default_cursor_field,
            "source_defined_primary_key": self.source_defined_primary_key,
            "namespace": self.namespace,
        }

    def to_schema(self):  # noqa: ANN201 - avoids importing Schema at module load
        """Convert the stream's JSON schema into a Clara table schema."""
        from clara.catalog.schema import DataType, Field_, Schema

        properties = (self.json_schema or {}).get("properties", {})
        required = set((self.json_schema or {}).get("required", []))
        fields = []
        for name, definition in properties.items():
            fields.append(
                Field_(
                    name=name,
                    type=_json_type_to_clara(definition),
                    nullable=name not in required,
                )
            )
        primary_key = [k[0] for k in self.source_defined_primary_key if k]
        return Schema(fields=fields, primary_key=primary_key)


@dataclass
class ConfiguredStream:
    """A stream plus the choices made about how to sync it."""

    stream: StreamDescriptor
    sync_mode: SyncMode = SyncMode.FULL_REFRESH
    write_mode: WriteMode = WriteMode.APPEND
    cursor_field: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    #: Destination table name; defaults to the stream name.
    destination_table: str | None = None

    def __post_init__(self) -> None:
        if not self.cursor_field:
            self.cursor_field = list(self.stream.default_cursor_field)
        if not self.primary_key:
            self.primary_key = [k[0] for k in self.stream.source_defined_primary_key if k]
        # Merge requires a key; fall back to append rather than silently
        # producing duplicates under a merge label.
        if self.write_mode is WriteMode.MERGE and not self.primary_key:
            self.write_mode = WriteMode.APPEND

    @property
    def target_table(self) -> str:
        return self.destination_table or self.stream.name

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream.to_dict(),
            "sync_mode": self.sync_mode.value,
            "write_mode": self.write_mode.value,
            "cursor_field": self.cursor_field,
            "primary_key": self.primary_key,
            "destination_table": self.target_table,
        }


def _json_type_to_clara(definition: dict[str, Any]) -> Any:
    """Map a JSON-schema type to a Clara ``DataType``."""
    from clara.catalog.schema import DataType

    raw = definition.get("type", "string")
    types = [t for t in (raw if isinstance(raw, list) else [raw]) if t != "null"]
    primary = types[0] if types else "string"
    fmt = definition.get("format", "")
    airbyte_type = definition.get("airbyte_type", "")

    if primary == "boolean":
        return DataType.BOOLEAN
    if primary == "integer":
        return DataType.LONG
    if primary == "number":
        return DataType.DECIMAL if airbyte_type == "big_number" else DataType.DOUBLE
    if primary in ("object", "array"):
        return DataType.JSON
    if fmt == "date":
        return DataType.DATE
    if fmt == "time":
        return DataType.TIME
    if fmt == "date-time":
        return (
            DataType.TIMESTAMP
            if airbyte_type == "timestamp_without_timezone"
            else DataType.TIMESTAMPTZ
        )
    return DataType.STRING
