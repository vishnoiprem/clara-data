"""Source and destination contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from clara.connectors.protocol import (
    AirbyteMessage,
    ConfiguredStream,
    StreamDescriptor,
)
from clara.errors import ConnectorError, ValidationError


@dataclass
class ConnectorSpec:
    """What a connector is and what configuration it needs.

    The JSON-schema config is what the UI renders as a form, so a business user
    can connect a database without reading documentation — a prerequisite for
    the "no data engineer" promise.
    """

    name: str
    title: str
    #: JSON Schema for the connector's configuration.
    config_schema: dict[str, Any] = field(default_factory=dict)
    documentation_url: str | None = None
    supports_incremental: bool = False
    #: Config keys whose values must never be logged or returned by the API.
    secret_fields: tuple[str, ...] = ()

    def validate_config(self, config: dict[str, Any]) -> None:
        """Check required keys and reject unknown ones.

        A deliberately small validator rather than a full JSON-schema
        implementation: it catches the mistakes users actually make (missing
        host, typo in a key) without adding a dependency.
        """
        properties = self.config_schema.get("properties", {})
        required = self.config_schema.get("required", [])

        missing = [key for key in required if config.get(key) in (None, "")]
        if missing:
            raise ValidationError(
                f"{self.name}: missing required configuration", missing=missing
            )
        if properties and not self.config_schema.get("additionalProperties", False):
            unknown = [key for key in config if key not in properties]
            if unknown:
                raise ValidationError(
                    f"{self.name}: unknown configuration keys",
                    unknown=unknown,
                    accepted=sorted(properties),
                )

    def redact(self, config: dict[str, Any]) -> dict[str, Any]:
        """Config with secrets masked, safe for logs and API responses."""
        return {
            key: ("***" if key in self.secret_fields and value else value)
            for key, value in config.items()
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "title": self.title,
            "config_schema": self.config_schema,
            "documentation_url": self.documentation_url,
            "supports_incremental": self.supports_incremental,
            "secret_fields": list(self.secret_fields),
        }


@dataclass
class CheckResult:
    """Outcome of a connection test."""

    succeeded: bool
    message: str = ""
    #: Filled in on success when the connector can cheaply report scale.
    detected_streams: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "message": self.message,
            "detected_streams": self.detected_streams,
        }


class Source(ABC):
    """A data source."""

    #: Registry key, e.g. ``postgres``.
    name: str = "abstract"

    def __init__(self, config: dict[str, Any]) -> None:
        self.spec().validate_config(config)
        self.config = config

    @classmethod
    @abstractmethod
    def spec(cls) -> ConnectorSpec:
        """Describe this connector and its configuration."""

    @abstractmethod
    def check(self) -> CheckResult:
        """Verify credentials and reachability without reading data."""

    @abstractmethod
    def discover(self) -> list[StreamDescriptor]:
        """List available streams and their schemas."""

    @abstractmethod
    def read(
        self,
        streams: list[ConfiguredStream],
        state: dict[str, Any] | None = None,
    ) -> Iterator[AirbyteMessage]:
        """Emit records and state checkpoints for the configured streams.

        Implementations must emit a STATE message only after the records it
        covers have been yielded, so an interrupted sync resumes without
        losing rows.
        """

    # ---------------------------------------------------------------- helpers

    def stream_by_name(self, name: str) -> StreamDescriptor:
        found = next((s for s in self.discover() if s.name == name), None)
        if found is None:
            raise ConnectorError(f"{self.name}: unknown stream {name!r}")
        return found

    def configured_streams(self, *names: str) -> list[ConfiguredStream]:
        """Convenience: configure streams by name with connector defaults."""
        discovered = {s.name: s for s in self.discover()}
        selected = names or tuple(discovered)
        return [ConfiguredStream(stream=discovered[n]) for n in selected if n in discovered]

    def close(self) -> None:
        """Release connections."""

    def __enter__(self) -> Source:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"<{type(self).__name__} name={self.name}>"


@dataclass
class WriteResult:
    """What one destination write accomplished."""

    records_written: int = 0
    bytes_written: int = 0
    table: str | None = None
    #: True when the write created or altered the destination table.
    schema_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "records_written": self.records_written,
            "bytes_written": self.bytes_written,
            "table": self.table,
            "schema_changed": self.schema_changed,
        }


class Destination(ABC):
    """A write target."""

    name: str = "abstract"

    def begin_sync(self) -> None:
        """Reset any per-sync state. Called once at the start of each sync."""

    @abstractmethod
    def prepare(self, stream: ConfiguredStream, namespace: str) -> None:
        """Create or evolve the target table before records arrive."""

    @abstractmethod
    def write(
        self,
        stream: ConfiguredStream,
        namespace: str,
        records: list[dict[str, Any]],
    ) -> WriteResult:
        """Persist a batch of records."""

    def finalize(self, stream: ConfiguredStream, namespace: str) -> None:
        """Run after the last batch — commit, compact, or swap in a staging table."""

    def close(self) -> None:
        """Release resources."""

    def __enter__(self) -> Destination:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
