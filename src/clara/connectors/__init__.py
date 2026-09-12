"""Data ingestion.

Connectors are registered in a plugin registry, so a customer or consultant can
ship a private connector without forking Clara:

    from clara.connectors import register_source
    register_source(MySaasSource)

Third-party packages can also publish a ``clara.sources`` entry point, which is
discovered automatically. Airbyte-protocol container images are usable through
the same runner, since Clara's protocol is message-compatible.
"""

from __future__ import annotations

from clara.connectors.base import (
    CheckResult,
    ConnectorSpec,
    Destination,
    Source,
    WriteResult,
)
from clara.connectors.destinations import (
    ConsoleDestination,
    LakehouseDestination,
    build_destination,
)
from clara.connectors.protocol import (
    AirbyteMessage,
    ConfiguredStream,
    MessageType,
    StreamDescriptor,
    SyncMode,
    WriteMode,
)
from clara.connectors.runner import (
    DEFAULT_BATCH_SIZE,
    StreamResult,
    SyncResult,
    SyncRunner,
    configure_streams,
)
from clara.connectors.sources import HttpFileSource, PostgresSource, SampleSource
from clara.errors import ConnectorError, ValidationError
from clara.logging_setup import get_logger

log = get_logger(__name__)

__all__ = [
    "AirbyteMessage",
    "CheckResult",
    "ConfiguredStream",
    "ConnectorSpec",
    "ConsoleDestination",
    "DEFAULT_BATCH_SIZE",
    "Destination",
    "HttpFileSource",
    "LakehouseDestination",
    "MessageType",
    "PostgresSource",
    "SampleSource",
    "Source",
    "StreamDescriptor",
    "StreamResult",
    "SyncMode",
    "SyncResult",
    "SyncRunner",
    "WriteMode",
    "WriteResult",
    "available_sources",
    "build_destination",
    "build_source",
    "configure_streams",
    "register_source",
    "source_specs",
]

_SOURCES: dict[str, type[Source]] = {
    SampleSource.name: SampleSource,
    PostgresSource.name: PostgresSource,
    HttpFileSource.name: HttpFileSource,
}
_ENTRY_POINTS_LOADED = False


def register_source(source: type[Source], *, replace: bool = False) -> None:
    """Register a source connector under its ``name``."""
    if not source.name or source.name == "abstract":
        raise ValidationError(f"{source.__name__} must define a unique 'name'")
    if source.name in _SOURCES and not replace:
        raise ValidationError(f"source already registered: {source.name}")
    _SOURCES[source.name] = source
    log.debug("registered source", extra={"source": source.name})


def _load_entry_points() -> None:
    global _ENTRY_POINTS_LOADED
    if _ENTRY_POINTS_LOADED:
        return
    _ENTRY_POINTS_LOADED = True
    from importlib.metadata import entry_points

    for entry in entry_points(group="clara.sources"):
        try:
            register_source(entry.load(), replace=True)
        except Exception as exc:  # noqa: BLE001 - a broken plugin must not break startup
            log.warning(
                "could not load source plugin",
                extra={"entry_point": entry.name, "error": str(exc)},
            )


def available_sources() -> list[str]:
    _load_entry_points()
    return sorted(_SOURCES)


def build_source(name: str, config: dict[str, object] | None = None) -> Source:
    """Instantiate a registered source with its configuration."""
    _load_entry_points()
    implementation = _SOURCES.get(name)
    if implementation is None:
        raise ConnectorError(f"unknown source: {name}", available=sorted(_SOURCES))
    return implementation(dict(config or {}))


def source_specs() -> list[dict[str, object]]:
    """Specs for every registered source — what the connector picker renders."""
    _load_entry_points()
    return [impl.spec().to_dict() for impl in _SOURCES.values()]
