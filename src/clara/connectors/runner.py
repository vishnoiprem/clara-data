"""The sync runner.

Drives a source into a destination: batches records, applies backpressure,
checkpoints state and meters the volume moved. This is the one place that
understands the at-least-once contract — state is committed only after the
records it covers have been written, so a crash replays rows rather than losing
them.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from clara.connectors.base import Destination, Source
from clara.connectors.protocol import (
    AirbyteMessage,
    ConfiguredStream,
    MessageType,
    SyncMode,
    WriteMode,
)
from clara.errors import ConnectorError
from clara.logging_setup import get_logger
from clara.time_utils import format_duration, to_iso, utcnow

log = get_logger(__name__)

#: Records buffered per destination write. Tuned so a Parquet file lands in the
#: tens of megabytes — small enough to bound memory, large enough to avoid the
#: small-file problem that degrades lakehouse query performance.
DEFAULT_BATCH_SIZE = 10_000


@dataclass
class StreamResult:
    """Per-stream outcome."""

    stream: str
    table: str | None = None
    records: int = 0
    bytes_moved: int = 0
    batches: int = 0
    schema_changed: bool = False
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "table": self.table,
            "records": self.records,
            "bytes_moved": self.bytes_moved,
            "batches": self.batches,
            "schema_changed": self.schema_changed,
            "succeeded": self.succeeded,
            "error": self.error,
        }


@dataclass
class SyncResult:
    """The outcome of one sync."""

    streams: list[StreamResult] = field(default_factory=list)
    state: dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0
    logs: list[str] = field(default_factory=list)
    started_at: Any = None
    finished_at: Any = None

    @property
    def records(self) -> int:
        return sum(s.records for s in self.streams)

    @property
    def bytes_moved(self) -> int:
        return sum(s.bytes_moved for s in self.streams)

    @property
    def succeeded(self) -> bool:
        return bool(self.streams) and all(s.succeeded for s in self.streams)

    @property
    def records_per_second(self) -> float:
        return self.records / self.duration_seconds if self.duration_seconds > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "records": self.records,
            "bytes_moved": self.bytes_moved,
            "duration_seconds": round(self.duration_seconds, 3),
            "duration": format_duration(self.duration_seconds),
            "records_per_second": round(self.records_per_second, 1),
            "started_at": to_iso(self.started_at) if self.started_at else None,
            "finished_at": to_iso(self.finished_at) if self.finished_at else None,
            "streams": [s.to_dict() for s in self.streams],
            "state": self.state,
            "logs": self.logs[-50:],
        }


class SyncRunner:
    """Executes a source-to-destination sync."""

    def __init__(
        self,
        source: Source,
        destination: Destination,
        *,
        namespace: str = "raw",
        batch_size: int = DEFAULT_BATCH_SIZE,
        meter: Any | None = None,
        pipeline_id: str | None = None,
        max_records: int | None = None,
    ) -> None:
        self.source = source
        self.destination = destination
        self.namespace = namespace
        self.batch_size = batch_size
        self.meter = meter
        self.pipeline_id = pipeline_id
        #: Safety valve for previews and trial plans.
        self.max_records = max_records

    def run(
        self,
        streams: list[ConfiguredStream] | None = None,
        state: dict[str, Any] | None = None,
    ) -> SyncResult:
        """Sync the configured streams and return the outcome.

        A failure in one stream does not abort the others: partial progress on
        an independent table is worth keeping, and the per-stream error is
        reported rather than swallowed.
        """
        configured = streams if streams is not None else self.source.configured_streams()
        if not configured:
            raise ConnectorError(f"{self.source.name}: no streams selected")

        result = SyncResult(state=dict(state or {}), started_at=utcnow())
        started = time.perf_counter()
        # Destinations are reused across runs (the executor holds one), so any
        # per-sync bookkeeping must be reset here rather than in __init__.
        self.destination.begin_sync()

        by_name = {c.stream.name: c for c in configured}
        results: dict[str, StreamResult] = {
            name: StreamResult(stream=name, table=c.target_table) for name, c in by_name.items()
        }
        buffers: dict[str, list[dict[str, Any]]] = {name: [] for name in by_name}

        for configured_stream in configured:
            try:
                self.destination.prepare(configured_stream, self.namespace)
            except Exception as exc:  # noqa: BLE001
                results[configured_stream.stream.name].error = f"prepare failed: {exc}"
                log.warning(
                    "destination prepare failed",
                    extra={"stream": configured_stream.stream.name, "error": str(exc)},
                )

        pending_state: dict[str, Any] | None = None
        total_records = 0

        try:
            for message in self.source.read(configured, result.state):
                if message.type is MessageType.RECORD and message.record is not None:
                    name = message.record.stream
                    if name not in buffers:
                        continue  # source emitted an unselected stream
                    buffers[name].append(message.record.data)
                    total_records += 1

                    if len(buffers[name]) >= self.batch_size:
                        self._flush(by_name[name], buffers[name], results[name])
                        buffers[name] = []

                    if self.max_records is not None and total_records >= self.max_records:
                        result.logs.append(f"stopped at max_records={self.max_records}")
                        break

                elif message.type is MessageType.STATE and message.state is not None:
                    # Hold the checkpoint until the records it covers are
                    # written. Committing it now would lose rows still buffered.
                    pending_state = dict(message.state.data)

                elif message.type is MessageType.LOG and message.log:
                    text = f"[{message.log.get('level')}] {message.log.get('message')}"
                    result.logs.append(text)
                    log.info("connector log", extra={"source": self.source.name, "detail": text})

                elif message.type is MessageType.TRACE and message.trace:
                    result.logs.append(f"[TRACE] {message.trace}")

        except Exception as exc:  # noqa: BLE001 - report, do not crash the scheduler
            log.exception("sync failed", extra={"source": self.source.name})
            for name in buffers:
                if results[name].error is None:
                    results[name].error = str(exc)

        # Drain whatever is left, then commit the checkpoint.
        for name, buffered in buffers.items():
            if buffered:
                self._flush(by_name[name], buffered, results[name])

        if pending_state is not None:
            result.state.update(pending_state)

        for configured_stream in configured:
            name = configured_stream.stream.name
            if results[name].succeeded and results[name].records:
                try:
                    self.destination.finalize(configured_stream, self.namespace)
                except Exception as exc:  # noqa: BLE001 - finalize is best-effort
                    log.debug("finalize failed", extra={"stream": name, "error": str(exc)})

        result.streams = list(results.values())
        result.duration_seconds = time.perf_counter() - started
        result.finished_at = utcnow()

        self._meter(result)
        log.info(
            "sync complete",
            extra={
                "source": self.source.name,
                "records": result.records,
                "duration": format_duration(result.duration_seconds),
                "succeeded": result.succeeded,
            },
        )
        return result

    # ---------------------------------------------------------------- internals

    def _flush(
        self, stream: ConfiguredStream, records: list[dict[str, Any]], into: StreamResult
    ) -> None:
        if not records or into.error is not None:
            return
        try:
            write = self.destination.write(stream, self.namespace, records)
        except Exception as exc:  # noqa: BLE001
            into.error = str(exc)
            log.warning(
                "destination write failed",
                extra={"stream": stream.stream.name, "error": str(exc)},
            )
            return
        into.records += write.records_written
        into.bytes_moved += write.bytes_written
        into.batches += 1
        into.schema_changed = into.schema_changed or write.schema_changed

    def _meter(self, result: SyncResult) -> None:
        """Record ingest volume, if a meter was supplied."""
        if self.meter is None or result.records == 0:
            return
        self.meter.record_sync(
            bytes_moved=result.bytes_moved,
            rows=result.records,
            connector=self.source.name,
            pipeline_id=self.pipeline_id,
        )
        self.meter.record_task(
            seconds=result.duration_seconds,
            task_name=f"sync:{self.source.name}",
            run_id=self.pipeline_id,
        )

    # ---------------------------------------------------------------- previews

    def preview(self, stream_name: str, limit: int = 20) -> list[dict[str, Any]]:
        """Read a handful of records without writing anything.

        Powers "show me what this connector returns" in the UI, which is how a
        non-technical user gains confidence before creating a pipeline.
        """
        descriptor = self.source.stream_by_name(stream_name)
        configured = ConfiguredStream(stream=descriptor, sync_mode=SyncMode.FULL_REFRESH)
        rows: list[dict[str, Any]] = []
        for message in self.source.read([configured], None):
            if message.type is MessageType.RECORD and message.record is not None:
                rows.append(message.record.data)
                if len(rows) >= limit:
                    break
        return rows


def configure_streams(
    source: Source,
    selections: list[dict[str, Any]],
) -> list[ConfiguredStream]:
    """Build configured streams from declarative selections.

    Bridges ``clara.yaml`` and the API to the protocol objects, so a user
    writes ``{name: orders, sync_mode: incremental}`` and never sees a
    ``StreamDescriptor``.
    """
    discovered = {s.name: s for s in source.discover()}
    configured: list[ConfiguredStream] = []

    for selection in selections:
        name = selection.get("name") or selection.get("stream")
        if name not in discovered:
            raise ConnectorError(
                f"{source.name}: stream {name!r} not found",
                available=sorted(discovered),
            )
        descriptor = discovered[name]
        sync_mode = SyncMode(str(selection.get("sync_mode", "full_refresh")).lower())
        if sync_mode is SyncMode.INCREMENTAL and not descriptor.supports_incremental:
            log.warning(
                "stream does not support incremental sync; using full refresh",
                extra={"stream": name},
            )
            sync_mode = SyncMode.FULL_REFRESH

        # Default write mode follows the sync mode: an incremental stream with a
        # primary key should merge, otherwise a full refresh should replace.
        declared_write = selection.get("write_mode")
        if declared_write:
            write_mode = WriteMode(str(declared_write).lower())
        elif sync_mode is SyncMode.INCREMENTAL:
            write_mode = WriteMode.MERGE
        else:
            write_mode = WriteMode.OVERWRITE

        configured.append(
            ConfiguredStream(
                stream=descriptor,
                sync_mode=sync_mode,
                write_mode=write_mode,
                cursor_field=_as_list(selection.get("cursor_field")),
                primary_key=_as_list(selection.get("primary_key")),
                destination_table=selection.get("table") or selection.get("destination_table"),
            )
        )
    return configured


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)
