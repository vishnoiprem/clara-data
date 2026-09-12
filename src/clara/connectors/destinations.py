"""Destinations.

There is really only one destination that matters — the lakehouse — because the
whole point of an open table format is that everything else reads from it
directly rather than needing another copy.
"""

from __future__ import annotations

import json
from typing import Any

from clara.catalog.base import Catalog, TableRef
from clara.catalog.schema import Schema
from clara.connectors.base import Destination, WriteResult
from clara.connectors.protocol import ConfiguredStream, WriteMode
from clara.engines.base import Engine
from clara.errors import ConnectorError
from clara.ids import slugify
from clara.logging_setup import get_logger

log = get_logger(__name__)


class LakehouseDestination(Destination):
    """Writes into Iceberg (or the local catalog) tables.

    Schema handling is additive: a new column in the source becomes a new
    nullable column in the table. Sources gain columns constantly, and failing a
    3 a.m. sync over a harmless new field is the kind of fragility that forces
    companies to keep a data engineer on call.
    """

    name = "lakehouse"

    def __init__(
        self,
        catalog: Catalog,
        *,
        engine: Engine | None = None,
        schema_inference_sample: int = 500,
    ) -> None:
        self.catalog = catalog
        #: Needed only for MERGE writes, which require SQL.
        self.engine = engine
        self.schema_inference_sample = schema_inference_sample
        self._prepared: dict[str, Schema] = {}
        self._overwritten: set[str] = set()

    # ---------------------------------------------------------------- lifecycle

    def _ref(self, stream: ConfiguredStream, namespace: str) -> TableRef:
        return TableRef(namespace, slugify(stream.target_table))

    def begin_sync(self) -> None:
        """Reset per-sync state.

        ``_overwritten`` tracks which tables a *single* sync has already
        replaced, so later batches append instead of clobbering earlier ones. It
        must be cleared between syncs, or a second full-refresh run on a reused
        destination would append and double the table.
        """
        self._overwritten.clear()

    def prepare(self, stream: ConfiguredStream, namespace: str) -> None:
        """Create the table from the stream's declared schema, if it has one."""
        ref = self._ref(stream, namespace)
        schema = stream.stream.to_schema()
        if not schema.fields:
            # Schemaless source (CSV, loose JSON API): defer until the first
            # batch arrives and infer from the data.
            return
        schema.primary_key = stream.primary_key or schema.primary_key
        info = self.catalog.ensure_table(ref, schema)
        self._prepared[ref.fqn] = info.schema

    def write(
        self,
        stream: ConfiguredStream,
        namespace: str,
        records: list[dict[str, Any]],
    ) -> WriteResult:
        if not records:
            return WriteResult(table=self._ref(stream, namespace).fqn)

        ref = self._ref(stream, namespace)
        schema_changed = self._reconcile_schema(ref, stream, records)

        if stream.write_mode is WriteMode.OVERWRITE:
            written = self._write_overwrite(ref, records)
        elif stream.write_mode is WriteMode.MERGE:
            written = self._write_merge(ref, stream, records)
        else:
            written = self.catalog.append(ref, records)

        return WriteResult(
            records_written=written,
            bytes_written=_estimate_bytes(records),
            table=ref.fqn,
            schema_changed=schema_changed,
        )

    def finalize(self, stream: ConfiguredStream, namespace: str) -> None:
        """Compact after a large write, where the engine supports it."""
        ref = self._ref(stream, namespace)
        if self.engine is None or not hasattr(self.engine, "optimize"):
            return
        try:
            self.engine.optimize(ref)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001 - compaction is best-effort
            log.debug("post-sync compaction skipped", extra={"table": ref.fqn, "error": str(exc)})

    # ------------------------------------------------------------------ schema

    def _reconcile_schema(
        self, ref: TableRef, stream: ConfiguredStream, records: list[dict[str, Any]]
    ) -> bool:
        """Create the table, or additively widen it to fit these records."""
        declared = stream.stream.to_schema()
        observed = Schema.infer(records, sample=self.schema_inference_sample)

        # Prefer declared types where a column appears in both: the source knows
        # its own types better than inference does.
        merged = declared if declared.fields else observed
        if declared.fields:
            merged = declared.with_fields(
                *[f for f in observed.fields if f.name not in declared]
            )
        merged.primary_key = stream.primary_key or merged.primary_key

        known = self._prepared.get(ref.fqn)
        if known is None:
            info = self.catalog.ensure_table(ref, merged)
            self._prepared[ref.fqn] = info.schema
            return True

        new_columns = [f for f in merged.fields if f.name not in known]
        if not new_columns:
            return False

        log.info(
            "source added columns; evolving table",
            extra={"table": ref.fqn, "columns": [f.name for f in new_columns]},
        )
        info = self.catalog.evolve_schema(ref, merged)
        self._prepared[ref.fqn] = info.schema
        return True

    # ------------------------------------------------------------ write modes

    def _write_overwrite(self, ref: TableRef, records: list[dict[str, Any]]) -> int:
        """Replace table contents — but only once per sync.

        A full-refresh sync arrives as many batches; overwriting on each would
        leave only the last. The first batch replaces, the rest append.
        """
        if ref.fqn in self._overwritten:
            return self.catalog.append(ref, records)

        self._overwritten.add(ref.fqn)
        overwrite = getattr(self.catalog, "overwrite", None)
        if callable(overwrite):
            return int(overwrite(ref, records))

        # Catalogs without atomic overwrite: drop and recreate.
        schema = self._prepared[ref.fqn]
        self.catalog.drop_table(ref, purge=True)
        self.catalog.create_table(ref, schema)
        return self.catalog.append(ref, records)

    def _write_merge(
        self, ref: TableRef, stream: ConfiguredStream, records: list[dict[str, Any]]
    ) -> int:
        """Upsert on the primary key.

        Preference order: a catalog that can upsert natively, then a SQL MERGE
        on the engine, then append. The first two are idempotent; the last is
        not, so it warns.
        """
        upsert = getattr(self.catalog, "upsert", None)
        if callable(upsert) and stream.primary_key:
            return int(upsert(ref, records, stream.primary_key))

        if self.engine is None or not self.engine.capabilities.supports_merge:
            log.warning(
                "engine cannot MERGE; appending instead",
                extra={"table": ref.fqn, "engine": getattr(self.engine, "name", None)},
            )
            return self.catalog.append(ref, records)

        merge = getattr(self.engine, "merge", None)
        if not callable(merge):
            # DuckDB reports MERGE support for its own tables, but cannot merge
            # into views over object-storage files. Say so rather than silently
            # appending, which would duplicate rows on every incremental run.
            log.warning(
                "engine has no MERGE implementation for lakehouse tables; appending instead "
                "(incremental runs may duplicate rows — use Trino for merge semantics)",
                extra={"table": ref.fqn, "engine": self.engine.name},
            )
            return self.catalog.append(ref, records)

        schema = self._prepared[ref.fqn]
        staging = TableRef(ref.namespace, f"_clara_stage_{ref.name}")
        self.catalog.create_table(staging, schema, exists_ok=True)
        try:
            self.catalog.append(staging, records)
            merge(
                ref,
                f"SELECT * FROM {self.engine.quote_ref(staging)}",
                stream.primary_key,
                schema.names,
            )
        finally:
            self.catalog.drop_table(staging, purge=True)
        return len(records)

    def close(self) -> None:
        self._prepared.clear()
        self._overwritten.clear()


class ConsoleDestination(Destination):
    """Prints records instead of storing them. Used by ``clara pipeline test``
    so a user can see what a source returns before committing to a table."""

    name = "console"

    def __init__(self, limit: int = 10) -> None:
        self.limit = limit
        self._shown = 0

    def prepare(self, stream: ConfiguredStream, namespace: str) -> None:
        print(f"-- stream {stream.target_table} ({stream.sync_mode.value}) --")

    def write(
        self, stream: ConfiguredStream, namespace: str, records: list[dict[str, Any]]
    ) -> WriteResult:
        for record in records:
            if self._shown >= self.limit:
                break
            print(json.dumps(record, default=str)[:400])
            self._shown += 1
        return WriteResult(
            records_written=len(records),
            bytes_written=_estimate_bytes(records),
            table=stream.target_table,
        )


def _estimate_bytes(records: list[dict[str, Any]]) -> int:
    """Approximate serialised size of a batch.

    Used for ingest metering. Sampling rather than measuring every record keeps
    the hot loop cheap; the error is well under the billing rounding.
    """
    if not records:
        return 0
    sample = records[: min(20, len(records))]
    total = sum(len(json.dumps(r, default=str)) for r in sample)
    return int(total / len(sample) * len(records))


def build_destination(
    kind: str = "lakehouse",
    *,
    catalog: Catalog | None = None,
    engine: Engine | None = None,
    **kwargs: Any,
) -> Destination:
    """Construct a destination by name."""
    if kind == "lakehouse":
        if catalog is None:
            raise ConnectorError("lakehouse destination requires a catalog")
        return LakehouseDestination(catalog, engine=engine)
    if kind == "console":
        return ConsoleDestination(**kwargs)
    raise ConnectorError(f"unknown destination: {kind}")
