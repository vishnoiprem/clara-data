"""Local catalog — the zero-dependency implementation.

Backs ``clara init`` quickstarts, CI and the test suite: no object store, no
catalog service, no network. Metadata is a JSON sidecar per table; data is
Parquet when PyArrow is installed and newline-delimited JSON otherwise. Both
formats are readable directly by the DuckDB engine, so a local table behaves
like a real one all the way up the stack.

Not intended for production — there is no concurrency control beyond a process
lock, and no snapshot isolation. That is what ``IcebergCatalog`` is for.
"""

from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path
from typing import Any

from clara.catalog.base import Catalog, TableInfo, TableRef
from clara.catalog.schema import Schema, coerce_record
from clara.errors import CatalogError, ConflictError, NotFoundError
from clara.ids import ulid
from clara.logging_setup import get_logger
from clara.time_utils import utcnow

log = get_logger(__name__)

_META_FILE = "_clara_table.json"


class LocalCatalog(Catalog):
    """Filesystem-backed catalog. Pass ``root=None`` for a pure in-memory catalog."""

    kind = "local"

    def __init__(self, root: str | Path | None = ".clara/catalog") -> None:
        self.root: Path | None = Path(root).expanduser().resolve() if root else None
        self._lock = threading.RLock()
        # In-memory mode keeps everything here; filesystem mode uses it as a
        # metadata cache only.
        self._tables: dict[str, TableInfo] = {}
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._namespaces: set[str] = set()

        if self.root is not None:
            self.root.mkdir(parents=True, exist_ok=True)
            self._load_from_disk()

    # ------------------------------------------------------------ persistence

    @property
    def _in_memory(self) -> bool:
        return self.root is None

    def _table_dir(self, ref: TableRef) -> Path:
        assert self.root is not None
        return self.root / ref.namespace / ref.name

    def _load_from_disk(self) -> None:
        assert self.root is not None
        for meta_path in self.root.glob(f"*/*/{_META_FILE}"):
            try:
                payload = json.loads(meta_path.read_text())
            except (OSError, json.JSONDecodeError) as exc:  # pragma: no cover
                log.warning("skipping unreadable table metadata", extra={"path": str(meta_path)})
                del exc
                continue
            ref = TableRef(payload["namespace"], payload["name"])
            self._namespaces.add(ref.namespace)
            self._tables[ref.fqn] = TableInfo(
                ref=ref,
                schema=Schema.from_dict(payload["schema"]),
                location=payload.get("location"),
                format=payload.get("format", "local"),
                properties=payload.get("properties", {}),
            )
        # Namespaces can exist with no tables in them.
        for child in self.root.iterdir():
            if child.is_dir():
                self._namespaces.add(child.name)

    def _write_meta(self, info: TableInfo) -> None:
        if self._in_memory:
            return
        target = self._table_dir(info.ref)
        target.mkdir(parents=True, exist_ok=True)
        payload = {
            "namespace": info.ref.namespace,
            "name": info.ref.name,
            "format": info.format,
            "location": info.location,
            "schema": info.schema.to_dict(),
            "properties": info.properties,
            "updated_at": utcnow().isoformat(),
        }
        (target / _META_FILE).write_text(json.dumps(payload, indent=2))

    # ------------------------------------------------------------- namespaces

    def list_namespaces(self) -> list[str]:
        with self._lock:
            return sorted(self._namespaces)

    def create_namespace(self, namespace: str, exists_ok: bool = True) -> None:
        with self._lock:
            if namespace in self._namespaces:
                if not exists_ok:
                    raise ConflictError(f"namespace already exists: {namespace}")
                return
            self._namespaces.add(namespace)
            if not self._in_memory:
                assert self.root is not None
                (self.root / namespace).mkdir(parents=True, exist_ok=True)

    def drop_namespace(self, namespace: str, cascade: bool = False) -> None:
        with self._lock:
            if namespace not in self._namespaces:
                raise NotFoundError(f"namespace not found: {namespace}")
            contained = [r for r in self._iter_refs() if r.namespace == namespace]
            if contained and not cascade:
                raise ConflictError(
                    f"namespace {namespace} is not empty; pass cascade=True",
                    tables=[r.name for r in contained],
                )
            for ref in contained:
                self.drop_table(ref, purge=True)
            self._namespaces.discard(namespace)
            if not self._in_memory:
                assert self.root is not None
                shutil.rmtree(self.root / namespace, ignore_errors=True)

    # ----------------------------------------------------------------- tables

    def _iter_refs(self) -> list[TableRef]:
        return [info.ref for info in self._tables.values()]

    def list_tables(self, namespace: str | None = None) -> list[TableRef]:
        with self._lock:
            refs = self._iter_refs()
            if namespace is not None:
                refs = [r for r in refs if r.namespace == namespace]
            return sorted(refs, key=lambda r: r.fqn)

    def table_exists(self, ref: TableRef) -> bool:
        with self._lock:
            return ref.fqn in self._tables

    def create_table(
        self,
        ref: TableRef,
        schema: Schema,
        *,
        location: str | None = None,
        properties: dict[str, str] | None = None,
        exists_ok: bool = False,
    ) -> TableInfo:
        with self._lock:
            if ref.fqn in self._tables:
                if not exists_ok:
                    raise ConflictError(f"table already exists: {ref.fqn}")
                return self._tables[ref.fqn]
            if not schema.fields:
                raise CatalogError(f"cannot create table {ref.fqn} with no columns")

            self.create_namespace(ref.namespace, exists_ok=True)
            resolved_location = location
            if resolved_location is None and not self._in_memory:
                resolved_location = str(self._table_dir(ref))

            info = TableInfo(
                ref=ref,
                schema=schema,
                location=resolved_location,
                format="local",
                properties=dict(properties or {}),
                created_at=utcnow(),
                updated_at=utcnow(),
            )
            self._tables[ref.fqn] = info
            self._rows.setdefault(ref.fqn, [])
            self._write_meta(info)
            log.info("created table", extra={"table": ref.fqn, "columns": len(schema)})
            return info

    def create_view(self, ref: TableRef, sql: str, schema: Schema) -> TableInfo:
        """Register a view: SQL plus its result schema, no data files.

        Views are first-class here so that ``materialization: view`` means the
        same thing on the local path as it does on Iceberg — a stored query, not
        a silently materialised table.
        """
        with self._lock:
            self.create_namespace(ref.namespace, exists_ok=True)
            info = TableInfo(
                ref=ref,
                schema=schema,
                location=None,
                format="view",
                properties={"clara.view-sql": sql},
                created_at=utcnow(),
                updated_at=utcnow(),
            )
            self._tables[ref.fqn] = info
            self._rows.pop(ref.fqn, None)
            self._write_meta(info)
            log.info("created view", extra={"view": ref.fqn})
            return info

    def is_view(self, ref: TableRef) -> bool:
        info = self._tables.get(ref.fqn)
        return info is not None and info.format == "view"

    def load_table(self, ref: TableRef) -> TableInfo:
        with self._lock:
            info = self._tables.get(ref.fqn)
            if info is None:
                raise NotFoundError(f"table not found: {ref.fqn}")
            if info.format == "view":
                # A view has no files; its statistics belong to whatever it reads.
                return info
            # Statistics are derived, so refresh them on read.
            info.row_count = self._row_count(ref)
            info.size_bytes = self._size_bytes(ref)
            return info

    def drop_table(self, ref: TableRef, purge: bool = False) -> None:
        with self._lock:
            if ref.fqn not in self._tables:
                raise NotFoundError(f"table not found: {ref.fqn}")
            del self._tables[ref.fqn]
            self._rows.pop(ref.fqn, None)
            if purge and not self._in_memory:
                shutil.rmtree(self._table_dir(ref), ignore_errors=True)

    def evolve_schema(self, ref: TableRef, schema: Schema) -> TableInfo:
        with self._lock:
            info = self._tables.get(ref.fqn)
            if info is None:
                raise NotFoundError(f"table not found: {ref.fqn}")
            new_columns = [f for f in schema.fields if f.name not in info.schema]
            if new_columns:
                info.schema = info.schema.with_fields(*new_columns)
                info.updated_at = utcnow()
                self._write_meta(info)
                log.info(
                    "evolved schema",
                    extra={"table": ref.fqn, "added": [f.name for f in new_columns]},
                )
            return info

    # ------------------------------------------------------------------- data

    def append(self, ref: TableRef, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        with self._lock:
            info = self._tables.get(ref.fqn)
            if info is None:
                raise NotFoundError(f"table not found: {ref.fqn}")

            # Project every record onto the table schema so files stay uniform
            # even when a source emits sparse or extra keys.
            projected = [self._project(info.schema, r) for r in records]

            if self._in_memory:
                self._rows.setdefault(ref.fqn, []).extend(projected)
            else:
                self._write_data_file(info, projected)
            info.updated_at = utcnow()
            return len(projected)

    def upsert(self, ref: TableRef, records: list[dict[str, Any]], keys: list[str]) -> int:
        """Insert-or-replace records by key, returning the number applied.

        There is no MERGE over plain files, so the table is rewritten without
        the incoming keys and then reloaded. Expensive, but it makes an
        incremental sync *idempotent* — re-running a pipeline cannot duplicate
        rows, which matters far more than write throughput on the local path.
        """
        if not records:
            return 0
        if not keys:
            return self.append(ref, records)

        with self._lock:
            info = self._tables.get(ref.fqn)
            if info is None:
                raise NotFoundError(f"table not found: {ref.fqn}")

            def key_of(record: dict[str, Any]) -> tuple:
                return tuple(record.get(k) for k in keys)

            projected = [self._project(info.schema, r) for r in records]
            # Last write wins within the incoming batch.
            incoming = {key_of(r): r for r in projected}
            retained = [
                r for r in self._read_all(ref) if key_of(r) not in incoming
            ]
            merged = [*retained, *incoming.values()]

            schema = info.schema
            self._truncate(ref, schema)
            self.append(ref, merged)
            return len(incoming)

    def _read_all(self, ref: TableRef) -> list[dict[str, Any]]:
        return self._rows.get(ref.fqn, []) if self._in_memory else self._read_data_files(ref)

    def _truncate(self, ref: TableRef, schema: Schema) -> None:
        """Drop a table's data while keeping its metadata."""
        if self._in_memory:
            self._rows[ref.fqn] = []
            return
        for path in self.data_files(ref):
            path.unlink(missing_ok=True)

    def scan(self, ref: TableRef, limit: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if ref.fqn not in self._tables:
                raise NotFoundError(f"table not found: {ref.fqn}")
            rows = self._rows[ref.fqn] if self._in_memory else self._read_data_files(ref)
            return rows[:limit] if limit is not None else list(rows)

    @staticmethod
    def _project(schema: Schema, record: dict[str, Any]) -> dict[str, Any]:
        return coerce_record(schema, record)

    # --------------------------------------------------- file format handling

    @staticmethod
    def _arrow() -> Any | None:
        try:
            import pyarrow  # noqa: F401

            return pyarrow
        except ImportError:  # pragma: no cover - depends on install shape
            return None

    def _write_data_file(self, info: TableInfo, records: list[dict[str, Any]]) -> None:
        target = self._table_dir(info.ref)
        target.mkdir(parents=True, exist_ok=True)
        pa = self._arrow()
        if pa is not None:
            import pyarrow.parquet as pq

            from clara.catalog.schema import to_arrow_schema

            # cast_to=True lets Arrow coerce Python values (str dates, ints for
            # longs) into the declared schema rather than failing the write.
            table = pa.Table.from_pylist(records, schema=to_arrow_schema(info.schema))
            pq.write_table(table, target / f"data-{ulid()}.parquet", compression="zstd")
        else:
            with (target / f"data-{ulid()}.jsonl").open("w") as handle:
                for record in records:
                    handle.write(json.dumps(record, default=str) + "\n")

    def data_files(self, ref: TableRef) -> list[Path]:
        """Data files backing a table, oldest first. Read by the DuckDB engine."""
        target = self._table_dir(ref)
        if not target.exists():
            return []
        return sorted([*target.glob("data-*.parquet"), *target.glob("data-*.jsonl")])

    def _read_data_files(self, ref: TableRef) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for path in self.data_files(ref):
            if path.suffix == ".parquet":
                import pyarrow.parquet as pq

                rows.extend(pq.read_table(path).to_pylist())
            else:
                with path.open() as handle:
                    rows.extend(json.loads(line) for line in handle if line.strip())
        return rows

    def _row_count(self, ref: TableRef) -> int:
        if self._in_memory:
            return len(self._rows.get(ref.fqn, []))
        total = 0
        for path in self.data_files(ref):
            if path.suffix == ".parquet":
                import pyarrow.parquet as pq

                total += pq.ParquetFile(path).metadata.num_rows
            else:
                with path.open() as handle:
                    total += sum(1 for line in handle if line.strip())
        return total

    def _size_bytes(self, ref: TableRef) -> int:
        if self._in_memory:
            return sum(len(json.dumps(r, default=str)) for r in self._rows.get(ref.fqn, []))
        return sum(p.stat().st_size for p in self.data_files(ref))

    # ---------------------------------------------------------------- engines

    def data_glob(self, ref: TableRef) -> str:
        """Glob pattern the DuckDB engine reads this table through."""
        if self._in_memory:
            raise CatalogError("in-memory catalog has no queryable files")
        files = self.data_files(ref)
        suffix = "parquet" if any(p.suffix == ".parquet" for p in files) else "jsonl"
        return str(self._table_dir(ref) / f"data-*.{suffix}")