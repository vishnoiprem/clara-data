"""Apache Iceberg catalog.

This is the production catalog. Iceberg is the choice that makes Clara
non-lock-in: the table format is an open standard, the metadata lives in the
customer's own object storage, and Trino, Spark, DuckDB, Flink and ClickHouse
can all read it. If a customer leaves Clara, their lakehouse still works.

Supports every catalog backend PyIceberg does — REST (Lakekeeper, Polaris,
Nessie, Unity OSS), SQL (Postgres/SQLite), Glue and Hive — selected by
configuration rather than code.
"""

from __future__ import annotations

import re
from typing import Any

from clara.catalog.base import Catalog, TableInfo, TableRef
from clara.catalog.schema import Schema, coerce_record, from_arrow_schema, to_arrow_schema
from clara.errors import CatalogError, ConflictError, NotFoundError, require
from clara.logging_setup import get_logger
from clara.settings import CatalogSettings

log = get_logger(__name__)

#: ``days(event_time)`` / ``bucket(16, user_id)`` / bare ``country``.
_TRANSFORM_RE = re.compile(r"^(?P<fn>\w+)\s*\(\s*(?P<args>[^)]*)\s*\)$")


class IcebergCatalog(Catalog):
    """Clara's ``Catalog`` interface over a PyIceberg catalog."""

    kind = "iceberg"

    def __init__(
        self,
        name: str = "clara",
        *,
        uri: str | None = None,
        warehouse: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self._properties = dict(properties or {})
        if uri:
            self._properties.setdefault("uri", uri)
        if warehouse:
            self._properties.setdefault("warehouse", warehouse)
        self._catalog: Any | None = None

    # ------------------------------------------------------------ construction

    @classmethod
    def from_settings(cls, settings: CatalogSettings, storage_props: dict[str, str] | None = None) -> IcebergCatalog:
        """Build from Clara settings, folding in S3 credentials.

        The ``s3.*`` properties are what let the same code target AWS S3,
        MinIO, Tencent COS or Alibaba OSS — only the endpoint changes.
        """
        props: dict[str, str] = dict(storage_props or {})
        if settings.kind == "rest":
            props["type"] = "rest"
            if settings.token:
                props["token"] = settings.token
        elif settings.kind == "glue":
            props["type"] = "glue"
        else:
            props["type"] = "sql"
        return cls(
            name=settings.name,
            uri=settings.uri,
            warehouse=settings.warehouse,
            properties=props,
        )

    @property
    def catalog(self) -> Any:
        """The underlying PyIceberg catalog, connected on first use."""
        if self._catalog is None:
            module = require("pyiceberg.catalog", "iceberg", "the Iceberg catalog")
            try:
                self._catalog = module.load_catalog(self.name, **self._properties)
            except Exception as exc:  # noqa: BLE001 - surface any backend failure uniformly
                raise CatalogError(
                    f"could not connect to Iceberg catalog {self.name!r}: {exc}",
                    catalog=self.name,
                ) from exc
        return self._catalog

    @staticmethod
    def _identifier(ref: TableRef) -> tuple[str, ...]:
        return (ref.namespace, ref.name)

    # ------------------------------------------------------------- namespaces

    def list_namespaces(self) -> list[str]:
        return sorted(".".join(ns) for ns in self.catalog.list_namespaces())

    def create_namespace(self, namespace: str, exists_ok: bool = True) -> None:
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            self.catalog.create_namespace((namespace,))
        except NamespaceAlreadyExistsError:
            if not exists_ok:
                raise ConflictError(f"namespace already exists: {namespace}") from None

    def drop_namespace(self, namespace: str, cascade: bool = False) -> None:
        from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

        if cascade:
            for ref in self.list_tables(namespace):
                self.drop_table(ref, purge=True)
        try:
            self.catalog.drop_namespace((namespace,))
        except NoSuchNamespaceError:
            raise NotFoundError(f"namespace not found: {namespace}") from None
        except NamespaceNotEmptyError:
            raise ConflictError(
                f"namespace {namespace} is not empty; pass cascade=True"
            ) from None

    # ----------------------------------------------------------------- tables

    def list_tables(self, namespace: str | None = None) -> list[TableRef]:
        namespaces = [namespace] if namespace else self.list_namespaces()
        refs: list[TableRef] = []
        for ns in namespaces:
            for identifier in self.catalog.list_tables((ns,)):
                refs.append(TableRef(".".join(identifier[:-1]) or ns, identifier[-1]))
        return sorted(refs, key=lambda r: r.fqn)

    def table_exists(self, ref: TableRef) -> bool:
        return bool(self.catalog.table_exists(self._identifier(ref)))

    def create_table(
        self,
        ref: TableRef,
        schema: Schema,
        *,
        location: str | None = None,
        properties: dict[str, str] | None = None,
        exists_ok: bool = False,
    ) -> TableInfo:
        from pyiceberg.exceptions import TableAlreadyExistsError

        self.create_namespace(ref.namespace, exists_ok=True)

        table_properties = {
            # Defaults chosen for cheap object storage: zstd compresses far
            # better than snappy, and merge-on-read keeps upserts affordable.
            "write.parquet.compression-codec": "zstd",
            "write.delete.mode": "merge-on-read",
            "write.update.mode": "merge-on-read",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "20",
            **(properties or {}),
        }
        if schema.primary_key:
            table_properties["clara.primary-key"] = ",".join(schema.primary_key)
        if schema.sort_by:
            table_properties["clara.sort-by"] = ",".join(schema.sort_by)

        try:
            table = self.catalog.create_table(
                identifier=self._identifier(ref),
                schema=to_arrow_schema(schema),
                location=location,
                properties=table_properties,
            )
        except TableAlreadyExistsError:
            if not exists_ok:
                raise ConflictError(f"table already exists: {ref.fqn}") from None
            return self.load_table(ref)
        except Exception as exc:  # noqa: BLE001
            raise CatalogError(f"failed to create {ref.fqn}: {exc}", table=ref.fqn) from exc

        if schema.partition_by:
            self._apply_partitioning(table, schema.partition_by)

        log.info(
            "created iceberg table",
            extra={"table": ref.fqn, "columns": len(schema), "partitions": schema.partition_by},
        )
        return self._to_info(ref, self.catalog.load_table(self._identifier(ref)), schema)

    def load_table(self, ref: TableRef) -> TableInfo:
        from pyiceberg.exceptions import NoSuchTableError

        try:
            table = self.catalog.load_table(self._identifier(ref))
        except NoSuchTableError:
            raise NotFoundError(f"table not found: {ref.fqn}") from None
        return self._to_info(ref, table)

    def drop_table(self, ref: TableRef, purge: bool = False) -> None:
        from pyiceberg.exceptions import NoSuchTableError

        try:
            if purge:
                self.catalog.purge_table(self._identifier(ref))
            else:
                self.catalog.drop_table(self._identifier(ref))
        except NoSuchTableError:
            raise NotFoundError(f"table not found: {ref.fqn}") from None

    def evolve_schema(self, ref: TableRef, schema: Schema) -> TableInfo:
        table = self.catalog.load_table(self._identifier(ref))
        current = from_arrow_schema(table.schema().as_arrow())
        additions = [f for f in schema.fields if f.name not in current]
        if not additions:
            return self._to_info(ref, table)

        arrow = to_arrow_schema(Schema(fields=additions))
        with table.update_schema() as update:
            for field, arrow_field in zip(additions, arrow, strict=True):
                # Added columns must be optional: existing rows have no value.
                update.add_column(field.name, arrow_field.type, field.doc, required=False)

        log.info("evolved iceberg schema", extra={"table": ref.fqn, "added": [f.name for f in additions]})
        return self._to_info(ref, self.catalog.load_table(self._identifier(ref)))

    # ------------------------------------------------------------------- data

    def append(self, ref: TableRef, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        pa = require("pyarrow", "duckdb", "Iceberg writes")
        table = self.catalog.load_table(self._identifier(ref))
        schema = from_arrow_schema(table.schema().as_arrow())
        rows = [coerce_record(schema, r) for r in records]
        batch = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
        table.append(batch)
        return len(records)

    def overwrite(self, ref: TableRef, records: list[dict[str, Any]]) -> int:
        """Replace table contents atomically — used by full-refresh syncs."""
        pa = require("pyarrow", "duckdb", "Iceberg writes")
        table = self.catalog.load_table(self._identifier(ref))
        schema = from_arrow_schema(table.schema().as_arrow())
        rows = [coerce_record(schema, r) for r in records]
        batch = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
        table.overwrite(batch)
        return len(records)

    def scan(self, ref: TableRef, limit: int | None = None) -> list[dict[str, Any]]:
        table = self.catalog.load_table(self._identifier(ref))
        scan = table.scan(limit=limit) if limit else table.scan()
        return scan.to_arrow().to_pylist()

    # -------------------------------------------------------- maintenance ops

    def expire_snapshots(self, ref: TableRef, retain_last: int = 5) -> int:
        """Drop old snapshots so metadata and storage do not grow forever.

        Snapshot expiry is the single highest-value maintenance job on a
        lakehouse: unbounded snapshots are the usual cause of surprise storage
        bills. The scheduler runs this nightly per table.
        """
        table = self.catalog.load_table(self._identifier(ref))
        snapshots = list(table.metadata.snapshots)
        if len(snapshots) <= retain_last:
            return 0
        doomed = snapshots[:-retain_last]
        try:
            with table.transaction() as tx:
                tx.expire_snapshots(snapshot_ids=[s.snapshot_id for s in doomed]).commit()
        except AttributeError:  # pragma: no cover - older PyIceberg
            log.warning("snapshot expiry unsupported by this PyIceberg version")
            return 0
        return len(doomed)

    # ---------------------------------------------------------------- helpers

    def _apply_partitioning(self, table: Any, partition_by: list[str]) -> None:
        """Apply Iceberg partition transforms parsed from spec strings."""
        from pyiceberg.transforms import (
            BucketTransform,
            DayTransform,
            HourTransform,
            IdentityTransform,
            MonthTransform,
            TruncateTransform,
            YearTransform,
        )

        simple = {
            "year": YearTransform,
            "years": YearTransform,
            "month": MonthTransform,
            "months": MonthTransform,
            "day": DayTransform,
            "days": DayTransform,
            "hour": HourTransform,
            "hours": HourTransform,
        }

        with table.update_spec() as update:
            for expression in partition_by:
                match = _TRANSFORM_RE.match(expression.strip())
                if match is None:
                    update.add_field(expression.strip(), IdentityTransform())
                    continue

                fn = match.group("fn").lower()
                args = [a.strip() for a in match.group("args").split(",") if a.strip()]
                if fn in simple:
                    update.add_field(args[0], simple[fn]())
                elif fn == "bucket":
                    count, column = (args[0], args[1]) if args[0].isdigit() else (args[1], args[0])
                    update.add_field(column, BucketTransform(int(count)))
                elif fn == "truncate":
                    width, column = (args[0], args[1]) if args[0].isdigit() else (args[1], args[0])
                    update.add_field(column, TruncateTransform(int(width)))
                elif fn == "identity":
                    update.add_field(args[0], IdentityTransform())
                else:
                    raise CatalogError(f"unsupported partition transform: {expression}")

    def _to_info(self, ref: TableRef, table: Any, schema: Schema | None = None) -> TableInfo:
        # Explicit None check: Schema defines __len__, so an empty one is falsy.
        resolved = schema if schema is not None else from_arrow_schema(table.schema().as_arrow())
        properties = dict(table.properties or {})
        if "clara.primary-key" in properties and not resolved.primary_key:
            resolved.primary_key = properties["clara.primary-key"].split(",")

        snapshot = table.current_snapshot()
        row_count: int | None = None
        size_bytes: int | None = None
        if snapshot is not None and snapshot.summary is not None:
            summary = snapshot.summary
            row_count = _int_or_none(summary.get("total-records"))
            size_bytes = _int_or_none(summary.get("total-files-size"))

        return TableInfo(
            ref=ref,
            schema=resolved,
            location=table.location(),
            format="iceberg",
            properties=properties,
            row_count=row_count,
            size_bytes=size_bytes,
            snapshot_id=snapshot.snapshot_id if snapshot else None,
        )


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
