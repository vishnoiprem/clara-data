"""Portable table schemas.

Clara sits between several type systems — Iceberg, Arrow, Trino SQL, DuckDB SQL,
JSON from connectors. Rather than convert N×N, everything converts through this
one neutral representation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DataType(str, Enum):
    """The neutral type set. Intentionally small — these are the types that
    survive a round trip through every engine Clara supports."""

    BOOLEAN = "boolean"
    INT = "int"  # 32-bit
    LONG = "long"  # 64-bit
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    STRING = "string"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"  # without timezone
    TIMESTAMPTZ = "timestamptz"  # with timezone
    BINARY = "binary"
    JSON = "json"  # stored as string; engines vary too much to do better

    @classmethod
    def parse(cls, value: str | DataType) -> DataType:
        if isinstance(value, DataType):
            return value
        text = str(value).strip().lower()
        aliases = {
            "bool": cls.BOOLEAN,
            "integer": cls.INT,
            "int32": cls.INT,
            "smallint": cls.INT,
            "int64": cls.LONG,
            "bigint": cls.LONG,
            "float32": cls.FLOAT,
            "real": cls.FLOAT,
            "float64": cls.DOUBLE,
            "numeric": cls.DECIMAL,
            "varchar": cls.STRING,
            "text": cls.STRING,
            "str": cls.STRING,
            "uuid": cls.STRING,
            "datetime": cls.TIMESTAMP,
            "timestamp_ntz": cls.TIMESTAMP,
            "timestamp with time zone": cls.TIMESTAMPTZ,
            "timestamptz": cls.TIMESTAMPTZ,
            "bytes": cls.BINARY,
            "jsonb": cls.JSON,
            "object": cls.JSON,
            "array": cls.JSON,
        }
        if text in aliases:
            return aliases[text]
        return cls(text)


@dataclass(frozen=True)
class Field_:
    """One column."""

    name: str
    type: DataType = DataType.STRING
    nullable: bool = True
    doc: str | None = None
    #: Only meaningful for DECIMAL.
    precision: int = 38
    scale: int = 9

    def sql_type(self, dialect: str = "trino") -> str:
        return sql_type(self.type, dialect, self.precision, self.scale)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "type": self.type.value,
            "nullable": self.nullable,
        }
        if self.doc:
            payload["doc"] = self.doc
        if self.type is DataType.DECIMAL:
            payload["precision"] = self.precision
            payload["scale"] = self.scale
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Field_:
        return cls(
            name=payload["name"],
            type=DataType.parse(payload.get("type", "string")),
            nullable=bool(payload.get("nullable", True)),
            doc=payload.get("doc"),
            precision=int(payload.get("precision", 38)),
            scale=int(payload.get("scale", 9)),
        )


@dataclass
class Schema:
    """An ordered set of columns, plus the table-level physical layout hints
    (partitioning, sort order) that Iceberg needs."""

    fields: list[Field_] = field(default_factory=list)
    #: Column names to partition by. Supports Iceberg transforms as
    #: ``"days(event_time)"`` or ``"bucket(16, user_id)"``.
    partition_by: list[str] = field(default_factory=list)
    #: Natural key used by merge/upsert writes.
    primary_key: list[str] = field(default_factory=list)
    sort_by: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for f in self.fields:
            key = f.name.lower()
            if key in seen:
                raise ValueError(f"duplicate column in schema: {f.name}")
            seen.add(key)

    # ----------------------------------------------------------------- lookups

    @property
    def names(self) -> list[str]:
        return [f.name for f in self.fields]

    def get(self, name: str) -> Field_ | None:
        lowered = name.lower()
        return next((f for f in self.fields if f.name.lower() == lowered), None)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and self.get(name) is not None

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self):
        return iter(self.fields)

    # ------------------------------------------------------------- mutation

    def with_fields(self, *fields: Field_) -> Schema:
        """Return a copy with extra columns appended (schema evolution)."""
        existing = {f.name.lower() for f in self.fields}
        added = [f for f in fields if f.name.lower() not in existing]
        return Schema(
            fields=[*self.fields, *added],
            partition_by=list(self.partition_by),
            primary_key=list(self.primary_key),
            sort_by=list(self.sort_by),
        )

    def select(self, *names: str) -> Schema:
        wanted = [n.lower() for n in names]
        return Schema(fields=[f for f in self.fields if f.name.lower() in wanted])

    # ------------------------------------------------------- serialisation

    def to_dict(self) -> dict[str, Any]:
        return {
            "fields": [f.to_dict() for f in self.fields],
            "partition_by": self.partition_by,
            "primary_key": self.primary_key,
            "sort_by": self.sort_by,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Schema:
        return cls(
            fields=[Field_.from_dict(f) for f in payload.get("fields", [])],
            partition_by=list(payload.get("partition_by", [])),
            primary_key=list(payload.get("primary_key", [])),
            sort_by=list(payload.get("sort_by", [])),
        )

    @classmethod
    def from_simple(cls, columns: dict[str, str], **kwargs: Any) -> Schema:
        """Build from a ``{"name": "type"}`` mapping — the form used in clara.yaml."""
        return cls(
            fields=[Field_(name=n, type=DataType.parse(t)) for n, t in columns.items()], **kwargs
        )

    # ---------------------------------------------------------- inference

    @classmethod
    def infer(cls, records: list[dict[str, Any]], sample: int = 200) -> Schema:
        """Infer a schema from sampled records.

        Connectors that expose no schema (CSV, JSON APIs) rely on this. Types
        widen on conflict — an int column that later sees a float becomes
        DOUBLE, and anything genuinely mixed falls back to STRING.
        """
        resolved: dict[str, DataType] = {}
        for record in records[:sample]:
            for key, value in record.items():
                found = _infer_value(value)
                if found is None:  # null tells us nothing
                    resolved.setdefault(key, DataType.STRING)
                    continue
                current = resolved.get(key)
                resolved[key] = found if current is None else _widen(current, found)
        return cls(fields=[Field_(name=k, type=v) for k, v in resolved.items()])


# --------------------------------------------------------------- type inference

_NUMERIC_ORDER = [DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.DECIMAL]


def _infer_value(value: Any) -> DataType | None:
    import datetime as _dt
    from decimal import Decimal

    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return DataType.BOOLEAN
    if isinstance(value, int):
        return DataType.INT if -(2**31) <= value < 2**31 else DataType.LONG
    if isinstance(value, float):
        return DataType.DOUBLE
    if isinstance(value, Decimal):
        return DataType.DECIMAL
    if isinstance(value, _dt.datetime):
        return DataType.TIMESTAMPTZ if value.tzinfo else DataType.TIMESTAMP
    if isinstance(value, _dt.date):
        return DataType.DATE
    if isinstance(value, (bytes, bytearray)):
        return DataType.BINARY
    if isinstance(value, (dict, list)):
        return DataType.JSON
    return DataType.STRING


def _widen(a: DataType, b: DataType) -> DataType:
    """Least common supertype of two observed types."""
    if a is b:
        return a
    if a in _NUMERIC_ORDER and b in _NUMERIC_ORDER:
        return _NUMERIC_ORDER[max(_NUMERIC_ORDER.index(a), _NUMERIC_ORDER.index(b))]
    if {a, b} == {DataType.TIMESTAMP, DataType.TIMESTAMPTZ}:
        return DataType.TIMESTAMPTZ
    if {a, b} == {DataType.DATE, DataType.TIMESTAMP}:
        return DataType.TIMESTAMP
    return DataType.STRING


# --------------------------------------------------------------- value coercion


def coerce_value(dtype: DataType, value: Any, *, precision: int = 38, scale: int = 9) -> Any:
    """Coerce a Python value into the representation a declared type expects.

    Connectors emit whatever their wire format carries — JSON has no date type,
    CSV has no types at all, and an API returns ``"2026-09-12T00:00:00Z"`` where
    the schema declares a timestamp. Arrow and Iceberg will not convert those
    implicitly, so the conversion happens here, once, at the write boundary.

    Unconvertible values are passed through rather than raising: a single bad
    cell should surface as a write error naming the column, not as an opaque
    failure deep inside Arrow.
    """
    import datetime as _dt
    from decimal import Decimal, InvalidOperation

    if value is None:
        return None

    if dtype is DataType.JSON:
        return json.dumps(value, default=str) if isinstance(value, (dict, list)) else value

    if dtype in (DataType.TIMESTAMP, DataType.TIMESTAMPTZ):
        parsed = value
        if isinstance(value, str):
            try:
                parsed = _parse_iso(value)
            except ValueError:
                return value
        elif isinstance(value, (int, float)):
            # Epoch seconds vs milliseconds: anything past year 5138 in seconds
            # is far more likely to be milliseconds.
            seconds = value / 1000.0 if value > 1e11 else float(value)
            parsed = _dt.datetime.fromtimestamp(seconds, tz=_dt.timezone.utc)
        elif isinstance(value, _dt.date) and not isinstance(value, _dt.datetime):
            parsed = _dt.datetime(value.year, value.month, value.day, tzinfo=_dt.timezone.utc)

        if not isinstance(parsed, _dt.datetime):
            return value
        if dtype is DataType.TIMESTAMPTZ:
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=_dt.timezone.utc)
        # A naive timestamp column must not carry a timezone, or Arrow rejects it.
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed

    if dtype is DataType.DATE:
        if isinstance(value, _dt.datetime):
            return value.date()
        if isinstance(value, _dt.date):
            return value
        if isinstance(value, str):
            try:
                return _parse_iso(value).date()
            except ValueError:
                return value
        return value

    if dtype is DataType.TIME:
        if isinstance(value, _dt.time):
            return value
        if isinstance(value, _dt.datetime):
            return value.time()
        if isinstance(value, str):
            try:
                return _dt.time.fromisoformat(value.replace("Z", ""))
            except ValueError:
                return value
        return value

    if dtype is DataType.DECIMAL:
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value)).quantize(Decimal(1).scaleb(-scale))
        except (InvalidOperation, ValueError, ArithmeticError):
            return value

    if dtype in (DataType.INT, DataType.LONG):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return value

    if dtype in (DataType.FLOAT, DataType.DOUBLE):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return value

    if dtype is DataType.BOOLEAN:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "t", "yes", "y", "1"):
                return True
            if lowered in ("false", "f", "no", "n", "0"):
                return False
        return value

    if dtype is DataType.BINARY:
        return value.encode() if isinstance(value, str) else value

    if dtype is DataType.STRING and not isinstance(value, str):
        return json.dumps(value, default=str) if isinstance(value, (dict, list)) else str(value)

    return value


def coerce_record(schema: Schema, record: dict[str, Any]) -> dict[str, Any]:
    """Project a record onto a schema, coercing every value to its column type.

    Projection matters as much as coercion: connectors emit sparse records and
    sometimes extra keys, and Arrow requires every batch to have exactly the
    declared columns.
    """
    return {
        f.name: coerce_value(
            f.type, record.get(f.name), precision=f.precision, scale=f.scale
        )
        for f in schema.fields
    }


def _parse_iso(value: str) -> Any:
    """Parse an ISO 8601 timestamp, tolerating a trailing Z and a space separator."""
    import datetime as _dt

    text = value.strip()
    if not text:
        raise ValueError("empty timestamp")
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        return _dt.datetime.fromisoformat(text)
    except ValueError:
        # Fractional seconds beyond microseconds, as some APIs emit.
        if "." in text:
            head, _, tail = text.partition(".")
            digits = "".join(c for c in tail if c.isdigit())[:6]
            offset = tail[len(digits) :].lstrip("0123456789")
            return _dt.datetime.fromisoformat(f"{head}.{digits or '0'}{offset}")
        raise


# ------------------------------------------------------------ SQL type mapping

_SQL: dict[str, dict[DataType, str]] = {
    "trino": {
        DataType.BOOLEAN: "BOOLEAN",
        DataType.INT: "INTEGER",
        DataType.LONG: "BIGINT",
        DataType.FLOAT: "REAL",
        DataType.DOUBLE: "DOUBLE",
        DataType.STRING: "VARCHAR",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME(6)",
        DataType.TIMESTAMP: "TIMESTAMP(6)",
        DataType.TIMESTAMPTZ: "TIMESTAMP(6) WITH TIME ZONE",
        DataType.BINARY: "VARBINARY",
        DataType.JSON: "VARCHAR",
    },
    "duckdb": {
        DataType.BOOLEAN: "BOOLEAN",
        DataType.INT: "INTEGER",
        DataType.LONG: "BIGINT",
        DataType.FLOAT: "FLOAT",
        DataType.DOUBLE: "DOUBLE",
        DataType.STRING: "VARCHAR",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMPTZ: "TIMESTAMPTZ",
        DataType.BINARY: "BLOB",
        DataType.JSON: "JSON",
    },
    "postgres": {
        DataType.BOOLEAN: "boolean",
        DataType.INT: "integer",
        DataType.LONG: "bigint",
        DataType.FLOAT: "real",
        DataType.DOUBLE: "double precision",
        DataType.STRING: "text",
        DataType.DATE: "date",
        DataType.TIME: "time",
        DataType.TIMESTAMP: "timestamp",
        DataType.TIMESTAMPTZ: "timestamptz",
        DataType.BINARY: "bytea",
        DataType.JSON: "jsonb",
    },
}


def sql_type(dtype: DataType, dialect: str = "trino", precision: int = 38, scale: int = 9) -> str:
    """Render a Clara type as a SQL type for the given dialect."""
    table = _SQL.get(dialect)
    if table is None:
        raise ValueError(f"unknown SQL dialect: {dialect}")
    if dtype is DataType.DECIMAL:
        return f"DECIMAL({precision}, {scale})"
    return table[dtype]


def ddl_columns(schema: Schema, dialect: str = "trino") -> str:
    """Column list for a CREATE TABLE statement."""
    parts = []
    for f in schema.fields:
        null = "" if f.nullable else " NOT NULL"
        parts.append(f'"{f.name}" {f.sql_type(dialect)}{null}')
    return ", ".join(parts)


# -------------------------------------------------------------- Arrow bridging


def to_arrow_schema(schema: Schema) -> Any:
    """Convert to a ``pyarrow.Schema`` (needed to write Parquet/Iceberg)."""
    from clara.errors import require

    pa = require("pyarrow", "duckdb", "Arrow schema conversion")
    mapping = {
        DataType.BOOLEAN: pa.bool_(),
        DataType.INT: pa.int32(),
        DataType.LONG: pa.int64(),
        DataType.FLOAT: pa.float32(),
        DataType.DOUBLE: pa.float64(),
        DataType.STRING: pa.string(),
        DataType.DATE: pa.date32(),
        DataType.TIME: pa.time64("us"),
        DataType.TIMESTAMP: pa.timestamp("us"),
        DataType.TIMESTAMPTZ: pa.timestamp("us", tz="UTC"),
        DataType.BINARY: pa.binary(),
        DataType.JSON: pa.string(),
    }
    fields = []
    for f in schema.fields:
        arrow_type = (
            pa.decimal128(f.precision, f.scale)
            if f.type is DataType.DECIMAL
            else mapping[f.type]
        )
        fields.append(pa.field(f.name, arrow_type, nullable=f.nullable))
    return pa.schema(fields)


def from_arrow_schema(arrow_schema: Any) -> Schema:
    """Convert a ``pyarrow.Schema`` into a Clara schema."""
    import pyarrow as pa

    fields: list[Field_] = []
    for f in arrow_schema:
        t = f.type
        if pa.types.is_boolean(t):
            dtype = DataType.BOOLEAN
        elif pa.types.is_int32(t) or pa.types.is_int16(t) or pa.types.is_int8(t):
            dtype = DataType.INT
        elif pa.types.is_integer(t):
            dtype = DataType.LONG
        elif pa.types.is_float32(t):
            dtype = DataType.FLOAT
        elif pa.types.is_floating(t):
            dtype = DataType.DOUBLE
        elif pa.types.is_decimal(t):
            fields.append(
                Field_(f.name, DataType.DECIMAL, f.nullable, precision=t.precision, scale=t.scale)
            )
            continue
        elif pa.types.is_date(t):
            dtype = DataType.DATE
        elif pa.types.is_time(t):
            dtype = DataType.TIME
        elif pa.types.is_timestamp(t):
            dtype = DataType.TIMESTAMPTZ if t.tz else DataType.TIMESTAMP
        elif pa.types.is_binary(t) or pa.types.is_large_binary(t):
            dtype = DataType.BINARY
        elif pa.types.is_nested(t):
            dtype = DataType.JSON
        else:
            dtype = DataType.STRING
        fields.append(Field_(f.name, dtype, f.nullable))
    return Schema(fields=fields)
