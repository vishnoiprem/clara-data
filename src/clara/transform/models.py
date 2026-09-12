"""SQL transformation models.

The transform layer is deliberately dbt-compatible in its *authoring* model —
``{{ ref('other_model') }}``, materializations, incremental logic — but does not
require dbt to run. Clara resolves references, builds the dependency graph and
executes models directly on whichever engine the router picks.

Why not just require dbt? Because dbt needs a project directory, a profile, a
Python environment and someone who understands all three. The target customer
has none of those. A user of Clara's console types SQL into a box and gets a
table; a dbt user can point Clara at their existing project instead
(``clara.transform.dbt_runner``). Both paths produce the same lakehouse tables.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from clara.catalog.base import TableRef
from clara.errors import ValidationError
from clara.ids import slugify
from clara.logging_setup import get_logger

log = get_logger(__name__)

#: ``{{ ref('model') }}`` — a dependency on another model.
_REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"]([\w.]+)['\"]\s*\)\s*\}\}")
#: ``{{ source('namespace', 'table') }}`` — a dependency on an ingested table.
_SOURCE_RE = re.compile(
    r"\{\{\s*source\(\s*['\"]([\w]+)['\"]\s*,\s*['\"]([\w]+)['\"]\s*\)\s*\}\}"
)
#: ``{{ var('name', 'default') }}`` — a run-time parameter.
_VAR_RE = re.compile(r"\{\{\s*var\(\s*['\"](\w+)['\"]\s*(?:,\s*(.+?))?\s*\)\s*\}\}")
#: ``{% if is_incremental() %} ... {% endif %}`` — incremental-only predicate.
_INCREMENTAL_BLOCK_RE = re.compile(
    r"\{%-?\s*if\s+is_incremental\(\)\s*-?%\}(.*?)\{%-?\s*endif\s*-?%\}",
    re.DOTALL | re.IGNORECASE,
)
#: ``{{ this }}`` — the model's own table, used inside incremental predicates.
_THIS_RE = re.compile(r"\{\{\s*this\s*\}\}")


class Materialization(str, Enum):
    """How a model's result is persisted."""

    #: Rebuild the whole table each run. Simple and always correct.
    TABLE = "table"
    #: Store only the query; compute on read. Free to build, costs on every read.
    VIEW = "view"
    #: Append or merge only new rows. The only affordable option at scale.
    INCREMENTAL = "incremental"
    #: Compute nothing, persist nothing — a named CTE inlined into consumers.
    EPHEMERAL = "ephemeral"


@dataclass
class Model:
    """One SQL transformation."""

    name: str
    sql: str
    materialization: Materialization = Materialization.TABLE
    namespace: str = "analytics"
    description: str = ""
    #: Columns forming the natural key. Required for incremental merges.
    unique_key: list[str] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    #: Explicit dependencies, merged with those parsed from the SQL.
    depends_on: list[str] = field(default_factory=list)
    #: Lightweight data tests, evaluated after the model builds.
    tests: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValidationError("model requires a name")
        self.name = slugify(self.name)
        if not self.sql.strip():
            raise ValidationError(f"model {self.name} has no SQL")
        if isinstance(self.materialization, str):
            self.materialization = Materialization(self.materialization.lower())
        if self.materialization is Materialization.INCREMENTAL and not self.unique_key:
            # Without a key, an incremental model can only append — which is
            # correct for immutable event data and wrong for anything mutable.
            # Warn rather than fail, since append-only is a legitimate choice.
            log.warning(
                "incremental model has no unique_key; it will append rather than merge",
                extra={"model": self.name},
            )

    @property
    def ref(self) -> TableRef:
        return TableRef(self.namespace, self.name)

    # ------------------------------------------------------------ dependencies

    def model_refs(self) -> list[str]:
        """Names of models this one reads, from ``ref()`` plus explicit deps."""
        found = list(dict.fromkeys(_REF_RE.findall(self.sql)))
        return list(dict.fromkeys([*found, *self.depends_on]))

    def source_refs(self) -> list[TableRef]:
        """Ingested tables this model reads, from ``source()``."""
        return [TableRef(ns, table) for ns, table in _SOURCE_RE.findall(self.sql)]

    # ------------------------------------------------------------- compilation

    def compile(
        self,
        *,
        models: dict[str, Model] | None = None,
        variables: dict[str, Any] | None = None,
        incremental: bool = False,
        quote: Any = None,
    ) -> str:
        """Resolve templating into executable SQL.

        ``incremental`` controls whether ``is_incremental()`` blocks are kept.
        On a first run the table does not exist yet, so those predicates must be
        dropped or the model cannot build.
        """
        models = models or {}
        variables = variables or {}
        quoter = quote or (lambda ref: f'"{ref.namespace}"."{ref.name}"')
        sql = self.sql

        # Incremental blocks first: they may contain refs and vars.
        sql = _INCREMENTAL_BLOCK_RE.sub(lambda m: m.group(1) if incremental else "", sql)
        sql = _THIS_RE.sub(quoter(self.ref), sql)

        def replace_ref(match: re.Match[str]) -> str:
            target = slugify(match.group(1))
            upstream = models.get(target)
            if upstream is None:
                # Unknown ref: assume it is a model in this namespace. The
                # engine will produce a clear "table not found" if it is not.
                return quoter(TableRef(self.namespace, target))
            if upstream.materialization is Materialization.EPHEMERAL:
                return f"({upstream.compile(models=models, variables=variables, quote=quote)})"
            return quoter(upstream.ref)

        sql = _REF_RE.sub(replace_ref, sql)
        sql = _SOURCE_RE.sub(lambda m: quoter(TableRef(m.group(1), m.group(2))), sql)

        def replace_var(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in variables:
                return _sql_literal(variables[name])
            if default is not None:
                return default.strip()
            raise ValidationError(
                f"model {self.name} uses undefined variable {name!r}", variable=name
            )

        sql = _VAR_RE.sub(replace_var, sql)
        return sql.strip().rstrip(";")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "namespace": self.namespace,
            "materialization": self.materialization.value,
            "description": self.description,
            "unique_key": self.unique_key,
            "partition_by": self.partition_by,
            "tags": self.tags,
            "depends_on": self.model_refs(),
            "sources": [r.fqn for r in self.source_refs()],
            "tests": self.tests,
            "sql": self.sql,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> Model:
        return cls(
            name=payload["name"],
            sql=payload["sql"],
            materialization=Materialization(
                str(payload.get("materialization", "table")).lower()
            ),
            namespace=payload.get("namespace", "analytics"),
            description=payload.get("description", ""),
            unique_key=_as_list(payload.get("unique_key")),
            partition_by=_as_list(payload.get("partition_by")),
            tags=_as_list(payload.get("tags")),
            depends_on=_as_list(payload.get("depends_on")),
            tests=list(payload.get("tests") or []),
        )


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def _sql_literal(value: Any) -> str:
    """Render a Python value as a SQL literal, escaping quotes."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
