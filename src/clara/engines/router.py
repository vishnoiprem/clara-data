"""Engine routing.

The decision "which engine should run this query" is normally a human's job on
other platforms — you pick a cluster, and you pay for that choice. Clara makes
it automatically, because it is the highest-leverage cost decision on the
platform and because the target customer has no data engineer to make it.

The policy, in order:

1. An explicitly pinned engine always wins. Predictability beats cleverness.
2. Statements needing distributed features (large joins, table maintenance) go
   to Trino.
3. Everything else is sized from catalog statistics. Small scans go to DuckDB —
   no cluster, no startup cost, no shuffle. Large ones go to Trino.
4. If only one engine is actually reachable, use it and say so.

Every decision carries a human-readable ``reason``, surfaced in query history,
so the routing is auditable rather than magic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from clara.catalog.base import Catalog, TableRef
from clara.engines.base import Engine
from clara.engines.warehouse import Warehouse, WarehouseSize
from clara.errors import EngineError
from clara.logging_setup import get_logger

log = get_logger(__name__)

#: Scans below this go to DuckDB. Set from measured crossover: below ~20 GB a
#: single well-provisioned node beats distributed execution once coordination
#: and shuffle overhead are counted.
DUCKDB_SCAN_LIMIT_BYTES = 20 * 1024**3

#: Table references in FROM / JOIN clauses. Deliberately simple: a real parser
#: is not needed to decide routing, and a wrong guess only costs a suboptimal
#: engine choice, never a wrong answer.
_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([\"\w.]+)",
    re.IGNORECASE,
)

#: Statements that must run on Trino: Iceberg maintenance and multi-statement DDL
#: that DuckDB either cannot express or would apply to the wrong catalog.
_TRINO_ONLY_RE = re.compile(
    r"\b(ALTER\s+TABLE\s+.*\bEXECUTE\b|CALL\s+system\.|SHOW\s+STATS)",
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class RoutingDecision:
    """Which engine, and why."""

    engine: Engine
    reason: str
    estimated_bytes: int = 0
    tables: list[str] = field(default_factory=list)
    #: Alternative that was considered but not chosen.
    runner_up: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "engine": self.engine.name,
            "reason": self.reason,
            "estimated_bytes": self.estimated_bytes,
            "tables": self.tables,
            "runner_up": self.runner_up,
        }


class EngineRouter:
    """Chooses an engine per statement."""

    def __init__(
        self,
        engines: dict[str, Engine],
        *,
        catalog: Catalog | None = None,
        scan_limit_bytes: int = DUCKDB_SCAN_LIMIT_BYTES,
    ) -> None:
        if not engines:
            raise EngineError("router requires at least one engine")
        self.engines = engines
        self.catalog = catalog
        self.scan_limit_bytes = scan_limit_bytes
        self._size_cache: dict[str, int] = {}

    # ------------------------------------------------------------------ routing

    def route(
        self,
        sql: str,
        *,
        warehouse: Warehouse | None = None,
        default_namespace: str = "main",
    ) -> RoutingDecision:
        statement = sql.strip()
        tables = self.referenced_tables(statement, default_namespace)
        table_names = [t.fqn for t in tables]

        # 1. Explicit pin.
        pinned = warehouse.engine if warehouse else None
        if pinned and pinned != "auto":
            engine = self.engines.get(pinned)
            if engine is None:
                raise EngineError(
                    f"warehouse pins engine {pinned!r}, which is not available",
                    available=sorted(self.engines),
                )
            return RoutingDecision(engine, f"pinned to {pinned} by warehouse", tables=table_names)

        # 2. Only one engine configured.
        if len(self.engines) == 1:
            only = next(iter(self.engines.values()))
            return RoutingDecision(only, f"{only.name} is the only configured engine", tables=table_names)

        # 3. Statements that require Trino.
        if _TRINO_ONLY_RE.search(statement) and "trino" in self.engines:
            return RoutingDecision(
                self.engines["trino"],
                "statement requires distributed engine features",
                tables=table_names,
                runner_up="duckdb",
            )

        # 4. Size-based routing.
        estimated = self.estimate_bytes(tables)
        duckdb = self.engines.get("duckdb")
        trino = self.engines.get("trino")

        if duckdb is not None and estimated <= self.scan_limit_bytes:
            # A big warehouse is a deliberate signal that the user expects
            # scale-out; honour it rather than quietly running single-node.
            if warehouse and warehouse.spec.total_vcpus > 16 and trino is not None:
                return RoutingDecision(
                    trino,
                    f"warehouse size {warehouse.size.value} implies distributed execution",
                    estimated_bytes=estimated,
                    tables=table_names,
                    runner_up="duckdb",
                )
            return RoutingDecision(
                duckdb,
                f"estimated scan {_human(estimated)} fits single-node",
                estimated_bytes=estimated,
                tables=table_names,
                runner_up="trino" if trino else None,
            )

        if trino is not None:
            return RoutingDecision(
                trino,
                f"estimated scan {_human(estimated)} exceeds single-node limit",
                estimated_bytes=estimated,
                tables=table_names,
                runner_up="duckdb" if duckdb else None,
            )

        fallback = next(iter(self.engines.values()))
        return RoutingDecision(
            fallback,
            f"no distributed engine available; running on {fallback.name}",
            estimated_bytes=estimated,
            tables=table_names,
        )

    # --------------------------------------------------------------- estimation

    def referenced_tables(self, sql: str, default_namespace: str = "main") -> list[TableRef]:
        """Table references found in the statement, best effort."""
        found: list[TableRef] = []
        seen: set[str] = set()
        for raw in _TABLE_RE.findall(sql):
            candidate = raw.strip().strip('"')
            # Skip table functions and subquery aliases.
            if "(" in candidate or not candidate:
                continue
            try:
                ref = TableRef.parse(candidate, default_namespace)
            except Exception:  # noqa: BLE001 - unparseable reference is not fatal
                continue
            if ref.fqn not in seen:
                seen.add(ref.fqn)
                found.append(ref)
        return found

    def estimate_bytes(self, tables: list[TableRef]) -> int:
        """Sum catalog-reported sizes of the referenced tables.

        Catalog statistics are free to read (they are in Iceberg snapshot
        metadata), which is why routing uses them instead of asking an engine
        to plan the query first.
        """
        if self.catalog is None:
            return 0
        total = 0
        for ref in tables:
            if ref.fqn in self._size_cache:
                total += self._size_cache[ref.fqn]
                continue
            try:
                info = self.catalog.load_table(ref)
                size = int(info.size_bytes or 0)
            except Exception:  # noqa: BLE001 - unknown table contributes nothing
                size = 0
            self._size_cache[ref.fqn] = size
            total += size
        return total

    def invalidate_stats(self, ref: TableRef | None = None) -> None:
        """Drop cached sizes after a write, so routing reflects new data."""
        if ref is None:
            self._size_cache.clear()
        else:
            self._size_cache.pop(ref.fqn, None)

    # ------------------------------------------------------------- convenience

    def execute(
        self,
        sql: str,
        *,
        warehouse: Warehouse | None = None,
        default_namespace: str = "main",
        max_rows: int | None = None,
    ) -> tuple[Any, RoutingDecision]:
        """Route and run, returning the result alongside the decision."""
        decision = self.route(sql, warehouse=warehouse, default_namespace=default_namespace)
        result = decision.engine.execute(sql, max_rows=max_rows)
        result.stats.engine = decision.engine.name
        return result, decision

    def recommend_warehouse_size(self, sql: str, default_namespace: str = "main") -> WarehouseSize:
        """Size recommendation for a statement — the autosizing entry point."""
        from clara.engines.warehouse import recommend_size

        tables = self.referenced_tables(sql, default_namespace)
        return recommend_size(self.estimate_bytes(tables))

    def close(self) -> None:
        for engine in self.engines.values():
            engine.close()


def _human(num_bytes: float) -> str:
    """Byte count as a short human string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024 or unit == "TB":
            return f"{num_bytes:.1f}{unit}" if unit != "B" else f"{int(num_bytes)}B"
        num_bytes /= 1024
    return f"{num_bytes:.1f}TB"
