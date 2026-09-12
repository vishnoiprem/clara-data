"""Dependency graphs.

A small, dependency-free DAG. Used by the transform runner for model ordering
and by the pipeline executor for task ordering, so both get the same cycle
detection and the same parallel-batch logic.
"""

from __future__ import annotations

import re
from collections import defaultdict, deque
from typing import Any, Iterable

from clara.errors import ValidationError


class DAG:
    """A directed acyclic graph of named nodes."""

    def __init__(self) -> None:
        self._nodes: set[str] = set()
        self._downstream: dict[str, set[str]] = defaultdict(set)
        self._upstream: dict[str, set[str]] = defaultdict(set)

    # ------------------------------------------------------------ construction

    def add_node(self, name: str) -> None:
        self._nodes.add(name)

    def add_edge(self, upstream: str, downstream: str) -> None:
        """Declare that ``downstream`` depends on ``upstream``."""
        if upstream == downstream:
            raise ValidationError(f"node cannot depend on itself: {upstream}")
        self._nodes.update((upstream, downstream))
        self._downstream[upstream].add(downstream)
        self._upstream[downstream].add(upstream)

    def add_dependencies(self, node: str, upstreams: Iterable[str]) -> None:
        self.add_node(node)
        for upstream in upstreams:
            self.add_edge(upstream, node)

    # -------------------------------------------------------------- inspection

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def upstream(self, node: str) -> set[str]:
        """Direct dependencies of a node."""
        return set(self._upstream.get(node, set()))

    def downstream(self, node: str) -> set[str]:
        """Direct dependents of a node."""
        return set(self._downstream.get(node, set()))

    def roots(self) -> list[str]:
        """Nodes with no dependencies."""
        return sorted(n for n in self._nodes if not self._upstream.get(n))

    def leaves(self) -> list[str]:
        return sorted(n for n in self._nodes if not self._downstream.get(n))

    def ancestors(self, node: str) -> set[str]:
        """Every node this one transitively depends on."""
        return self._reachable(node, self._upstream)

    def descendants(self, node: str) -> set[str]:
        """Every node that transitively depends on this one."""
        return self._reachable(node, self._downstream)

    def _reachable(self, start: str, edges: dict[str, set[str]]) -> set[str]:
        seen: set[str] = set()
        queue = deque(edges.get(start, set()))
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            queue.extend(edges.get(current, set()))
        return seen

    # -------------------------------------------------------------- ordering

    def topological_order(self) -> list[str]:
        """Nodes in dependency order, raising on a cycle.

        Kahn's algorithm, with ties broken alphabetically so that runs are
        reproducible — a non-deterministic build order makes failures much
        harder to reason about.
        """
        indegree = {n: len(self._upstream.get(n, set())) for n in self._nodes}
        ready = sorted(n for n, degree in indegree.items() if degree == 0)
        order: list[str] = []

        while ready:
            current = ready.pop(0)
            order.append(current)
            for dependent in sorted(self._downstream.get(current, set())):
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    ready.append(dependent)
            ready.sort()

        if len(order) != len(self._nodes):
            raise ValidationError(
                "dependency cycle detected", cycle=sorted(set(self._nodes) - set(order))
            )
        return order

    def batches(self) -> list[list[str]]:
        """Nodes grouped into levels that can run concurrently.

        Everything in one batch is independent, so a scheduler can fan out
        within a batch and only synchronise between batches.
        """
        remaining = {n: set(self._upstream.get(n, set())) for n in self._nodes}
        levels: list[list[str]] = []
        done: set[str] = set()

        while remaining:
            ready = sorted(n for n, deps in remaining.items() if deps <= done)
            if not ready:
                raise ValidationError(
                    "dependency cycle detected", cycle=sorted(remaining)
                )
            levels.append(ready)
            done.update(ready)
            for node in ready:
                del remaining[node]
        return levels

    def has_cycle(self) -> bool:
        try:
            self.topological_order()
            return False
        except ValidationError:
            return True

    # ----------------------------------------------------------- presentation

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": self.nodes,
            "edges": [
                {"from": up, "to": down}
                for up, downs in sorted(self._downstream.items())
                for down in sorted(downs)
            ],
            "roots": self.roots(),
            "leaves": self.leaves(),
            "batches": self.batches(),
        }

    def to_mermaid(self) -> str:
        """Mermaid diagram source — rendered as the lineage graph in the console."""
        lines = ["graph LR"]
        for node in self.nodes:
            lines.append(f'  {_safe(node)}["{node}"]')
        for up, downs in sorted(self._downstream.items()):
            for down in sorted(downs):
                lines.append(f"  {_safe(up)} --> {_safe(down)}")
        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node: object) -> bool:
        return node in self._nodes


def _safe(name: str) -> str:
    """Sanitise a node name into a Mermaid-safe identifier.

    Mermaid ids must be alphanumeric or underscore. Colons matter most here:
    every Clara task is named ``ingest:x`` or ``model:y``, so without this the
    diagram is malformed for every real pipeline.
    """
    return re.sub(r"[^0-9A-Za-z_]", "_", name)
