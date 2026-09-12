"""Runs and task runs.

A *run* is one execution of a pipeline; a *task run* is one step inside it.
Both are plain dataclasses so the executor, the API and the CLI share a single
status vocabulary and the same duration arithmetic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from clara.ids import Prefix, new_id
from clara.time_utils import format_duration, seconds_between, to_iso, utcnow


class RunStatus(str, Enum):
    """Lifecycle of a run or task run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    #: Skipped because an upstream task failed.
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in (
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
            RunStatus.SKIPPED,
            RunStatus.CANCELLED,
        )

    @property
    def is_failure(self) -> bool:
        return self in (RunStatus.FAILED, RunStatus.CANCELLED)


class TaskKind(str, Enum):
    """What a task does. Drives how the executor dispatches it."""

    INGEST = "ingest"
    TRANSFORM = "transform"
    #: Iceberg housekeeping: compaction, snapshot expiry, orphan cleanup.
    MAINTENANCE = "maintenance"
    #: Arbitrary SQL, e.g. a post-load assertion.
    SQL = "sql"


@dataclass
class TaskRun:
    """One step of a run."""

    name: str
    kind: TaskKind
    id: str = field(default_factory=lambda: new_id(Prefix.TASK))
    status: RunStatus = RunStatus.PENDING
    started_at: datetime | None = None
    finished_at: datetime | None = None
    attempt: int = 1
    error: str | None = None
    #: Step-specific payload: records synced, rows built, tables touched.
    output: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        return seconds_between(self.started_at, self.finished_at or utcnow())

    def start(self) -> None:
        self.status = RunStatus.RUNNING
        self.started_at = utcnow()

    def succeed(self, **output: Any) -> None:
        self.status = RunStatus.SUCCEEDED
        self.finished_at = utcnow()
        self.output.update(output)

    def fail(self, error: str) -> None:
        self.status = RunStatus.FAILED
        self.finished_at = utcnow()
        self.error = error

    def skip(self, reason: str) -> None:
        self.status = RunStatus.SKIPPED
        self.finished_at = utcnow()
        self.error = reason

    def log(self, message: str) -> None:
        self.logs.append(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "kind": self.kind.value,
            "status": self.status.value,
            "attempt": self.attempt,
            "started_at": to_iso(self.started_at) if self.started_at else None,
            "finished_at": to_iso(self.finished_at) if self.finished_at else None,
            "duration_seconds": round(self.duration_seconds, 3),
            "duration": format_duration(self.duration_seconds),
            "depends_on": self.depends_on,
            "error": self.error,
            "output": self.output,
            "logs": self.logs[-100:],
        }


@dataclass
class Run:
    """One execution of a pipeline."""

    pipeline: str
    id: str = field(default_factory=lambda: new_id(Prefix.RUN))
    workspace_id: str | None = None
    tenant_id: str | None = None
    status: RunStatus = RunStatus.PENDING
    #: ``scheduled``, ``manual``, ``api``, ``backfill``.
    trigger: str = "manual"
    started_at: datetime | None = None
    finished_at: datetime | None = None
    tasks: list[TaskRun] = field(default_factory=list)
    #: Connector state carried between runs, keyed by source name.
    state: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        return seconds_between(self.started_at, self.finished_at or utcnow())

    @property
    def records_synced(self) -> int:
        return sum(int(t.output.get("records", 0)) for t in self.tasks if t.kind is TaskKind.INGEST)

    @property
    def models_built(self) -> int:
        return sum(
            1
            for t in self.tasks
            if t.kind is TaskKind.TRANSFORM and t.status is RunStatus.SUCCEEDED
        )

    def task(self, name: str) -> TaskRun | None:
        return next((t for t in self.tasks if t.name == name), None)

    def add_task(self, task: TaskRun) -> TaskRun:
        self.tasks.append(task)
        return task

    def start(self) -> None:
        self.status = RunStatus.RUNNING
        self.started_at = utcnow()

    def finish(self) -> None:
        """Close the run, deriving its status from its tasks."""
        self.finished_at = utcnow()
        if any(t.status is RunStatus.FAILED for t in self.tasks):
            self.status = RunStatus.FAILED
            failed = [t.name for t in self.tasks if t.status is RunStatus.FAILED]
            self.error = f"failed tasks: {', '.join(failed)}"
        elif not self.tasks:
            self.status = RunStatus.SUCCEEDED
        else:
            self.status = RunStatus.SUCCEEDED

    def cancel(self) -> None:
        self.status = RunStatus.CANCELLED
        self.finished_at = utcnow()
        for task in self.tasks:
            if not task.status.is_terminal:
                task.status = RunStatus.CANCELLED
                task.finished_at = utcnow()

    def to_dict(self, *, include_tasks: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "pipeline": self.pipeline,
            "workspace_id": self.workspace_id,
            "status": self.status.value,
            "trigger": self.trigger,
            "started_at": to_iso(self.started_at) if self.started_at else None,
            "finished_at": to_iso(self.finished_at) if self.finished_at else None,
            "duration_seconds": round(self.duration_seconds, 3),
            "duration": format_duration(self.duration_seconds),
            "records_synced": self.records_synced,
            "models_built": self.models_built,
            "task_count": len(self.tasks),
            "error": self.error,
        }
        if include_tasks:
            payload["tasks"] = [t.to_dict() for t in self.tasks]
            payload["state"] = self.state
        return payload
