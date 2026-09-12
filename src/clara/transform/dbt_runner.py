"""dbt-core bridge.

For teams that already have a dbt project, Clara runs it rather than asking them
to rewrite it. dbt is invoked as a subprocess with a generated profile pointing
at Clara's engine, and its ``run_results.json`` is parsed back into Clara's
result types so a dbt run appears in the console like any other.

This is the "consulting and flexibility" half of the product requirement: the
console-authored path for businesses with no data team, and this path for teams
who have one and want their existing tooling.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from clara.errors import DependencyMissingError, ValidationError
from clara.logging_setup import get_logger
from clara.settings import Settings, get_settings
from clara.transform.runner import ModelResult, TransformResult
from clara.time_utils import utcnow

log = get_logger(__name__)


@dataclass
class DbtProject:
    """A dbt project on disk."""

    path: Path
    profile_name: str = "clara"
    target: str = "clara"

    def __post_init__(self) -> None:
        self.path = Path(self.path).expanduser().resolve()
        if not (self.path / "dbt_project.yml").is_file():
            raise ValidationError(f"not a dbt project (no dbt_project.yml): {self.path}")

    @property
    def name(self) -> str:
        payload = yaml.safe_load((self.path / "dbt_project.yml").read_text()) or {}
        return str(payload.get("name", self.path.name))


class DbtRunner:
    """Runs dbt-core against Clara's engines."""

    def __init__(
        self,
        project: DbtProject | str | Path,
        *,
        settings: Settings | None = None,
        engine: str = "trino",
    ) -> None:
        self.project = project if isinstance(project, DbtProject) else DbtProject(Path(project))
        self.settings = settings or get_settings()
        self.engine = engine

    # -------------------------------------------------------------- discovery

    @staticmethod
    def available() -> bool:
        """Whether a ``dbt`` executable is on PATH."""
        return shutil.which("dbt") is not None

    def _require_dbt(self) -> str:
        executable = shutil.which("dbt")
        if executable is None:
            raise DependencyMissingError("dbt-core", "dbt", "running a dbt project")
        return executable

    # ----------------------------------------------------------------- profile

    def write_profile(self, directory: Path | None = None) -> Path:
        """Generate ``profiles.yml`` pointing dbt at Clara's engine.

        Generating this rather than asking the user to maintain it removes the
        single most common dbt onboarding failure — a misconfigured profile.
        """
        target_dir = directory or (self.project.path / ".clara")
        target_dir.mkdir(parents=True, exist_ok=True)

        if self.engine == "trino":
            output = {
                "type": "trino",
                "method": "none" if not self.settings.trino.password else "ldap",
                "host": self.settings.trino.host,
                "port": self.settings.trino.port,
                "user": self.settings.trino.user,
                "password": self.settings.trino.password,
                "catalog": self.settings.trino.catalog,
                "schema": self.settings.trino.schema_,
                "http_scheme": self.settings.trino.http_scheme,
                "threads": 4,
            }
        elif self.engine == "duckdb":
            output = {
                "type": "duckdb",
                "path": str(self.settings.path_in_state(self.settings.duckdb.path)),
                "schema": "analytics",
                "threads": self.settings.duckdb.threads,
            }
        else:
            raise ValidationError(f"no dbt adapter mapping for engine: {self.engine}")

        profile = {
            self.project.profile_name: {
                "target": self.project.target,
                "outputs": {self.project.target: {k: v for k, v in output.items() if v is not None}},
            }
        }
        path = target_dir / "profiles.yml"
        path.write_text(yaml.safe_dump(profile, sort_keys=False))
        return path

    # --------------------------------------------------------------- execution

    def run(
        self,
        *,
        select: list[str] | None = None,
        full_refresh: bool = False,
        command: str = "run",
        variables: dict[str, Any] | None = None,
        timeout_seconds: float = 3600.0,
    ) -> TransformResult:
        """Invoke dbt and translate its results."""
        executable = self._require_dbt()
        profiles_dir = self.write_profile().parent

        argv = [
            executable,
            command,
            "--project-dir",
            str(self.project.path),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            self.project.target,
        ]
        if select:
            argv += ["--select", " ".join(select)]
        if full_refresh and command == "run":
            argv.append("--full-refresh")
        if variables:
            argv += ["--vars", json.dumps(variables)]

        log.info("running dbt", extra={"command": command, "project": self.project.name})
        started = time.perf_counter()
        completed = subprocess.run(  # noqa: S603 - executable resolved from PATH
            argv,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=str(self.project.path),
        )
        duration = time.perf_counter() - started

        if completed.returncode != 0:
            log.warning(
                "dbt exited non-zero",
                extra={"returncode": completed.returncode, "stderr": completed.stderr[-2000:]},
            )

        result = self._parse_run_results(duration)
        if not result.results:
            # dbt failed before producing artifacts (bad profile, compile error).
            result.results.append(
                ModelResult(
                    model=command,
                    table="-",
                    materialization="dbt",
                    succeeded=completed.returncode == 0,
                    duration_seconds=duration,
                    error=(completed.stderr or completed.stdout or "").strip()[-1000:] or None,
                )
            )
        return result

    def test(self, select: list[str] | None = None) -> TransformResult:
        return self.run(command="test", select=select)

    def _parse_run_results(self, duration: float) -> TransformResult:
        """Read dbt's ``run_results.json`` into Clara's result types."""
        outcome = TransformResult(duration_seconds=duration, started_at=utcnow())
        artifact = self.project.path / "target" / "run_results.json"
        if not artifact.is_file():
            return outcome

        try:
            payload = json.loads(artifact.read_text())
        except (OSError, json.JSONDecodeError):  # pragma: no cover
            return outcome

        for node in payload.get("results", []):
            unique_id = node.get("unique_id", "")
            name = unique_id.split(".")[-1] if unique_id else "unknown"
            status = str(node.get("status", "")).lower()
            outcome.results.append(
                ModelResult(
                    model=name,
                    table=node.get("relation_name") or name,
                    materialization="dbt",
                    succeeded=status in ("success", "pass"),
                    skipped=status == "skipped",
                    rows=(node.get("adapter_response") or {}).get("rows_affected"),
                    duration_seconds=float(node.get("execution_time") or 0.0),
                    engine=self.engine,
                    error=node.get("message") if status not in ("success", "pass") else None,
                )
            )
        return outcome

    # ----------------------------------------------------------------- imports

    def import_models(self) -> list[Any]:
        """Read a dbt project's models into Clara ``Model`` objects.

        Lets a team migrate off dbt-as-a-runtime while keeping their SQL, and
        lets the console display and edit dbt-authored models.
        """
        from clara.transform.models import Materialization, Model

        project_config = yaml.safe_load((self.project.path / "dbt_project.yml").read_text()) or {}
        default_materialization = (
            ((project_config.get("models") or {}).get(self.project.name) or {}).get(
                "+materialized"
            )
            or "table"
        )

        models: list[Model] = []
        for model_dir in ("models",):
            for sql_path in sorted((self.project.path / model_dir).rglob("*.sql")):
                models.append(
                    Model(
                        name=sql_path.stem,
                        sql=sql_path.read_text(),
                        materialization=Materialization(str(default_materialization).lower()),
                        description=f"imported from {sql_path.relative_to(self.project.path)}",
                    )
                )
        return models
