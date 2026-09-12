"""SQL transformation."""

from __future__ import annotations

from clara.transform.models import Materialization, Model
from clara.transform.runner import ModelResult, TransformResult, TransformRunner

__all__ = [
    "Materialization",
    "Model",
    "ModelResult",
    "TransformResult",
    "TransformRunner",
    "dbt_available",
]


def dbt_available() -> bool:
    """Whether a dbt executable is present, without importing the bridge."""
    import shutil

    return shutil.which("dbt") is not None
