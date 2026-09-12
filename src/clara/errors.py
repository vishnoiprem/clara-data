"""Clara's error taxonomy.

Every error carries a stable machine-readable ``code`` and an HTTP ``status``, so
the control plane can translate any internal failure into a consistent API
response without a pile of per-route try/except blocks.
"""

from __future__ import annotations

from typing import Any


class ClaraError(Exception):
    """Base class for every error Clara raises deliberately."""

    code = "internal_error"
    status = 500

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return payload

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


# ------------------------------------------------------------------- 4xx family


class ValidationError(ClaraError):
    """Input was structurally or semantically invalid."""

    code = "validation_error"
    status = 422


class NotFoundError(ClaraError):
    """A referenced resource does not exist."""

    code = "not_found"
    status = 404


class ConflictError(ClaraError):
    """The request conflicts with current state (duplicate name, bad transition)."""

    code = "conflict"
    status = 409


class AuthenticationError(ClaraError):
    """Missing or invalid credentials."""

    code = "unauthenticated"
    status = 401


class PermissionError_(ClaraError):
    """Authenticated, but not allowed to do this."""

    code = "permission_denied"
    status = 403


class QuotaExceededError(ClaraError):
    """A plan limit or free-tier allowance has been exhausted.

    Carries the specific limit so the caller can render an actionable message
    ("you have used 20 of 20 daily CCU-minutes on the trial plan").
    """

    code = "quota_exceeded"
    status = 429


class BudgetExceededError(QuotaExceededError):
    """A customer-configured spend cap would be breached by this operation."""

    code = "budget_exceeded"


# ------------------------------------------------------------------- 5xx family


class EngineError(ClaraError):
    """A query engine rejected or failed a statement."""

    code = "engine_error"
    status = 400


class CatalogError(ClaraError):
    """The table catalog could not satisfy the operation."""

    code = "catalog_error"
    status = 500


class ConnectorError(ClaraError):
    """A source or destination connector failed."""

    code = "connector_error"
    status = 502


class ProviderError(ClaraError):
    """An IaaS provider operation failed."""

    code = "provider_error"
    status = 502


class DependencyMissingError(ClaraError):
    """An optional extra is required for the requested capability.

    Clara keeps heavy engines optional; this error tells the operator exactly
    which extra to install rather than surfacing a raw ImportError.
    """

    code = "dependency_missing"
    status = 501

    def __init__(self, package: str, extra: str, purpose: str) -> None:
        super().__init__(
            f"{purpose} requires the optional dependency '{package}'. "
            f"Install it with: pip install 'clara-data[{extra}]'",
            package=package,
            extra=extra,
        )


def require(module: str, extra: str, purpose: str) -> Any:
    """Import an optional dependency or raise an actionable error.

    Keeping every optional import behind this helper is what lets ``clara`` be
    installed with no engines at all and still start up cleanly.
    """
    import importlib

    try:
        return importlib.import_module(module)
    except ImportError as exc:  # pragma: no cover - depends on install shape
        raise DependencyMissingError(module, extra, purpose) from exc