"""API authentication.

API-key based, with the key hashed at rest. Local development can disable auth
entirely (``CLARA_AUTH_DISABLED=true``), which ``Settings`` refuses outside the
local environment — a misconfigured production deployment should fail to start
rather than serve unauthenticated.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from fastapi import Header, HTTPException

from clara.ids import Prefix, new_api_key, new_id
from clara.logging_setup import get_logger
from clara.settings import Settings, get_settings
from clara.time_utils import to_iso, utcnow

log = get_logger(__name__)


def hash_key(secret: str) -> str:
    """Hash an API key secret. Only the digest is ever stored."""
    return hashlib.sha256(secret.encode()).hexdigest()


@dataclass
class ApiKey:
    """An issued API key."""

    key_hash: str
    id: str = field(default_factory=lambda: new_id(Prefix.API_KEY))
    name: str = "default"
    tenant_id: str = "ten_local"
    created_at: datetime = field(default_factory=utcnow)
    last_used_at: datetime | None = None
    #: ``admin`` can mutate; ``read`` cannot.
    role: str = "admin"
    revoked: bool = False

    def matches(self, secret: str) -> bool:
        # Constant-time comparison: a timing side channel on key verification
        # is a real, cheap attack.
        return hmac.compare_digest(self.key_hash, hash_key(secret))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "tenant_id": self.tenant_id,
            "role": self.role,
            "revoked": self.revoked,
            "created_at": to_iso(self.created_at),
            "last_used_at": to_iso(self.last_used_at) if self.last_used_at else None,
        }


class KeyStore:
    """In-memory API key registry.

    A production deployment swaps this for the control-plane database; the
    interface is intentionally tiny so that substitution is trivial.
    """

    def __init__(self) -> None:
        self._keys: dict[str, ApiKey] = {}
        self._lock = threading.RLock()
        self.bootstrap_secret: str | None = None

    def issue(self, name: str = "default", *, role: str = "admin", tenant_id: str = "ten_local") -> tuple[ApiKey, str]:
        """Mint a key, returning it with its one-time plaintext secret."""
        _, secret = new_api_key()
        key = ApiKey(key_hash=hash_key(secret), name=name, role=role, tenant_id=tenant_id)
        with self._lock:
            self._keys[key.id] = key
        return key, secret

    def adopt(self, secret: str, name: str = "bootstrap") -> ApiKey:
        """Register a caller-supplied secret (``CLARA_BOOTSTRAP_API_KEY``)."""
        key = ApiKey(key_hash=hash_key(secret), name=name)
        with self._lock:
            self._keys[key.id] = key
        return key

    def verify(self, secret: str) -> ApiKey | None:
        with self._lock:
            for key in self._keys.values():
                if not key.revoked and key.matches(secret):
                    key.last_used_at = utcnow()
                    return key
        return None

    def revoke(self, key_id: str) -> bool:
        with self._lock:
            key = self._keys.get(key_id)
            if key is None:
                return False
            key.revoked = True
            return True

    def list(self) -> list[ApiKey]:
        with self._lock:
            return list(self._keys.values())

    @property
    def empty(self) -> bool:
        return not any(not k.revoked for k in self.list())


_STORE = KeyStore()


def get_key_store() -> KeyStore:
    return _STORE


def bootstrap(settings: Settings | None = None) -> str | None:
    """Ensure at least one key exists, returning the plaintext if newly minted.

    Called at startup. If ``CLARA_BOOTSTRAP_API_KEY`` is set it is adopted;
    otherwise a key is generated and logged once, so an operator is never left
    with a running server and no way in.
    """
    cfg = settings or get_settings()
    if cfg.auth_disabled:
        return None
    if not _STORE.empty:
        return None

    if cfg.bootstrap_api_key:
        _STORE.adopt(cfg.bootstrap_api_key)
        return None

    _, secret = _STORE.issue("bootstrap")
    _STORE.bootstrap_secret = secret
    return secret


def require_auth(
    authorization: str | None = Header(default=None),
    x_clara_api_key: str | None = Header(default=None),
) -> ApiKey | None:
    """FastAPI dependency enforcing authentication.

    Accepts ``Authorization: Bearer <key>`` or ``X-Clara-Api-Key: <key>``.
    """
    settings = get_settings()
    if settings.auth_disabled:
        return None

    secret = x_clara_api_key
    if not secret and authorization:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer":
            secret = value.strip()

    if not secret:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "unauthenticated",
                "message": "missing API key; send 'Authorization: Bearer <key>'",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

    key = _STORE.verify(secret)
    if key is None:
        log.warning("rejected API request with invalid key")
        raise HTTPException(
            status_code=401,
            detail={"code": "unauthenticated", "message": "invalid or revoked API key"},
        )
    return key
