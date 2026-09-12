"""Prefixed, sortable identifiers.

Clara IDs look like ``wh_01HQ8Z3M4N5P6Q7R8S9T0V``: a short type prefix plus a
ULID-style body. The prefix makes IDs self-describing in logs and API payloads;
the time-ordered body means primary keys stay index-friendly.
"""

from __future__ import annotations

import os
import re
import secrets
import time

# Crockford base32: no I, L, O, U — unambiguous when read aloud or typed.
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ID_RE = re.compile(r"^(?P<prefix>[a-z]{2,6})_(?P<body>[0-9A-HJKMNP-TV-Z]{26})$")


class Prefix:
    """Canonical type prefixes. Add here, never inline at a call site."""

    TENANT = "ten"
    WORKSPACE = "ws"
    USER = "usr"
    API_KEY = "key"
    WAREHOUSE = "wh"
    QUERY = "qry"
    PIPELINE = "pl"
    JOB = "job"
    RUN = "run"
    TASK = "task"
    TABLE = "tbl"
    CONNECTION = "con"
    USAGE_EVENT = "ue"
    INVOICE = "inv"
    CREDIT_GRANT = "cg"
    SPEC = "spec"


def _encode(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(_ALPHABET[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def ulid(now_ms: int | None = None) -> str:
    """Generate a 26-character ULID: 48-bit millisecond timestamp + 80 random bits."""
    ts = now_ms if now_ms is not None else int(time.time() * 1000)
    rand = secrets.randbits(80)
    return _encode(ts, 10) + _encode(rand, 16)


def new_id(prefix: str) -> str:
    """Generate a prefixed identifier, e.g. ``new_id(Prefix.WAREHOUSE)``."""
    return f"{prefix}_{ulid()}"


def parse_id(value: str) -> tuple[str, str]:
    """Split a Clara ID into ``(prefix, body)``, raising ValueError if malformed."""
    match = _ID_RE.match(value)
    if not match:
        raise ValueError(f"not a Clara id: {value!r}")
    return match.group("prefix"), match.group("body")


def is_id(value: str, prefix: str | None = None) -> bool:
    """True if ``value`` is a Clara ID, optionally of a specific type."""
    match = _ID_RE.match(value or "")
    if not match:
        return False
    return prefix is None or match.group("prefix") == prefix


def id_timestamp_ms(value: str) -> int:
    """Recover the creation timestamp (ms since epoch) encoded in a Clara ID."""
    _, body = parse_id(value)
    ts = 0
    for char in body[:10]:
        ts = (ts << 5) | _ALPHABET.index(char)
    return ts


# --------------------------------------------------------------------- secrets


def new_api_key() -> tuple[str, str]:
    """Mint an API key.

    Returns ``(key_id, secret)``. Only the hash of the secret is ever stored, so
    this is the single moment the plaintext exists.
    """
    return new_id(Prefix.API_KEY), "clara_sk_" + secrets.token_urlsafe(32)


def slugify(value: str, max_length: int = 63) -> str:
    """Normalise a human name into an identifier safe for SQL and object keys."""
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    slug = re.sub(r"_{2,}", "_", slug)
    if not slug:
        slug = "x" + _encode(secrets.randbits(20), 4).lower()
    if slug[0].isdigit():
        slug = f"t_{slug}"
    return slug[:max_length].rstrip("_")


def random_suffix(n: int = 6) -> str:
    """Short random token for temp table / staging path names."""
    return os.urandom(n).hex()[:n]