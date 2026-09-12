"""Object storage clients.

Two implementations behind one interface: ``S3ObjectStore`` for anything with an
S3 API, and ``LocalObjectStore`` for a filesystem. The local one exists so that
``clara init`` works with nothing installed and so the test suite needs no
network.
"""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from clara.errors import NotFoundError, ProviderError, require
from clara.logging_setup import get_logger
from clara.providers.base import ObjectStoreConfig

log = get_logger(__name__)


@dataclass
class ObjectInfo:
    """One stored object."""

    key: str
    size_bytes: int
    etag: str | None = None
    last_modified: Any | None = None


class ObjectStore(ABC):
    """Minimal object storage interface — the operations Clara actually uses."""

    @abstractmethod
    def put(self, key: str, data: bytes) -> ObjectInfo: ...

    @abstractmethod
    def get(self, key: str) -> bytes: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list(self, prefix: str = "") -> Iterator[ObjectInfo]: ...

    def total_size_bytes(self, prefix: str = "") -> int:
        """Bytes stored under a prefix. This is the storage billing input."""
        return sum(obj.size_bytes for obj in self.list(prefix))

    def upload_file(self, key: str, path: str | Path) -> ObjectInfo:
        return self.put(key, Path(path).read_bytes())

    def download_file(self, key: str, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.get(key))

    @abstractmethod
    def uri(self, key: str = "") -> str:
        """Fully-qualified URI for a key, as engines will address it."""

    def health_check(self) -> bool:
        """Round-trip a marker object to prove credentials and permissions."""
        probe = ".clara/_healthcheck"
        try:
            self.put(probe, b"ok")
            ok = self.get(probe) == b"ok"
            self.delete(probe)
            return ok
        except Exception as exc:  # noqa: BLE001 - health checks must not raise
            log.warning("object store health check failed", extra={"error": str(exc)})
            return False


class S3ObjectStore(ObjectStore):
    """S3-compatible storage: AWS, MinIO, Tencent COS, Alibaba OSS, R2, B2."""

    def __init__(self, config: ObjectStoreConfig) -> None:
        self.config = config
        self._client: Any | None = None

    @property
    def client(self) -> Any:
        if self._client is None:
            boto3 = require("boto3", "s3", "S3-compatible object storage")
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=self.config.endpoint,
                region_name=self.config.region,
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
                config=Config(
                    s3={"addressing_style": "path" if self.config.path_style else "virtual"},
                    # Cheap providers are less reliable than AWS; retry harder.
                    retries={"max_attempts": 5, "mode": "adaptive"},
                    signature_version="s3v4",
                ),
            )
        return self._client

    def _full_key(self, key: str) -> str:
        prefix = self.config.prefix.strip("/")
        cleaned = key.lstrip("/")
        return f"{prefix}/{cleaned}" if prefix else cleaned

    def put(self, key: str, data: bytes) -> ObjectInfo:
        try:
            response = self.client.put_object(
                Bucket=self.config.bucket, Key=self._full_key(key), Body=data
            )
        except Exception as exc:  # noqa: BLE001
            raise ProviderError(f"put failed for {key}: {exc}", key=key) from exc
        return ObjectInfo(key=key, size_bytes=len(data), etag=response.get("ETag"))

    def get(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.config.bucket, Key=self._full_key(key))
            return bytes(response["Body"].read())
        except self.client.exceptions.NoSuchKey:
            raise NotFoundError(f"object not found: {key}") from None
        except Exception as exc:  # noqa: BLE001
            raise ProviderError(f"get failed for {key}: {exc}", key=key) from exc

    def delete(self, key: str) -> None:
        try:
            self.client.delete_object(Bucket=self.config.bucket, Key=self._full_key(key))
        except Exception as exc:  # noqa: BLE001
            raise ProviderError(f"delete failed for {key}: {exc}", key=key) from exc

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.config.bucket, Key=self._full_key(key))
            return True
        except Exception:  # noqa: BLE001 - any failure means "not usable"
            return False

    def list(self, prefix: str = "") -> Iterator[ObjectInfo]:
        paginator = self.client.get_paginator("list_objects_v2")
        base = self._full_key(prefix)
        strip = len(self.config.prefix.strip("/")) + 1 if self.config.prefix.strip("/") else 0
        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=base):
            for item in page.get("Contents", []):
                yield ObjectInfo(
                    key=item["Key"][strip:],
                    size_bytes=item["Size"],
                    etag=item.get("ETag"),
                    last_modified=item.get("LastModified"),
                )

    def uri(self, key: str = "") -> str:
        return f"{self.config.scheme}://{self.config.bucket}/{self._full_key(key)}".rstrip("/")

    def ensure_bucket(self) -> None:
        """Create the bucket if absent. Used by ``clara up`` against MinIO."""
        try:
            self.client.head_bucket(Bucket=self.config.bucket)
        except Exception:  # noqa: BLE001 - head fails for both missing and denied
            try:
                self.client.create_bucket(Bucket=self.config.bucket)
                log.info("created bucket", extra={"bucket": self.config.bucket})
            except Exception as exc:  # noqa: BLE001
                raise ProviderError(
                    f"could not create bucket {self.config.bucket}: {exc}"
                ) from exc


class LocalObjectStore(ObjectStore):
    """Filesystem-backed store with object-store semantics."""

    def __init__(self, root: str | Path, config: ObjectStoreConfig | None = None) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.config = config

    def _path(self, key: str) -> Path:
        # Reject traversal: a connector-supplied key must not escape the root.
        target = (self.root / key.lstrip("/")).resolve()
        if not str(target).startswith(str(self.root)):
            raise ProviderError(f"key escapes storage root: {key}")
        return target

    def put(self, key: str, data: bytes) -> ObjectInfo:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return ObjectInfo(key=key, size_bytes=len(data))

    def get(self, key: str) -> bytes:
        path = self._path(key)
        if not path.is_file():
            raise NotFoundError(f"object not found: {key}")
        return path.read_bytes()

    def delete(self, key: str) -> None:
        path = self._path(key)
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list(self, prefix: str = "") -> Iterator[ObjectInfo]:
        base = self._path(prefix) if prefix else self.root
        if base.is_file():
            yield ObjectInfo(key=prefix, size_bytes=base.stat().st_size)
            return
        if not base.is_dir():
            return
        for path in sorted(base.rglob("*")):
            if path.is_file():
                yield ObjectInfo(
                    key=str(path.relative_to(self.root)),
                    size_bytes=path.stat().st_size,
                )

    def uri(self, key: str = "") -> str:
        return str(self._path(key)) if key else str(self.root)
