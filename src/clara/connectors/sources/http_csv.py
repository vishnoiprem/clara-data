"""HTTP CSV / JSON source.

Covers the long tail that every company has: a file on an SFTP-replacement URL,
an export endpoint, a public dataset, a partner's nightly drop. Schema is
inferred, because these sources never declare one.
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any, Iterator

from clara.catalog.schema import Schema
from clara.connectors.base import CheckResult, ConnectorSpec, Source
from clara.connectors.protocol import (
    AirbyteMessage,
    ConfiguredStream,
    StreamDescriptor,
    SyncMode,
)
from clara.errors import ConnectorError, require
from clara.logging_setup import get_logger

log = get_logger(__name__)

#: Rows read for schema inference before the first record is emitted.
_INFER_ROWS = 200


class HttpFileSource(Source):
    """Reads a delimited or JSON file over HTTP(S)."""

    name = "http_file"

    @classmethod
    def spec(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name=cls.name,
            title="HTTP file (CSV / TSV / JSON / JSONL)",
            supports_incremental=False,
            secret_fields=("auth_token",),
            config_schema={
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": {"type": "string", "title": "URL"},
                    "format": {
                        "type": "string",
                        "title": "Format",
                        "default": "auto",
                        "enum": ["auto", "csv", "tsv", "json", "jsonl"],
                    },
                    "stream_name": {
                        "type": "string",
                        "title": "Destination table name",
                        "default": "http_file",
                    },
                    "delimiter": {"type": "string", "title": "Delimiter", "default": ","},
                    "encoding": {"type": "string", "title": "Encoding", "default": "utf-8"},
                    "auth_token": {
                        "type": "string",
                        "title": "Bearer token",
                        "airbyte_secret": True,
                    },
                    "headers": {"type": "object", "title": "Extra HTTP headers"},
                    # JSON APIs commonly wrap rows in an envelope.
                    "record_path": {
                        "type": "string",
                        "title": "JSON path to the record array",
                        "description": "Dotted path, e.g. 'data.items'. Leave blank for a top-level array.",
                    },
                },
            },
        )

    # ------------------------------------------------------------------ fetch

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": "clara-data/0.1"}
        headers.update(self.config.get("headers") or {})
        if token := self.config.get("auth_token"):
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _fetch_text(self) -> str:
        httpx = require("httpx", "", "the HTTP file source")
        url = self.config["url"]
        try:
            response = httpx.get(
                url, headers=self._headers(), timeout=120.0, follow_redirects=True
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(f"could not fetch {url}: {exc}", url=url) from exc
        response.encoding = self.config.get("encoding", "utf-8")
        return response.text

    def _resolved_format(self) -> str:
        declared = (self.config.get("format") or "auto").lower()
        if declared != "auto":
            return declared
        url = self.config["url"].split("?", 1)[0].lower()
        for suffix, name in ((".tsv", "tsv"), (".jsonl", "jsonl"), (".ndjson", "jsonl"), (".json", "json")):
            if url.endswith(suffix):
                return name
        return "csv"

    # ----------------------------------------------------------------- parsing

    def _parse(self, text: str) -> list[dict[str, Any]]:
        fmt = self._resolved_format()

        if fmt in ("csv", "tsv"):
            delimiter = "\t" if fmt == "tsv" else self.config.get("delimiter", ",")
            reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
            return [{k: _clean(v) for k, v in row.items() if k is not None} for row in reader]

        if fmt == "jsonl":
            return [json.loads(line) for line in text.splitlines() if line.strip()]

        payload = json.loads(text)
        if path := self.config.get("record_path"):
            for part in path.split("."):
                if not isinstance(payload, dict):
                    raise ConnectorError(f"record_path '{path}' does not match the response shape")
                payload = payload.get(part, [])
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            raise ConnectorError("JSON response did not contain a record array")
        return payload

    # ------------------------------------------------------------------ checks

    @property
    def _stream_name(self) -> str:
        from clara.ids import slugify

        return slugify(self.config.get("stream_name") or "http_file")

    def check(self) -> CheckResult:
        try:
            rows = self._parse(self._fetch_text())
        except Exception as exc:  # noqa: BLE001
            return CheckResult(succeeded=False, message=str(exc))
        if not rows:
            return CheckResult(succeeded=False, message="source returned no rows")
        return CheckResult(
            succeeded=True,
            message=f"{len(rows):,} rows, {len(rows[0])} columns",
            detected_streams=1,
        )

    def discover(self) -> list[StreamDescriptor]:
        rows = self._parse(self._fetch_text())[:_INFER_ROWS]
        schema = Schema.infer(rows)
        return [
            StreamDescriptor(
                name=self._stream_name,
                json_schema=_to_json_schema(schema),
                supported_sync_modes=[SyncMode.FULL_REFRESH],
            )
        ]

    def read(
        self,
        streams: list[ConfiguredStream],
        state: dict[str, Any] | None = None,
    ) -> Iterator[AirbyteMessage]:
        rows = self._parse(self._fetch_text())
        target = streams[0] if streams else None
        name = target.stream.name if target else self._stream_name

        yield AirbyteMessage.log_message("INFO", f"fetched {len(rows):,} rows from {self.config['url']}")
        for row in rows:
            yield AirbyteMessage.record_message(name, row)
        # Full refresh has no meaningful cursor, but emitting state keeps the
        # runner's checkpoint handling uniform across connectors.
        yield AirbyteMessage.state_message({name: {"rows": len(rows)}}, name)


def _clean(value: Any) -> Any:
    """Normalise CSV cells: empty strings become nulls, numbers become numbers.

    CSV has no types, and leaving everything as a string pushes the problem
    into every downstream query. Inferring here means a business user gets
    ``sum(amount)`` working without casting.
    """
    if value is None:
        return None
    text = value.strip() if isinstance(value, str) else value
    if text == "":
        return None
    if not isinstance(text, str):
        return text
    lowered = text.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered in ("null", "none", "n/a", "na"):
        return None
    try:
        if text.lstrip("-+").isdigit():
            return int(text)
        return float(text)
    except ValueError:
        return text


def _to_json_schema(schema: Schema) -> dict[str, Any]:
    """Render an inferred Clara schema as JSON Schema for the protocol."""
    from clara.catalog.schema import DataType

    mapping = {
        DataType.BOOLEAN: {"type": "boolean"},
        DataType.INT: {"type": "integer"},
        DataType.LONG: {"type": "integer"},
        DataType.FLOAT: {"type": "number"},
        DataType.DOUBLE: {"type": "number"},
        DataType.DECIMAL: {"type": "number", "airbyte_type": "big_number"},
        DataType.DATE: {"type": "string", "format": "date"},
        DataType.TIME: {"type": "string", "format": "time"},
        DataType.TIMESTAMP: {
            "type": "string",
            "format": "date-time",
            "airbyte_type": "timestamp_without_timezone",
        },
        DataType.TIMESTAMPTZ: {"type": "string", "format": "date-time"},
        DataType.BINARY: {"type": "string"},
        DataType.JSON: {"type": "object"},
        DataType.STRING: {"type": "string"},
    }
    return {
        "type": "object",
        "properties": {f.name: dict(mapping[f.type]) for f in schema.fields},
    }
