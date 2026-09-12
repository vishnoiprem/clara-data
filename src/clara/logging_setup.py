"""Structured logging.

Two formatters: a colour-coded console format for humans (using the CP AXTRA
palette) and line-delimited JSON for log shipping. Both attach any ``extra``
fields, so metering and run identifiers stay queryable downstream.

Caution: ``logging`` reserves the ``LogRecord`` attribute names listed in
``_STANDARD`` below. Passing one of them in ``extra`` (``message`` is the easy
mistake) makes ``Logger.makeRecord`` raise ``KeyError``, so a logging call
inside an error handler can turn a handled error into a crash. Use ``detail``
rather than ``message``.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from clara import branding

_CONFIGURED = False

#: Attributes present on every LogRecord; anything else is caller-supplied extra.
_STANDARD = frozenset(
    ["name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs", "relativeCreated", "thread", "threadName", "process", "processName", "taskName", "getMessage", "message", "asctime"]
)

_LEVEL_COLORS = {
    "DEBUG": branding.BLUE,
    "INFO": branding.GREEN,
    "WARNING": branding.YELLOW,
    "ERROR": branding.RED,
    "CRITICAL": branding.RED,
}


def _extras(record: logging.LogRecord) -> dict[str, Any]:
    return {k: v for k, v in record.__dict__.items() if k not in _STANDARD and not k.startswith("_")}


class JsonFormatter(logging.Formatter):
    """One JSON object per line, suitable for Loki/CloudWatch/OpenSearch."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_extras(record))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


class ConsoleFormatter(logging.Formatter):
    """Human-readable, colourised when stderr is a TTY."""

    def __init__(self, color: bool = True) -> None:
        super().__init__(datefmt="%H:%M:%S")
        self.color = color

    def format(self, record: logging.LogRecord) -> str:
        stamp = self.formatTime(record, self.datefmt)
        level = record.levelname
        if self.color:
            c = _LEVEL_COLORS.get(level, branding.ACCENT)
            level = f"\x1b[38;2;{c.r};{c.g};{c.b}m{level:<8}\x1b[0m"
        else:
            level = f"{level:<8}"

        line = f"{stamp} {level} {record.name:<28} {record.getMessage()}"
        extras = _extras(record)
        if extras:
            line += "  " + " ".join(f"{k}={v}" for k, v in extras.items())
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure_logging(level: str = "INFO", json_output: bool = False, force: bool = False) -> None:
    """Install Clara's root handler. Idempotent unless ``force``."""
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    handler = logging.StreamHandler(sys.stderr)
    if json_output:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(ConsoleFormatter(color=sys.stderr.isatty()))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())

    # Third-party engines are chatty at INFO; keep our own output legible.
    for noisy in (
        "botocore", "boto3", "urllib3", "s3transfer", "asyncio",
        "httpx", "httpcore", "trino", "trino.client", "pyiceberg",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Namespaced logger. Call sites use ``get_logger(__name__)``."""
    return logging.getLogger(name)