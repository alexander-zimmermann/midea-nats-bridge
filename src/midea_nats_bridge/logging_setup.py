"""Root-logger configuration: structured JSON to stdout by default."""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import UTC, datetime
from typing import TextIO

from .config import LogFormat


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(timespec="microseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key in {
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "message",
                "module",
                "msecs",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
                "taskName",
            }:
                continue
            payload[key] = value
        return json.dumps(payload, default=str, ensure_ascii=False)


class TrackedStreamHandler(logging.StreamHandler[TextIO]):
    """StreamHandler that exposes emit failures so a wedged stdout becomes observable.

    StreamHandler.emit() routes its own exceptions through handleError(), which by
    default writes to stderr and returns silently. Without this override, a broken
    stdout pipe would leave the process log-dead with no signal to the liveness
    probe. The class-level counter and timestamp let downstream code (Prometheus
    gauges, liveness watchdogs) read failure state without holding a handler ref.
    """

    emit_errors_total: int = 0
    last_emit_ok_ts: float = 0.0

    def emit(self, record: logging.LogRecord) -> None:
        pre = type(self).emit_errors_total
        super().emit(record)
        # handleError() bumps the counter on failure; reaching here with the
        # counter unchanged means the write went through.
        if type(self).emit_errors_total == pre:
            type(self).last_emit_ok_ts = time.monotonic()

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802 (stdlib override)
        type(self).emit_errors_total += 1
        super().handleError(record)


def configure(level: str, fmt: LogFormat) -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = TrackedStreamHandler(sys.stdout)
    if fmt is LogFormat.JSON:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(handler)
    # Seed the watchdog timestamp so a process that hasn't logged anything yet
    # still looks healthy to the liveness probe.
    TrackedStreamHandler.last_emit_ok_ts = time.monotonic()
