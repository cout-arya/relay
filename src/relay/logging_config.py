"""Structured logging support for Relay.

When LOG_FORMAT=json, all log records are serialized to a JSON line containing:
  timestamp, level, logger, message, and any extra fields (job_id, worker_id,
  attempt, event) that handlers attach via the `extra` kwarg.

Usage in handlers/worker:
    logger.info("Job claimed", extra={"job_id": 42, "worker_id": "w1", "attempt": 1})
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    """Formats log records as newline-delimited JSON for structured log pipelines."""

    # Fields automatically extracted from LogRecord if present as extras
    EXTRA_FIELDS = ("job_id", "worker_id", "attempt", "event")

    def format(self, record: logging.LogRecord) -> str:
        obj: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=UTC
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include exception info if present
        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)

        # Include standard extra fields attached via logger.info(..., extra={...})
        for field in self.EXTRA_FIELDS:
            if hasattr(record, field):
                obj[field] = getattr(record, field)

        return json.dumps(obj, default=str)


def configure_logging(level: str = "info", log_format: str = "text") -> None:
    """Configure root logger with the appropriate formatter.

    Args:
        level: Log level string (debug, info, warning, error, critical).
        log_format: "text" for human-readable logs, "json" for structured JSON.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    if log_format.lower() == "json":
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(log_level)
    # Clear any existing handlers to avoid duplicate output
    root.handlers.clear()
    root.addHandler(handler)

