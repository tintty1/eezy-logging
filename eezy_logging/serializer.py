"""Log record serialization for eezy-logging."""

import logging
import socket
import traceback
from datetime import UTC, datetime
from typing import Any

_hostname: str | None = None


def _get_hostname() -> str:
    """Get the hostname, cached for performance."""
    global _hostname
    if _hostname is None:
        try:
            _hostname = socket.gethostname()
        except Exception:
            _hostname = "unknown"
    return _hostname


def serialize_record(record: logging.LogRecord) -> dict[str, Any]:
    """Serialize a LogRecord to a dictionary suitable for storage.

    Converts a Python logging.LogRecord to a dictionary with all relevant
    fields. Debugging context (line number, thread info, etc.) is grouped
    under a `metadata` field for cleaner document structure.

    Args:
        record: Python logging.LogRecord to serialize.

    Returns:
        Dictionary representation of the log record.

    Example:
        record = logging.LogRecord(...)
        data = serialize_record(record)
        # data = {
        #     "@timestamp": "...",
        #     "message": "...",
        #     "level": "INFO",
        #     "logger": "myapp",
        #     "metadata": {"lineno": 42, "funcName": "main", ...},
        #     ...
        # }
    """
    try:
        message = record.getMessage()
    except Exception:
        message = str(record.msg)

    metadata: dict[str, Any] = {
        "hostname": _get_hostname(),
        "levelno": record.levelno,
        "pathname": record.pathname,
        "filename": record.filename,
        "module": record.module,
        "funcName": record.funcName,
        "lineno": record.lineno,
        "process": record.process,
        "processName": record.processName,
        "thread": record.thread,
        "threadName": record.threadName,
    }

    data: dict[str, Any] = {
        "@timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
        "message": message,
        "level": record.levelname,
        "logger": record.name,
        "metadata": metadata,
    }

    if record.exc_info:
        data["exc_info"] = "".join(traceback.format_exception(*record.exc_info))

    if record.stack_info:
        data["stack_info"] = record.stack_info

    standard_attrs = {
        "name",
        "msg",
        "args",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "exc_info",
        "exc_text",
        "thread",
        "threadName",
        "message",
        "taskName",
    }

    # Add extra fields
    for key, value in record.__dict__.items():
        if key not in standard_attrs and not key.startswith("_"):
            # Try to serialize the value
            try:
                if isinstance(value, str | int | float | bool | type(None)):
                    data[key] = value
                elif isinstance(value, list | dict):
                    data[key] = value
                else:
                    data[key] = str(value)
            except Exception:
                data[key] = repr(value)

    return data
