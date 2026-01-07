"""Base sink interface for eezy-logging."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class WriteResult:
    """Result of a write_batch operation.

    Attributes:
        failed_records: List of records that failed to write and should be retried.
        errors: List of error messages for logging/debugging.
    """

    failed_records: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Return True if all records were written successfully."""
        return len(self.failed_records) == 0

    @classmethod
    def ok(cls) -> "WriteResult":
        """Create a successful result with no failures."""
        return cls()

    @classmethod
    def failure(cls, records: list[dict[str, Any]], error: str | None = None) -> "WriteResult":
        """Create a failure result with records to retry."""
        errors = [error] if error else []
        return cls(failed_records=records, errors=errors)


class Sink(ABC):
    """Abstract base class for sink implementations.

    Sinks are responsible for writing batches of log records to their
    destination (e.g., Elasticsearch, files, HTTP endpoints).

    To implement a custom sink:
        1. Subclass this class
        2. Implement `write_batch` method (required)
        3. Optionally override `setup` and `close` methods

    Example:
        class MySink(Sink):
            def __init__(self, endpoint: str):
                self.endpoint = endpoint

            def setup(self) -> None:
                # Optional: Initialize connections, create resources
                pass

            def write_batch(self, records: list[dict[str, Any]]) -> WriteResult:
                # Write records to destination
                try:
                    for record in records:
                        requests.post(self.endpoint, json=record)
                    return WriteResult.ok()
                except Exception as e:
                    return WriteResult.failure(records, str(e))

            def close(self) -> None:
                # Optional: Cleanup resources
                pass
    """

    def setup(self) -> None:
        """Initialize the sink and create any required resources.

        Called once before any records are written. Override to perform
        setup tasks like creating index templates, establishing connections, etc.

        The default implementation does nothing.
        """

    @abstractmethod
    def write_batch(self, records: list[dict[str, Any]]) -> WriteResult:
        """Write a batch of log records to the destination.

        This method is called by the worker thread with batches of records.
        Implementations should NOT block on retries - instead, return failed
        records in the WriteResult so the worker can schedule retries without
        blocking the main consumption loop.

        Args:
            records: List of serialized log records as dictionaries.

        Returns:
            WriteResult containing any failed records that should be retried.
        """

    def close(self) -> None:
        """Close the sink and release resources.

        Called during handler shutdown. Override to perform cleanup tasks
        like closing connections, flushing buffers, etc.

        The default implementation does nothing.
        """
