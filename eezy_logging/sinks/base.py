"""Base sink interface for eezy-logging."""

from abc import ABC, abstractmethod
from typing import Any


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

            def write_batch(self, records: list[dict[str, Any]]) -> None:
                # Write records to destination
                for record in records:
                    requests.post(self.endpoint, json=record)

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
    def write_batch(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of log records to the destination.

        This method is called by the worker thread with batches of records.
        Implementations should handle errors appropriately (retry, log, etc.).

        Args:
            records: List of serialized log records as dictionaries.

        Raises:
            Exception: If the write fails after any retry attempts.
        """

    def close(self) -> None:
        """Close the sink and release resources.

        Called during handler shutdown. Override to perform cleanup tasks
        like closing connections, flushing buffers, etc.

        The default implementation does nothing.
        """
