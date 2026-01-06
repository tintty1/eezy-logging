"""Base queue interface for eezy-logging."""

from abc import ABC, abstractmethod
from typing import Any


class Queue(ABC):
    """Abstract base class for queue implementations.

    Queue implementations must be thread-safe. The `put` method should be
    non-blocking to avoid impacting application performance when logging.

    To implement a custom queue backend:
        1. Subclass this class
        2. Implement `put`, `get_batch`, `close`, and `size` methods
        3. Ensure thread-safety for concurrent access

    Example:
        class MyCustomQueue(Queue):
            def put(self, record: dict[str, Any]) -> None:
                # Push record to your queue backend
                ...

            def get_batch(self, max_size: int, timeout: float) -> list[dict[str, Any]]:
                # Retrieve up to max_size records, blocking up to timeout seconds
                ...

            def close(self) -> None:
                # Cleanup resources
                ...

            @property
            def size(self) -> int:
                # Return approximate queue size
                ...
    """

    @abstractmethod
    def put(self, record: dict[str, Any]) -> None:
        """Add a log record to the queue.

        This method should be non-blocking. If the queue is full, implementations
        should drop the oldest record and emit a warning.

        Args:
            record: Serialized log record as a dictionary.
        """

    @abstractmethod
    def get_batch(self, max_size: int, timeout: float) -> list[dict[str, Any]]:
        """Retrieve a batch of log records from the queue.

        This method may block up to `timeout` seconds waiting for records.
        Returns immediately if records are available.

        Args:
            max_size: Maximum number of records to retrieve.
            timeout: Maximum seconds to wait for records.

        Returns:
            List of log records (may be empty if timeout reached).
        """

    @abstractmethod
    def close(self) -> None:
        """Close the queue and release resources.

        Called during handler shutdown. Implementations should ensure
        any remaining records can still be retrieved before closing.
        """

    @property
    @abstractmethod
    def size(self) -> int:
        """Return the approximate number of records in the queue.

        This value may be approximate for distributed queue implementations.
        """
