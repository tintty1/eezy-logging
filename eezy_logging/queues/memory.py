"""In-memory queue implementation using Python's queue.Queue."""

import logging
import queue
import threading
from typing import Any

from eezy_logging.queues.base import Queue

_logger = logging.getLogger("eezy_logging")


class InMemoryQueue(Queue):
    """Thread-safe in-memory queue implementation using queue.Queue.

    Uses Python's `queue.Queue` internally which provides built-in thread safety.
    When the queue is full, the oldest record is dropped and a warning is logged.

    This is the default queue implementation. For an alternative using
    `collections.deque`, see `DequeQueue`.

    Note:
        Records in this queue are lost if the process crashes.
        For critical logs, consider using `RedisQueue` for persistence.

    Args:
        max_size: Maximum number of records to hold. Defaults to 10000.

    Example:
        queue = InMemoryQueue(max_size=5000)
        queue.put({"message": "Hello", "level": "INFO"})
        records = queue.get_batch(max_size=100, timeout=1.0)
    """

    def __init__(self, max_size: int = 10000) -> None:
        self._max_size = max_size
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._closed = False
        self._drop_count = 0
        self._last_warning_count = 0

    def put(self, record: dict[str, Any]) -> None:
        """Add a record to the queue, dropping oldest if full."""
        if self._closed:
            return

        try:
            self._queue.put_nowait(record)
        except queue.Full:
            # Queue is full, drop the oldest record
            with self._lock:
                try:
                    self._queue.get_nowait()
                    self._drop_count += 1
                    # Log warning every 100 drops to avoid spam
                    if self._drop_count - self._last_warning_count >= 100:
                        _logger.warning(
                            "eezy-logging: Queue full, dropped %d records. "
                            "Consider increasing queue size or reducing log volume.",
                            self._drop_count,
                        )
                        self._last_warning_count = self._drop_count
                except queue.Empty:
                    pass

            try:
                self._queue.put_nowait(record)
            except queue.Full:
                # Still full (race condition), just drop
                pass

    def get_batch(self, max_size: int, timeout: float) -> list[dict[str, Any]]:
        """Retrieve up to max_size records, blocking up to timeout seconds."""
        records: list[dict[str, Any]] = []

        # First, try to get one record with the full timeout
        try:
            record = self._queue.get(timeout=timeout)
            records.append(record)
        except queue.Empty:
            return records

        # Then get remaining records without blocking
        while len(records) < max_size:
            try:
                record = self._queue.get_nowait()
                records.append(record)
            except queue.Empty:
                break

        return records

    def close(self) -> None:
        """Mark the queue as closed."""
        self._closed = True

    @property
    def size(self) -> int:
        """Return the current number of records in the queue."""
        return self._queue.qsize()

    @property
    def drop_count(self) -> int:
        """Return the total number of records dropped due to queue overflow."""
        return self._drop_count
