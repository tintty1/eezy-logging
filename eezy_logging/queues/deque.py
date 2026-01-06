"""In-memory queue implementation using collections.deque."""

import logging
import threading
from collections import deque
from typing import Any

from eezy_logging.queues.base import Queue

_logger = logging.getLogger("eezy_logging")


class DequeQueue(Queue):
    """Thread-safe in-memory queue implementation using collections.deque.

    Uses `collections.deque` with `maxlen` internally. When the queue is full,
    the oldest record is automatically dropped by deque and a warning is logged
    periodically.

    This implementation uses a lock for thread safety and a condition variable
    for efficient blocking in `get_batch()`.

    Compared to `InMemoryQueue` (which uses `queue.Queue`):
        - Simpler overflow handling (deque auto-drops oldest)
        - Slightly more efficient under high load when queue is full
        - Requires explicit locking

    Note:
        Records in this queue are lost if the process crashes.
        For critical logs, consider using `RedisQueue` for persistence.

    Args:
        max_size: Maximum number of records to hold. Defaults to 10000.

    Example:
        queue = DequeQueue(max_size=5000)
        queue.put({"message": "Hello", "level": "INFO"})
        records = queue.get_batch(max_size=100, timeout=1.0)
    """

    def __init__(self, max_size: int = 10000) -> None:
        self._max_size = max_size
        self._deque: deque[dict[str, Any]] = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._closed = False
        self._drop_count = 0
        self._last_warning_count = 0

    def put(self, record: dict[str, Any]) -> None:
        """Add a record to the queue, dropping oldest if full.

        This method is non-blocking. If the queue is at capacity,
        the oldest record is automatically dropped by the underlying deque.
        """
        if self._closed:
            return

        with self._lock:
            # Check if we're at capacity (oldest will be dropped)
            if len(self._deque) >= self._max_size:
                self._drop_count += 1
                # Log warning every 100 drops to avoid spam
                if self._drop_count - self._last_warning_count >= 100:
                    _logger.warning(
                        "eezy-logging: Queue full, dropped %d records. "
                        "Consider increasing queue size or reducing log volume.",
                        self._drop_count,
                    )
                    self._last_warning_count = self._drop_count

            # deque with maxlen automatically drops oldest on append
            self._deque.append(record)
            self._not_empty.notify()

    def get_batch(self, max_size: int, timeout: float) -> list[dict[str, Any]]:
        """Retrieve up to max_size records, blocking up to timeout seconds."""
        records: list[dict[str, Any]] = []

        with self._not_empty:
            if not self._deque:
                self._not_empty.wait(timeout=timeout)

            while self._deque and len(records) < max_size:
                records.append(self._deque.popleft())

        return records

    def close(self) -> None:
        """Mark the queue as closed and wake up any waiting consumers."""
        with self._lock:
            self._closed = True
            self._not_empty.notify_all()

    @property
    def size(self) -> int:
        """Return the current number of records in the queue."""
        with self._lock:
            return len(self._deque)

    @property
    def drop_count(self) -> int:
        """Return the total number of records dropped due to queue overflow."""
        return self._drop_count
