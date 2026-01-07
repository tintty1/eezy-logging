"""Worker thread for consuming log records from queue and writing to sink."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eezy_logging.queues.base import Queue
    from eezy_logging.sinks.base import Sink

_logger = logging.getLogger("eezy_logging")


@dataclass
class _RetryBatch:
    """A batch of records scheduled for retry."""

    records: list[dict[str, Any]]
    retry_at: float  # time.monotonic() timestamp
    attempt: int = 1


@dataclass
class _RetryState:
    """Manages retry batches with non-blocking scheduling.

    Failed batches are stored with a retry timestamp. The worker checks
    for due retries on each loop iteration without blocking.
    """

    pending_retries: list[_RetryBatch] = field(default_factory=list)
    max_retries: int = 3
    base_delay: float = 1.0
    max_pending_batches: int = 1000  # Limit memory usage

    def schedule_retry(self, records: list[dict[str, Any]], attempt: int) -> bool:
        """Schedule records for retry.

        Returns:
            True if retry was scheduled, False if max retries exceeded or
            too many pending batches.
        """
        if attempt >= self.max_retries:
            _logger.error(
                "eezy-logging: Dropping %d records after %d failed attempts",
                len(records),
                attempt,
            )
            return False

        if len(self.pending_retries) >= self.max_pending_batches:
            _logger.warning(
                "eezy-logging: Retry queue full, dropping %d records",
                len(records),
            )
            return False

        # Exponential backoff: 1s, 2s, 4s, ...
        delay = self.base_delay * (2 ** (attempt - 1))
        retry_at = time.monotonic() + delay

        self.pending_retries.append(
            _RetryBatch(records=records, retry_at=retry_at, attempt=attempt)
        )
        _logger.debug(
            "eezy-logging: Scheduled retry for %d records (attempt %d) in %.1fs",
            len(records),
            attempt + 1,
            delay,
        )
        return True

    def get_due_retries(self) -> list[_RetryBatch]:
        """Get and remove all retry batches that are due now."""
        if not self.pending_retries:
            return []

        now = time.monotonic()
        due = []
        remaining = []

        for batch in self.pending_retries:
            if batch.retry_at <= now:
                due.append(batch)
            else:
                remaining.append(batch)

        self.pending_retries = remaining
        return due

    def time_until_next_retry(self) -> float | None:
        """Return seconds until next retry is due, or None if no retries pending."""
        if not self.pending_retries:
            return None

        now = time.monotonic()
        next_retry = min(batch.retry_at for batch in self.pending_retries)
        return max(0.0, next_retry - now)

    def drain_all(self) -> list[_RetryBatch]:
        """Return all pending retries for final flush during shutdown."""
        batches = self.pending_retries
        self.pending_retries = []
        return batches


class Worker:
    """Background worker thread for consuming log records from queue.

    The worker runs as a daemon thread, consuming batches of records from
    the queue and writing them to the sink. It uses configurable batch
    sizes and flush intervals for optimal performance.

    Failed writes are scheduled for retry without blocking the main
    consumption loop, ensuring new logs continue to be processed even
    when the sink is temporarily unavailable.

    Args:
        queue: Queue instance to consume records from.
        sink: Sink instance to write records to.
        batch_size: Maximum number of records per batch. Defaults to 100.
        flush_interval: Maximum seconds to wait before flushing a partial
            batch. Defaults to 5.0.
        max_retries: Maximum retry attempts for failed writes. Defaults to 3.
        retry_base_delay: Base delay for exponential backoff. Defaults to 1.0.

    Example:
        queue = InMemoryQueue()
        sink = ElasticsearchSink()
        worker = Worker(queue=queue, sink=sink, batch_size=200)
        worker.start()
        # ... use the queue ...
        worker.stop()
    """

    def __init__(
        self,
        queue: Queue,
        sink: Sink,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        max_retries: int = 3,
        retry_base_delay: float = 1.0,
    ) -> None:
        self._queue = queue
        self._sink = sink
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._retry_state = _RetryState(
            max_retries=max_retries,
            base_delay=retry_base_delay,
        )

    def start(self) -> None:
        """Start the worker thread.

        The worker will begin consuming records from the queue and writing
        them to the sink. The thread runs as a daemon so it won't prevent
        program exit.
        """
        if self._started:
            return

        # Run sink setup
        try:
            self._sink.setup()
        except Exception as e:
            _logger.error("eezy-logging: Sink setup failed: %s", e)

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="eezy-logging-worker")
        self._thread.start()
        self._started = True

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the worker thread and flush remaining records.

        Args:
            timeout: Maximum seconds to wait for the worker to stop.
        """
        if not self._started:
            return

        self._stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                _logger.warning("eezy-logging: Worker thread did not stop within timeout")

        self._started = False
        self._thread = None

    def _run(self) -> None:
        """Main worker loop."""
        while not self._stop_event.is_set():
            try:
                self._process_due_retries()

                # Calculate timeout: min of flush_interval and time to next retry
                timeout = self._flush_interval
                next_retry = self._retry_state.time_until_next_retry()
                if next_retry is not None:
                    timeout = min(timeout, next_retry)

                # Get a batch of records, blocking up to timeout
                records = self._queue.get_batch(
                    max_size=self._batch_size,
                    timeout=timeout,
                )

                if records:
                    self._write_batch(records, attempt=0)

            except Exception as e:
                _logger.error("eezy-logging: Worker error: %s", e)

        # Drain remaining records on shutdown
        self._drain_queue()

    def _process_due_retries(self) -> None:
        """Process any retry batches that are due."""
        due_batches = self._retry_state.get_due_retries()
        for batch in due_batches:
            self._write_batch(batch.records, attempt=batch.attempt)

    def _drain_queue(self) -> None:
        """Drain remaining records from queue on shutdown."""
        # First drain the queue
        while True:
            try:
                records = self._queue.get_batch(
                    max_size=self._batch_size,
                    timeout=0.1,
                )
                if not records:
                    break
                self._write_batch(records, attempt=0)
            except Exception as e:
                _logger.error("eezy-logging: Error draining queue: %s", e)
                break

        # Then attempt any pending retries one final time
        pending = self._retry_state.drain_all()
        for batch in pending:
            self._write_batch(batch.records, attempt=batch.attempt)

    def _write_batch(self, records: list[dict[str, Any]], attempt: int) -> None:
        """Write a batch of records to the sink."""
        try:
            result = self._sink.write_batch(records)

            if not result.success:
                for error in result.errors:
                    _logger.warning("eezy-logging: Write error: %s", error)
                self._retry_state.schedule_retry(result.failed_records, attempt=attempt + 1)

        except Exception as e:
            _logger.error(
                "eezy-logging: Failed to write batch of %d records: %s",
                len(records),
                e,
            )
            # Schedule retry for entire batch on unexpected exception
            self._retry_state.schedule_retry(records, attempt=attempt + 1)

    @property
    def is_alive(self) -> bool:
        """Return True if the worker thread is running."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def pending_retries(self) -> int:
        """Return the number of batches pending retry."""
        return len(self._retry_state.pending_retries)
