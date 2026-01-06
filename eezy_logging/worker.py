"""Worker thread for consuming log records from queue and writing to sink."""

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eezy_logging.queues.base import Queue
    from eezy_logging.sinks.base import Sink

_logger = logging.getLogger("eezy_logging")


class Worker:
    """Background worker thread for consuming log records from queue.

    The worker runs as a daemon thread, consuming batches of records from
    the queue and writing them to the sink. It uses configurable batch
    sizes and flush intervals for optimal performance.

    Args:
        queue: Queue instance to consume records from.
        sink: Sink instance to write records to.
        batch_size: Maximum number of records per batch. Defaults to 100.
        flush_interval: Maximum seconds to wait before flushing a partial
            batch. Defaults to 5.0.

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
        queue: "Queue",
        sink: "Sink",
        batch_size: int = 100,
        flush_interval: float = 5.0,
    ) -> None:
        self._queue = queue
        self._sink = sink
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False

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
                # Get a batch of records, blocking up to flush_interval
                records = self._queue.get_batch(
                    max_size=self._batch_size,
                    timeout=self._flush_interval,
                )

                if records:
                    self._write_batch(records)

            except Exception as e:
                _logger.error("eezy-logging: Worker error: %s", e)

        # Drain remaining records on shutdown
        self._drain_queue()

    def _drain_queue(self) -> None:
        """Drain remaining records from queue on shutdown."""
        while True:
            try:
                records = self._queue.get_batch(
                    max_size=self._batch_size,
                    timeout=0.1,
                )
                if not records:
                    break
                self._write_batch(records)
            except Exception as e:
                _logger.error("eezy-logging: Error draining queue: %s", e)
                break

    def _write_batch(self, records: list[dict]) -> None:
        """Write a batch of records to the sink."""
        try:
            self._sink.write_batch(records)
        except Exception as e:
            _logger.error(
                "eezy-logging: Failed to write batch of %d records: %s",
                len(records),
                e,
            )

    @property
    def is_alive(self) -> bool:
        """Return True if the worker thread is running."""
        return self._thread is not None and self._thread.is_alive()
