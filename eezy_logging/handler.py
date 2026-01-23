"""Main logging handler for eezy-logging."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable

from eezy_logging.queues.memory import InMemoryQueue
from eezy_logging.serializer import serialize_record
from eezy_logging.worker import Worker

if TYPE_CHECKING:
    from eezy_logging.queues.base import Queue
    from eezy_logging.sinks.base import Sink


class EezyHandler(logging.Handler):
    """Non-blocking logging handler that ships logs to configurable sinks.

    EezyHandler integrates with Python's standard logging module. It serializes
    log records and pushes them to a queue, where a background worker thread
    consumes them and writes to the configured sink.

    This design ensures that logging calls never block the main application
    thread, even when the sink is slow or temporarily unavailable.

    Args:
        sink: Sink instance to write records to.
        queue: Queue instance for buffering records. If not provided,
            an InMemoryQueue with default settings is created.
        batch_size: Maximum number of records per batch. Defaults to 100.
        flush_interval: Maximum seconds to wait before flushing a partial
            batch. Defaults to 5.0.
        level: Minimum log level to handle. Defaults to NOTSET (all levels).
        max_retries: Maximum retry attempts for failed writes. Defaults to 3.
        retry_base_delay: Base delay for exponential backoff. Defaults to 1.0.
        serializer: Custom function to serialize LogRecord to dict. If not
            provided, uses the default serialize_record function. The function
            should accept a logging.LogRecord and return a dict[str, Any].

    Note:
        When using InMemoryQueue, records in the queue are lost if the
        process crashes. For critical logs, use RedisQueue for persistence.

    Example:
        import logging
        from eezy_logging import EezyHandler
        from eezy_logging.sinks import ElasticsearchSink

        # Create handler with Elasticsearch sink
        sink = ElasticsearchSink(index_prefix="myapp-logs")
        handler = EezyHandler(sink=sink)

        # Attach to logger
        logger = logging.getLogger("myapp")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Log normally - non-blocking!
        logger.info("Application started", extra={"version": "1.0.0"})

        # Clean shutdown
        handler.close()
    """

    def __init__(
        self,
        sink: Sink,
        queue: Queue | None = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        level: int = logging.NOTSET,
        max_retries: int = 3,
        retry_base_delay: float = 1.0,
        serializer: Callable[[logging.LogRecord], dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(level=level)

        self._sink = sink
        self._queue = queue or InMemoryQueue()
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._serializer = serializer or serialize_record

        self._worker = Worker(
            queue=self._queue,
            sink=self._sink,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
        )
        self._worker.start()

    def emit(self, record: logging.LogRecord) -> None:
        """Serialize and queue the log record.

        This method is called by the logging framework for each log record.
        It serializes the record and pushes it to the queue without blocking.

        Args:
            record: The log record to handle.
        """
        try:
            data = self._serializer(record)
            self._queue.put(data)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        """Stop the worker and release resources.

        This method stops the background worker thread, allowing it to flush
        any remaining records to the sink before shutting down.

        Note:
            Users are responsible for calling this method during application
            shutdown. Consider registering it with atexit if needed.
        """
        try:
            self._worker.stop()
            self._queue.close()
            self._sink.close()
        finally:
            super().close()

    def flush(self) -> None:
        """Flush is a no-op since records are handled asynchronously.

        The background worker automatically flushes records based on
        batch_size and flush_interval settings.
        """
        # No-op - worker handles flushing

    @property
    def queue_size(self) -> int:
        """Return the current number of records in the queue."""
        return self._queue.size

    @property
    def worker_alive(self) -> bool:
        """Return True if the background worker is running."""
        return self._worker.is_alive
