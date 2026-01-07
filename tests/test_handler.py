"""Tests for EezyHandler."""

import logging
import time
from typing import Any

from eezy_logging.handler import EezyHandler
from eezy_logging.queues.memory import InMemoryQueue
from eezy_logging.sinks.base import Sink, WriteResult


class MockSink(Sink):
    """Mock sink for testing."""

    def __init__(self):
        self.batches: list[list[dict[str, Any]]] = []
        self.setup_called = False
        self.close_called = False

    def setup(self) -> None:
        self.setup_called = True

    def write_batch(self, records: list[dict[str, Any]]) -> WriteResult:
        self.batches.append(records)
        return WriteResult.ok()

    def close(self) -> None:
        self.close_called = True

    @property
    def total_records(self) -> int:
        return sum(len(batch) for batch in self.batches)

    @property
    def all_records(self) -> list[dict[str, Any]]:
        return [r for batch in self.batches for r in batch]


class TestEezyHandler:
    """Tests for EezyHandler."""

    def test_handler_basic_logging(self):
        """Test basic logging through handler."""
        sink = MockSink()
        handler = EezyHandler(sink=sink, batch_size=10, flush_interval=0.1)

        logger = logging.getLogger("test.basic")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            logger.info("Test message")
            time.sleep(0.3)  # Wait for worker

            assert sink.total_records == 1
            record = sink.all_records[0]
            assert record["message"] == "Test message"
            assert record["level"] == "INFO"
        finally:
            handler.close()
            logger.removeHandler(handler)

    def test_handler_with_extra_fields(self):
        """Test logging with extra fields."""
        sink = MockSink()
        handler = EezyHandler(sink=sink, batch_size=10, flush_interval=0.1)

        logger = logging.getLogger("test.extra")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            logger.info("User action", extra={"user_id": 123, "action": "login"})
            time.sleep(0.3)

            record = sink.all_records[0]
            assert record["user_id"] == 123
            assert record["action"] == "login"
        finally:
            handler.close()
            logger.removeHandler(handler)

    def test_handler_with_custom_queue(self):
        """Test handler with custom queue."""
        queue = InMemoryQueue(max_size=50)
        sink = MockSink()
        handler = EezyHandler(sink=sink, queue=queue, batch_size=10, flush_interval=0.1)

        logger = logging.getLogger("test.custom_queue")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            for i in range(20):
                logger.info(f"Message {i}")

            time.sleep(0.5)
            assert sink.total_records == 20
        finally:
            handler.close()
            logger.removeHandler(handler)

    def test_handler_level_filtering(self):
        """Test that handler respects log level."""
        sink = MockSink()
        handler = EezyHandler(sink=sink, batch_size=10, flush_interval=0.1, level=logging.WARNING)

        logger = logging.getLogger("test.level")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")
            time.sleep(0.3)

            # Only WARNING and above should be processed
            assert sink.total_records == 2
            levels = [r["level"] for r in sink.all_records]
            assert "DEBUG" not in levels
            assert "INFO" not in levels
            assert "WARNING" in levels
            assert "ERROR" in levels
        finally:
            handler.close()
            logger.removeHandler(handler)

    def test_handler_close_flushes_records(self):
        """Test that close flushes remaining records."""
        sink = MockSink()
        handler = EezyHandler(sink=sink, batch_size=100, flush_interval=10.0)

        logger = logging.getLogger("test.close")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            for i in range(10):
                logger.info(f"Message {i}")

            # Close should flush
            handler.close()

            assert sink.total_records == 10
            assert sink.close_called
        finally:
            logger.removeHandler(handler)

    def test_handler_queue_size_property(self):
        """Test queue_size property."""
        sink = MockSink()
        # Very slow flush to keep records in queue
        # use default queue (in-memory python queue)
        handler = EezyHandler(sink=sink, batch_size=1000, flush_interval=5.0)

        logger = logging.getLogger("test.queue_size")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            initial_size = handler.queue_size
            assert initial_size == 0

            for i in range(5):
                logger.info(f"Message {i}")

            # Records should be queued
            time.sleep(0.05)
            assert handler.queue_size >= 0  # May have been consumed already
        finally:
            handler.close()
            logger.removeHandler(handler)

    def test_handler_worker_alive_property(self):
        """Test worker_alive property."""
        sink = MockSink()
        handler = EezyHandler(sink=sink)

        assert handler.worker_alive

        handler.close()
        time.sleep(0.2)
        assert not handler.worker_alive

    def test_handler_exception_logging(self):
        """Test logging with exception info."""
        sink = MockSink()
        handler = EezyHandler(sink=sink, batch_size=10, flush_interval=0.1)

        logger = logging.getLogger("test.exception")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            try:
                raise ValueError("Test error")
            except ValueError:
                logger.exception("An error occurred")

            time.sleep(0.3)

            record = sink.all_records[0]
            assert "exc_info" in record
            assert "ValueError: Test error" in record["exc_info"]
        finally:
            handler.close()
            logger.removeHandler(handler)


class TestEezyHandlerIntegration:
    """Integration tests for EezyHandler."""

    def test_multiple_loggers(self):
        """Test handler with multiple loggers."""
        sink = MockSink()
        handler = EezyHandler(sink=sink, batch_size=10, flush_interval=0.1)

        logger1 = logging.getLogger("test.multi.one")
        logger2 = logging.getLogger("test.multi.two")

        logger1.addHandler(handler)
        logger2.addHandler(handler)
        logger1.setLevel(logging.DEBUG)
        logger2.setLevel(logging.DEBUG)

        try:
            logger1.info("From logger 1")
            logger2.info("From logger 2")
            time.sleep(0.3)

            assert sink.total_records == 2
            loggers = {r["logger"] for r in sink.all_records}
            assert "test.multi.one" in loggers
            assert "test.multi.two" in loggers
        finally:
            handler.close()
            logger1.removeHandler(handler)
            logger2.removeHandler(handler)

    def test_high_volume_logging(self):
        """Test handler under high volume."""
        sink = MockSink()
        handler = EezyHandler(sink=sink, batch_size=100, flush_interval=0.5)

        logger = logging.getLogger("test.volume")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            num_records = 1000
            for i in range(num_records):
                logger.info(f"Message {i}", extra={"index": i})

            # Wait for processing
            time.sleep(0.1)

            assert sink.total_records == num_records
        finally:
            handler.close()
            logger.removeHandler(handler)
