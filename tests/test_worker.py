"""Tests for Worker thread."""

import time
from typing import Any

from eezy_logging.queues.memory import InMemoryQueue
from eezy_logging.sinks.base import Sink, WriteResult
from eezy_logging.worker import Worker


class MockSink(Sink):
    """Mock sink for testing."""

    def __init__(self):
        self.batches: list[list[dict[str, Any]]] = []
        self.setup_called = False
        self.close_called = False
        self.write_delay = 0.0

    def setup(self) -> None:
        self.setup_called = True

    def write_batch(self, records: list[dict[str, Any]]) -> WriteResult:
        if self.write_delay:
            time.sleep(self.write_delay)
        self.batches.append(records)
        return WriteResult.ok()

    def close(self) -> None:
        self.close_called = True

    @property
    def total_records(self) -> int:
        return sum(len(batch) for batch in self.batches)


class TestWorker:
    """Tests for Worker class."""

    def test_start_calls_sink_setup(self):
        """Test that starting worker calls sink setup."""
        queue = InMemoryQueue()
        sink = MockSink()
        worker = Worker(queue=queue, sink=sink)

        worker.start()
        time.sleep(0.1)

        assert sink.setup_called
        worker.stop()

    def test_worker_processes_records(self):
        """Test that worker processes records from queue."""
        queue = InMemoryQueue()
        sink = MockSink()
        worker = Worker(queue=queue, sink=sink, batch_size=10, flush_interval=0.1)

        worker.start()

        # Add records
        for i in range(5):
            queue.put({"index": i})

        # Wait for processing
        time.sleep(0.3)

        assert sink.total_records == 5
        worker.stop()

    def test_worker_batches_records(self):
        """Test that worker respects batch size."""
        queue = InMemoryQueue()
        sink = MockSink()
        sink.write_delay = 0.05  # Slow sink to accumulate records
        worker = Worker(queue=queue, sink=sink, batch_size=5, flush_interval=0.5)

        worker.start()

        # Add records quickly
        for i in range(15):
            queue.put({"index": i})

        # Wait for processing
        time.sleep(0.5)

        # Check that batches were created with appropriate sizes
        assert sink.total_records == 15
        # At least some batches should have multiple records
        assert any(len(batch) > 1 for batch in sink.batches)

        worker.stop()

    def test_worker_drains_on_stop(self):
        """Test that worker drains queue on stop."""
        queue = InMemoryQueue()
        sink = MockSink()
        worker = Worker(queue=queue, sink=sink, batch_size=100, flush_interval=10.0)

        worker.start()

        # Add records
        for i in range(10):
            queue.put({"index": i})

        # Stop immediately - should still process remaining
        worker.stop(timeout=5.0)

        assert sink.total_records == 10
        assert queue.size == 0

    def test_worker_is_daemon_thread(self):
        """Test that worker thread is a daemon."""
        queue = InMemoryQueue()
        sink = MockSink()
        worker = Worker(queue=queue, sink=sink)

        worker.start()
        assert worker._thread is not None
        assert worker._thread.daemon is True
        worker.stop()

    def test_worker_is_alive_property(self):
        """Test is_alive property."""
        queue = InMemoryQueue()
        sink = MockSink()
        worker = Worker(queue=queue, sink=sink)

        assert not worker.is_alive

        worker.start()
        assert worker.is_alive

        worker.stop()
        time.sleep(0.1)
        assert not worker.is_alive

    def test_start_is_idempotent(self):
        """Test that calling start multiple times is safe."""
        queue = InMemoryQueue()
        sink = MockSink()
        worker = Worker(queue=queue, sink=sink)

        worker.start()
        thread1 = worker._thread

        worker.start()  # Second call
        thread2 = worker._thread

        assert thread1 is thread2  # Same thread
        worker.stop()

    def test_stop_is_idempotent(self):
        """Test that calling stop multiple times is safe."""
        queue = InMemoryQueue()
        sink = MockSink()
        worker = Worker(queue=queue, sink=sink)

        worker.start()
        worker.stop()
        worker.stop()  # Second call should not raise


class TestWorkerErrorHandling:
    """Tests for Worker error handling."""

    def test_worker_continues_after_sink_error(self):
        """Test that worker continues after sink write error."""
        queue = InMemoryQueue()
        sink = MockSink()
        call_count = 0

        original_write = sink.write_batch

        def failing_write(records: list[dict[str, Any]]) -> WriteResult:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Simulated error")
            return original_write(records)

        sink.write_batch = failing_write  # type: ignore[method-assign]

        worker = Worker(queue=queue, sink=sink, batch_size=5, flush_interval=0.1)
        worker.start()

        # Add two batches
        for i in range(10):
            queue.put({"index": i})

        time.sleep(0.5)

        # Second batch should have been processed despite first error
        assert call_count >= 2
        worker.stop()


class TestWorkerRetry:
    """Tests for Worker non-blocking retry behavior."""

    def test_worker_retries_failed_records(self):
        """Test that worker retries records returned as failed."""
        queue = InMemoryQueue()
        sink = MockSink()
        attempt_count = 0

        def failing_then_success(records: list[dict[str, Any]]) -> WriteResult:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                # First attempt fails
                return WriteResult.failure(records, "Temporary error")
            # Subsequent attempts succeed
            sink.batches.append(records)
            return WriteResult.ok()

        sink.write_batch = failing_then_success  # type: ignore[method-assign]

        worker = Worker(
            queue=queue,
            sink=sink,
            batch_size=5,
            flush_interval=0.1,
            retry_base_delay=0.1,  # Fast retries for testing
        )
        worker.start()

        # Add records
        for i in range(5):
            queue.put({"index": i})

        # Wait for initial attempt + retry
        time.sleep(0.5)

        worker.stop()

        # Records should have been written after retry
        assert sink.total_records == 5
        assert attempt_count == 2

    def test_worker_drops_records_after_max_retries(self):
        """Test that records are dropped after max retries exceeded."""
        queue = InMemoryQueue()
        sink = MockSink()
        attempt_count = 0

        def always_fail(records: list[dict[str, Any]]) -> WriteResult:
            nonlocal attempt_count
            attempt_count += 1
            return WriteResult.failure(records, "Persistent error")

        sink.write_batch = always_fail  # type: ignore[method-assign]

        worker = Worker(
            queue=queue,
            sink=sink,
            batch_size=5,
            flush_interval=0.1,
            max_retries=3,
            retry_base_delay=0.05,  # Fast retries for testing
        )
        worker.start()

        # Add records
        for i in range(5):
            queue.put({"index": i})

        # Wait for all retry attempts (0.05 + 0.1 + 0.2 = 0.35s plus processing)
        time.sleep(1.0)

        worker.stop()

        # Should have attempted max_retries times (initial + 2 retries = 3)
        assert attempt_count == 3
        # No records should be written since all attempts failed
        assert sink.total_records == 0

    def test_worker_continues_processing_during_retry_delay(self):
        """Test that worker continues consuming new records during retry delays."""
        queue = InMemoryQueue()
        sink = MockSink()
        first_batch_failed = False
        batches_received: list[list[dict[str, Any]]] = []

        def selective_fail(records: list[dict[str, Any]]) -> WriteResult:
            nonlocal first_batch_failed
            batches_received.append(list(records))

            # Fail the first batch once
            if not first_batch_failed and any(r.get("batch") == 1 for r in records):
                first_batch_failed = True
                return WriteResult.failure(records, "Temporary error")

            sink.batches.append(records)
            return WriteResult.ok()

        sink.write_batch = selective_fail  # type: ignore[method-assign]

        worker = Worker(
            queue=queue,
            sink=sink,
            batch_size=5,
            flush_interval=0.1,
            retry_base_delay=0.2,  # Longer delay to ensure second batch processes first
        )
        worker.start()

        # Add first batch
        for i in range(5):
            queue.put({"batch": 1, "index": i})

        time.sleep(0.15)  # Wait for first batch to be attempted

        # Add second batch while first is waiting for retry
        for i in range(5):
            queue.put({"batch": 2, "index": i})

        # Wait for processing
        time.sleep(0.5)

        worker.stop()

        # Both batches should eventually be written
        assert sink.total_records == 10

    def test_pending_retries_property(self):
        """Test that pending_retries property tracks retry queue."""
        queue = InMemoryQueue()
        sink = MockSink()

        def always_fail(records: list[dict[str, Any]]) -> WriteResult:
            return WriteResult.failure(records, "Error")

        sink.write_batch = always_fail  # type: ignore[method-assign]

        worker = Worker(
            queue=queue,
            sink=sink,
            batch_size=5,
            flush_interval=0.1,
            max_retries=5,
            retry_base_delay=10.0,  # Long delay so retries stay pending
        )
        worker.start()

        assert worker.pending_retries == 0

        # Add records
        for i in range(5):
            queue.put({"index": i})

        # Wait for first attempt to fail
        time.sleep(0.3)

        # Should have pending retry
        assert worker.pending_retries >= 1

        worker.stop()
