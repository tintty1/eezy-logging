"""Tests for Worker thread."""

import time
from typing import Any

from eezy_logging.queues.memory import InMemoryQueue
from eezy_logging.sinks.base import Sink
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

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        if self.write_delay:
            time.sleep(self.write_delay)
        self.batches.append(records)

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

        def failing_write(records):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Simulated error")
            original_write(records)

        sink.write_batch = failing_write

        worker = Worker(queue=queue, sink=sink, batch_size=5, flush_interval=0.1)
        worker.start()

        # Add two batches
        for i in range(10):
            queue.put({"index": i})

        time.sleep(0.5)

        # Second batch should have been processed despite first error
        assert call_count >= 2
        worker.stop()
