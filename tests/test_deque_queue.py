"""Tests for DequeQueue."""

import threading
import time

from eezy_logging.queues.deque import DequeQueue


class TestDequeQueue:
    """Tests for DequeQueue."""

    def test_put_and_get_single_record(self):
        """Test basic put and get operations."""
        queue = DequeQueue(max_size=100)
        record = {"message": "test", "level": "INFO"}

        queue.put(record)
        assert queue.size == 1

        records = queue.get_batch(max_size=10, timeout=1.0)
        assert len(records) == 1
        assert records[0] == record
        assert queue.size == 0

    def test_get_batch_respects_max_size(self):
        """Test that get_batch returns at most max_size records."""
        queue = DequeQueue(max_size=100)

        for i in range(50):
            queue.put({"index": i})

        records = queue.get_batch(max_size=10, timeout=1.0)
        assert len(records) == 10
        assert queue.size == 40

    def test_get_batch_timeout_with_empty_queue(self):
        """Test that get_batch returns empty list after timeout on empty queue."""
        queue = DequeQueue(max_size=100)

        start = time.time()
        records = queue.get_batch(max_size=10, timeout=0.1)
        elapsed = time.time() - start

        assert records == []
        assert elapsed >= 0.1
        assert elapsed < 0.3  # Some tolerance

    def test_drop_oldest_when_full(self):
        """Test that oldest records are dropped when queue is full."""
        queue = DequeQueue(max_size=5)

        # Fill the queue
        for i in range(5):
            queue.put({"index": i})

        assert queue.size == 5

        # Add more - should drop oldest automatically
        queue.put({"index": 5})
        queue.put({"index": 6})

        assert queue.size == 5
        assert queue.drop_count == 2

        # Verify we have the newest records
        records = queue.get_batch(max_size=10, timeout=0.1)
        indices = [r["index"] for r in records]
        # Oldest records (0, 1) should be dropped
        assert indices == [2, 3, 4, 5, 6]

    def test_close_prevents_further_puts(self):
        """Test that close prevents further puts."""
        queue = DequeQueue(max_size=100)
        queue.put({"message": "before close"})
        queue.close()
        queue.put({"message": "after close"})

        # Only the first record should be in queue
        assert queue.size == 1

    def test_thread_safety(self):
        """Test concurrent access from multiple threads."""
        queue = DequeQueue(max_size=1000)
        records_to_add = 100
        num_producers = 5

        def producer():
            for i in range(records_to_add):
                queue.put({"thread": threading.current_thread().name, "index": i})

        threads = [
            threading.Thread(target=producer, name=f"producer-{i}")
            for i in range(num_producers)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert queue.size == records_to_add * num_producers


class TestDequeQueueEdgeCases:
    """Edge case tests for DequeQueue."""

    def test_get_batch_returns_available_records_immediately(self):
        """Test that get_batch returns immediately when records are available."""
        queue = DequeQueue(max_size=100)

        for i in range(5):
            queue.put({"index": i})

        start = time.time()
        records = queue.get_batch(max_size=10, timeout=5.0)
        elapsed = time.time() - start

        assert len(records) == 5
        assert elapsed < 0.1  # Should return immediately

    def test_empty_queue_size(self):
        """Test size of empty queue."""
        queue = DequeQueue(max_size=100)
        assert queue.size == 0

    def test_drop_count_initial_value(self):
        """Test drop_count starts at zero."""
        queue = DequeQueue(max_size=100)
        assert queue.drop_count == 0

    def test_fifo_order_preserved(self):
        """Test that FIFO order is preserved."""
        queue = DequeQueue(max_size=100)

        for i in range(10):
            queue.put({"index": i})

        records = queue.get_batch(max_size=10, timeout=0.1)
        indices = [r["index"] for r in records]
        assert indices == list(range(10))

    def test_close_wakes_up_waiting_consumers(self):
        """Test that close wakes up consumers waiting on get_batch."""
        queue = DequeQueue(max_size=100)
        result = []

        def consumer():
            records = queue.get_batch(max_size=10, timeout=10.0)
            result.append(len(records))

        thread = threading.Thread(target=consumer)
        thread.start()

        time.sleep(0.1)  # Let consumer start waiting
        queue.close()
        thread.join(timeout=1.0)

        assert not thread.is_alive()
        assert result == [0]  # Should return empty list
