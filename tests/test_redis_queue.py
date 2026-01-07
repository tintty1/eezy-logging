"""Tests for RedisQueue.

These tests require a running Redis instance.
Start it with: docker compose up -d

Run these tests with:
    pytest tests/test_redis_queue.py -v
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

# Check if Redis is available
REDIS_HOST = os.environ.get("EEZY_REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("EEZY_REDIS_PORT", 6379))


def is_redis_available() -> bool:
    """Check if Redis is available."""
    try:
        from redis import Redis

        client = Redis(host=REDIS_HOST, port=REDIS_PORT)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


redis_available = pytest.mark.skipif(
    not is_redis_available(),
    reason=f"Redis not available at {REDIS_HOST}:{REDIS_PORT}",
)


@pytest.fixture
def redis_client():
    """Create a Redis client for testing."""
    from redis import Redis

    client = Redis(host=REDIS_HOST, port=REDIS_PORT)
    yield client
    client.close()


@pytest.fixture
def unique_key() -> str:
    """Generate a unique Redis key for each test."""
    return f"eezy_test:{uuid.uuid4().hex[:8]}"


@pytest.fixture
def redis_queue(redis_client, unique_key):
    """Create a RedisQueue for testing with cleanup."""
    from eezy_logging.queues.redis import RedisQueue

    queue = RedisQueue(client=redis_client, key=unique_key, max_size=100)
    yield queue
    # Cleanup: delete the key after test
    redis_client.delete(unique_key)


@redis_available
class TestRedisQueue:
    """Tests for RedisQueue."""

    def test_put_and_get_single_record(self, redis_queue):
        """Test basic put and get operations."""
        record = {"message": "test", "level": "INFO"}

        redis_queue.put(record)
        assert redis_queue.size == 1

        records = redis_queue.get_batch(max_size=10, timeout=1.0)
        assert len(records) == 1
        assert records[0] == record
        assert redis_queue.size == 0

    def test_get_batch_respects_max_size(self, redis_queue):
        """Test that get_batch returns at most max_size records."""
        for i in range(50):
            redis_queue.put({"index": i})

        records = redis_queue.get_batch(max_size=10, timeout=1.0)
        assert len(records) == 10
        assert redis_queue.size == 40

    def test_get_batch_timeout_with_empty_queue(self, redis_client, unique_key):
        """Test that get_batch returns empty list after timeout on empty queue."""
        from eezy_logging.queues.redis import RedisQueue

        queue = RedisQueue(client=redis_client, key=unique_key, max_size=100)

        start = time.time()
        records = queue.get_batch(max_size=10, timeout=1)  # Redis BRPOP uses int seconds
        elapsed = time.time() - start

        assert records == []
        assert elapsed >= 1.0
        assert elapsed < 2.0  # Some tolerance

        redis_client.delete(unique_key)

    def test_trim_when_exceeds_max_size(self, redis_client, unique_key):
        """Test that oldest records are trimmed when queue exceeds max_size."""
        from eezy_logging.queues.redis import RedisQueue

        queue = RedisQueue(client=redis_client, key=unique_key, max_size=5)

        # Fill beyond max_size
        for i in range(7):
            queue.put({"index": i})

        # Queue should be trimmed to max_size
        assert queue.size == 5

        # Verify we have the newest records (FIFO order)
        records = queue.get_batch(max_size=10, timeout=1)
        indices = [r["index"] for r in records]
        # Oldest records (0, 1) should be trimmed
        assert indices == [2, 3, 4, 5, 6]

        redis_client.delete(unique_key)

    def test_close_prevents_further_puts(self, redis_queue):
        """Test that close prevents further puts."""
        redis_queue.put({"message": "before close"})
        redis_queue.close()
        redis_queue.put({"message": "after close"})

        # Only the first record should be in queue
        assert redis_queue.size == 1

    def test_thread_safety(self, redis_client, unique_key):
        """Test concurrent access from multiple threads."""
        from eezy_logging.queues.redis import RedisQueue

        queue = RedisQueue(client=redis_client, key=unique_key, max_size=1000)
        records_to_add = 50
        num_producers = 5

        def producer():
            for i in range(records_to_add):
                queue.put({"thread": threading.current_thread().name, "index": i})

        threads = [
            threading.Thread(target=producer, name=f"producer-{i}") for i in range(num_producers)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert queue.size == records_to_add * num_producers

        redis_client.delete(unique_key)

    def test_fifo_order_preserved(self, redis_queue):
        """Test that FIFO order is preserved."""
        for i in range(10):
            redis_queue.put({"index": i})

        records = redis_queue.get_batch(max_size=10, timeout=1)
        indices = [r["index"] for r in records]
        assert indices == list(range(10))

    def test_json_serialization(self, redis_queue):
        """Test that complex data structures are serialized correctly."""
        record = {
            "message": "test",
            "level": "INFO",
            "metadata": {
                "nested": {"key": "value"},
                "list": [1, 2, 3],
            },
            "count": 42,
            "enabled": True,
            "nullable": None,
        }

        redis_queue.put(record)
        records = redis_queue.get_batch(max_size=1, timeout=1)

        assert len(records) == 1
        assert records[0] == record


@redis_available
class TestRedisQueueEdgeCases:
    """Edge case tests for RedisQueue."""

    def test_get_batch_returns_available_records_immediately(self, redis_queue):
        """Test that get_batch returns immediately when records are available."""
        for i in range(5):
            redis_queue.put({"index": i})

        start = time.time()
        records = redis_queue.get_batch(max_size=10, timeout=5)
        elapsed = time.time() - start

        assert len(records) == 5
        assert elapsed < 1.0  # Should return quickly (network latency tolerance)

    def test_empty_queue_size(self, redis_queue):
        """Test size of empty queue."""
        assert redis_queue.size == 0

    def test_multiple_get_batches(self, redis_queue):
        """Test getting records in multiple batches."""
        for i in range(25):
            redis_queue.put({"index": i})

        batch1 = redis_queue.get_batch(max_size=10, timeout=1)
        batch2 = redis_queue.get_batch(max_size=10, timeout=1)
        batch3 = redis_queue.get_batch(max_size=10, timeout=1)

        assert len(batch1) == 10
        assert len(batch2) == 10
        assert len(batch3) == 5

        # Verify all indices are present and in order
        all_indices = [r["index"] for r in batch1 + batch2 + batch3]
        assert all_indices == list(range(25))

    def test_persistence_across_queue_instances(self, redis_client, unique_key):
        """Test that records persist across queue instances."""
        from eezy_logging.queues.redis import RedisQueue

        # Create first queue and add records
        queue1 = RedisQueue(client=redis_client, key=unique_key, max_size=100)
        queue1.put({"message": "from queue1"})

        # Create second queue with same key
        queue2 = RedisQueue(client=redis_client, key=unique_key, max_size=100)
        queue2.put({"message": "from queue2"})

        # Both records should be available
        assert queue2.size == 2

        records = queue2.get_batch(max_size=10, timeout=1)
        messages = [r["message"] for r in records]
        assert "from queue1" in messages
        assert "from queue2" in messages

        redis_client.delete(unique_key)


@redis_available
class TestRedisQueueIntegration:
    """Integration tests for RedisQueue with EezyHandler."""

    def test_full_handler_integration(self, redis_client, unique_key):
        """Test full integration with EezyHandler."""
        import logging

        from eezy_logging import EezyHandler
        from eezy_logging.queues.redis import RedisQueue
        from eezy_logging.sinks.base import Sink, WriteResult

        # Create a mock sink to capture records
        captured_records: list[dict] = []

        class CaptureSink(Sink):
            def setup(self) -> None:
                pass

            def write_batch(self, records: list[dict]) -> WriteResult:
                captured_records.extend(records)
                return WriteResult.ok()

            def close(self) -> None:
                pass

        queue = RedisQueue(client=redis_client, key=unique_key, max_size=100)
        sink = CaptureSink()
        handler = EezyHandler(sink=sink, queue=queue, batch_size=5, flush_interval=0.5)

        logger = logging.getLogger(f"test-redis-{unique_key}")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            # Log messages
            logger.info("Test message 1")
            logger.warning("Test message 2")
            logger.error("Test message 3")

            # Wait for processing
            handler.close()

            # Verify records were captured
            assert len(captured_records) == 3
            messages = [r["message"] for r in captured_records]
            assert "Test message 1" in messages
            assert "Test message 2" in messages
            assert "Test message 3" in messages

        finally:
            logger.removeHandler(handler)
            redis_client.delete(unique_key)

    def test_queue_survives_handler_restart(self, redis_client, unique_key):
        """Test that records in Redis survive handler restart."""

        from eezy_logging import EezyHandler
        from eezy_logging.queues.redis import RedisQueue
        from eezy_logging.sinks.base import Sink, WriteResult

        captured_records: list[dict] = []

        class SlowSink(Sink):
            """Sink that doesn't process records immediately."""

            def __init__(self):
                self.should_process = False

            def setup(self) -> None:
                pass

            def write_batch(self, records: list[dict]) -> WriteResult:
                if self.should_process:
                    captured_records.extend(records)
                return WriteResult.ok()

            def close(self) -> None:
                pass

        # First handler - add records but don't process them
        queue1 = RedisQueue(client=redis_client, key=unique_key, max_size=100)
        # Manually push records to Redis (simulating records left in queue)
        queue1.put({"message": "Orphaned message 1", "level": "INFO", "logger": "test"})
        queue1.put({"message": "Orphaned message 2", "level": "INFO", "logger": "test"})

        # Verify records are in Redis
        assert queue1.size == 2

        # Second handler picks up orphaned records
        queue2 = RedisQueue(client=redis_client, key=unique_key, max_size=100)
        sink = SlowSink()
        sink.should_process = True
        handler = EezyHandler(sink=sink, queue=queue2, batch_size=5, flush_interval=0.5)

        try:
            # Wait for processing
            time.sleep(1.0)
            handler.close()

            # Verify orphaned records were processed
            assert len(captured_records) == 2
            messages = [r["message"] for r in captured_records]
            assert "Orphaned message 1" in messages
            assert "Orphaned message 2" in messages

        finally:
            redis_client.delete(unique_key)
