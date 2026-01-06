"""Redis queue implementation for eezy-logging."""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any

from eezy_logging.queues.base import Queue

if TYPE_CHECKING:
    from redis import Redis

_logger = logging.getLogger("eezy_logging")

ENV_REDIS_HOST = "EEZY_REDIS_HOST"
ENV_REDIS_PORT = "EEZY_REDIS_PORT"
ENV_REDIS_PASSWORD = "EEZY_REDIS_PASSWORD"
ENV_REDIS_DB = "EEZY_REDIS_DB"

DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0


def _create_redis_client() -> Redis[bytes]:
    """Create a Redis client from environment variables or defaults."""
    try:
        from redis import Redis
    except ImportError as e:
        raise ImportError(
            "Redis client not installed. Install with: pip install eezy-logging[redis]"
        ) from e

    host = os.environ.get(ENV_REDIS_HOST, DEFAULT_REDIS_HOST)
    port = int(os.environ.get(ENV_REDIS_PORT, DEFAULT_REDIS_PORT))
    password = os.environ.get(ENV_REDIS_PASSWORD)
    db = int(os.environ.get(ENV_REDIS_DB, DEFAULT_REDIS_DB))

    return Redis(host=host, port=port, password=password, db=db)


class RedisQueue(Queue):
    """Redis-backed queue implementation using Redis LIST.

    Uses LPUSH to add records and BRPOP to retrieve them, ensuring
    FIFO ordering. Records are serialized to JSON for storage.

    This queue provides persistence across process restarts and can be
    shared across multiple processes/machines for distributed logging.

    Args:
        client: Redis client instance. If not provided, a client will be
            created using environment variables or defaults.
        key: Redis key for the list. Defaults to "eezy_logging:queue".
        max_size: Maximum number of records to hold. Defaults to 10000.
            When exceeded, oldest records are dropped.

    Environment Variables (used when client is not provided):
        EEZY_REDIS_HOST: Redis host (default: localhost)
        EEZY_REDIS_PORT: Redis port (default: 6379)
        EEZY_REDIS_PASSWORD: Redis password (default: None)
        EEZY_REDIS_DB: Redis database number (default: 0)

    Example:
        # Using environment variables or defaults
        queue = RedisQueue(key="myapp:logs")

        # Using custom client
        from redis import Redis
        client = Redis(host="redis.example.com", port=6379)
        queue = RedisQueue(client=client, key="myapp:logs")
    """

    def __init__(
        self,
        client: Redis[bytes] | None = None,
        key: str = "eezy_logging:queue",
        max_size: int = 10000,
    ) -> None:
        self._client = client or _create_redis_client()
        self._key = key
        self._max_size = max_size
        self._closed = False
        self._owns_client = client is None

    def put(self, record: dict[str, Any]) -> None:
        """Add a record to the Redis list."""
        if self._closed:
            return

        try:
            serialized = json.dumps(record)
            self._client.lpush(self._key, serialized)

            # Trim to max_size, keeping the newest records (left side)
            # LTRIM keeps elements from start to stop (inclusive)
            current_size = self._client.llen(self._key)
            if current_size > self._max_size:
                # Keep only the first max_size elements (newest)
                self._client.ltrim(self._key, 0, self._max_size - 1)
                _logger.warning(
                    "eezy-logging: Redis queue exceeded max size, trimmed to %d records.",
                    self._max_size,
                )
        except Exception as e:
            _logger.error("eezy-logging: Failed to push record to Redis: %s", e)

    def get_batch(self, max_size: int, timeout: float) -> list[dict[str, Any]]:
        """Retrieve up to max_size records from Redis."""
        records: list[dict[str, Any]] = []

        try:
            # First record with blocking wait
            result = self._client.brpop(self._key, timeout=int(timeout) or 1)
            if result is None:
                return records

            _, data = result
            records.append(json.loads(data))

            # Get remaining records without blocking
            while len(records) < max_size:
                data = self._client.rpop(self._key)
                if data is None:
                    break
                records.append(json.loads(data))

        except Exception as e:
            _logger.error("eezy-logging: Failed to retrieve records from Redis: %s", e)

        return records

    def close(self) -> None:
        """Close the queue and optionally the Redis client."""
        self._closed = True
        if self._owns_client:
            try:
                self._client.close()
            except Exception:
                pass

    @property
    def size(self) -> int:
        """Return the approximate number of records in the Redis list."""
        try:
            return self._client.llen(self._key)
        except Exception:
            return 0
