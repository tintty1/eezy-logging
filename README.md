# eezy-logging

A non-blocking, queue-based Python logging library for shipping logs to Elasticsearch and other destinations.

## Features

- **Non-blocking**: Logs are queued and processed in a background thread, never blocking your application
- **Queue-based**: Choose between in-memory queue or Redis for persistence
- **Batching**: Logs are batched for efficient bulk writes
- **Extensible**: Easy to create custom sinks and queues
- **Elasticsearch support**: Built-in sink with ILM (Index Lifecycle Management) policies
- **OpenSearch support**: Built-in sink with ISM (Index State Management) policies

## Installation

```bash
pip install eezy-logging
```

With optional dependencies:

```bash
# For Elasticsearch support
pip install eezy-logging[elasticsearch]

# For OpenSearch support
pip install eezy-logging[opensearch]

# For Redis queue support
pip install eezy-logging[redis]

# All optional dependencies
pip install eezy-logging[all]
```

## Quick Start

```python
import logging
from eezy_logging import EezyHandler
from eezy_logging.sinks import ElasticsearchSink

# Create sink and handler
sink = ElasticsearchSink(index_prefix="myapp-logs")
handler = EezyHandler(sink=sink)

# Attach to logger
logger = logging.getLogger("myapp")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Log normally - non-blocking!
logger.info("Application started", extra={"version": "1.0.0"})
logger.info("User logged in", extra={"user_id": 123, "ip": "192.168.1.1"})

# Clean shutdown (flushes remaining logs)
handler.close()
```

## Configuration

### EezyHandler

```python
from eezy_logging import EezyHandler

handler = EezyHandler(
    sink=sink,              # Required: where to send logs
    queue=None,             # Optional: queue backend (default: InMemoryQueue)
    batch_size=100,         # Max records per batch
    flush_interval=5.0,     # Max seconds before flushing partial batch
    level=logging.NOTSET,   # Minimum log level
    max_retries=3,          # Max retry attempts for failed writes
    retry_base_delay=1.0,   # Base delay for exponential backoff (seconds)
)
```

> **Note**: Retries use exponential backoff (1s, 2s, 4s, ...) and are non-blocking - the worker continues consuming new logs while waiting to retry failed batches.

### Queue Backends

#### InMemoryQueue (default)

Uses Python's `queue.Queue`. Simple and efficient for most use cases.

```python
from eezy_logging import InMemoryQueue

queue = InMemoryQueue(max_size=10000)
handler = EezyHandler(sink=sink, queue=queue)
```

> **Note**: Records are lost if the process crashes. For critical logs, use `RedisQueue`.

#### DequeQueue

Uses `collections.deque`. Slightly more efficient under high load when queue is frequently full.

```python
from eezy_logging import DequeQueue

queue = DequeQueue(max_size=10000)
handler = EezyHandler(sink=sink, queue=queue)
```

#### RedisQueue

Uses Redis LIST for persistence. Logs survive process restarts.

```python
from eezy_logging import RedisQueue

# Using environment variables or defaults
queue = RedisQueue(key="myapp:logs")

# Using custom client
from redis import Redis
client = Redis(host="redis.example.com", port=6379, password="secret")
queue = RedisQueue(client=client, key="myapp:logs", max_size=50000)

handler = EezyHandler(sink=sink, queue=queue)
```

**Environment variables** (used when client is not provided):
- `EEZY_REDIS_HOST` (default: `localhost`)
- `EEZY_REDIS_PORT` (default: `6379`)
- `EEZY_REDIS_PASSWORD` (default: none)
- `EEZY_REDIS_DB` (default: `0`)

### Sinks

#### ElasticsearchSink

For Elasticsearch clusters. Supports Index Lifecycle Management (ILM) for automatic index rollover and deletion.

```python
from eezy_logging.sinks import ElasticsearchSink, ILMPolicy

# Using environment variables or defaults
sink = ElasticsearchSink(
    index_prefix="myapp-logs",      # Index name prefix
    index_date_format="%Y.%m.%d",   # Date suffix format (None to disable)
    setup_index_template=True,       # Create index template on setup
    setup_ilm_policy=True,           # Create ILM policy on setup
    max_retries=3,                   # Retry attempts for failed writes
    retry_delay=1.0,                 # Initial retry delay (exponential backoff)
)

# Custom ILM policy
policy = ILMPolicy(
    rollover_max_age="1d",
    rollover_max_size="10gb",
    rollover_max_docs=1000000,
    warm_after="7d",
    delete_after="30d",
)
sink = ElasticsearchSink(
    index_prefix="myapp-logs",
    ilm_policy=policy,
)

# Using custom client
from elasticsearch import Elasticsearch
client = Elasticsearch(
    ["https://es.example.com:9200"],
    api_key="your-api-key"
)
sink = ElasticsearchSink(client=client, index_prefix="myapp-logs")
```

**Environment variables** (used when client is not provided):
- `EEZY_ES_HOSTS` (comma-separated, default: `http://localhost:9200`)
- `EEZY_ES_USERNAME`
- `EEZY_ES_PASSWORD`
- `EEZY_ES_API_KEY`
- `EEZY_ES_VERIFY_CERTS` (default: `true`)

**Supported versions**: Elasticsearch 7.x, 8.x, and 9.x are supported. The library automatically detects the installed client version and uses the appropriate APIs.

> **Note**: For Elastic Cloud deployments, create your own client with the `cloud_id` parameter and pass it to the sink.

#### OpenSearchSink

For OpenSearch clusters. Supports Index State Management (ISM) for automatic index rollover and deletion.

```python
from eezy_logging.sinks import OpenSearchSink, ISMPolicy

# Using environment variables or defaults
sink = OpenSearchSink(
    index_prefix="myapp-logs",      # Index name prefix
    index_date_format="%Y.%m.%d",   # Date suffix format (None to disable)
    setup_index_template=True,       # Create index template on setup
    setup_ism_policy=True,           # Create ISM policy on setup
    max_retries=3,                   # Retry attempts for failed writes
    retry_delay=1.0,                 # Initial retry delay (exponential backoff)
)

# Custom ISM policy
policy = ISMPolicy(
    rollover_min_index_age="1d",
    rollover_min_size="10gb",
    rollover_min_doc_count=1000000,
    warm_after="7d",
    delete_after="30d",
)
sink = OpenSearchSink(
    index_prefix="myapp-logs",
    ism_policy=policy,
)

# Using custom client
from opensearchpy import OpenSearch
client = OpenSearch(
    ["https://opensearch.example.com:9200"],
    http_auth=("user", "password")
)
sink = OpenSearchSink(client=client, index_prefix="myapp-logs")
```

**Environment variables** (used when client is not provided):
- `EEZY_OS_HOSTS` (comma-separated, default: `http://localhost:9200`)
- `EEZY_OS_USERNAME`
- `EEZY_OS_PASSWORD`
- `EEZY_OS_VERIFY_CERTS` (default: `true`)

### Custom Sink

Implement the `Sink` interface to send logs anywhere:

```python
from eezy_logging.sinks import Sink, WriteResult

class SlackSink(Sink):
    def __init__(self, webhook_url: str, min_level: int = logging.ERROR):
        self.webhook_url = webhook_url
        self.min_level = min_level

    def setup(self) -> None:
        # Optional: called once before first write
        pass

    def write_batch(self, records: list[dict]) -> WriteResult:
        import requests
        failed = []
        for record in records:
            if record.get("levelno", 0) >= self.min_level:
                try:
                    requests.post(self.webhook_url, json={
                        "text": f"[{record['level']}] {record['message']}"
                    })
                except Exception:
                    failed.append(record)
        if failed:
            return WriteResult.failure(failed, "Failed to post to Slack")
        return WriteResult.ok()

    def close(self) -> None:
        # Optional: cleanup resources
        pass
```

> **Note**: `write_batch` should return `WriteResult.ok()` on success or `WriteResult.failure(records, error)` for records that should be retried. The worker handles retry scheduling with exponential backoff - sinks should NOT block on retries.

### Custom Queue

Implement the `Queue` interface for custom queue backends:

```python
from eezy_logging.queues import Queue

class MyQueue(Queue):
    def put(self, record: dict) -> None:
        # Add record to queue (non-blocking)
        ...

    def get_batch(self, max_size: int, timeout: float) -> list[dict]:
        # Get up to max_size records, block up to timeout seconds
        ...

    def close(self) -> None:
        # Cleanup resources
        ...

    @property
    def size(self) -> int:
        # Return approximate queue size
        ...
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Application Code                               │
│              logger.info("message", extra={...})                 │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    EezyHandler                                   │
│   - Serializes LogRecord to dict                                 │
│   - Pushes to Queue (non-blocking)                               │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Queue                                         │
│   - InMemoryQueue (queue.Queue)                                  │
│   - DequeQueue (collections.deque)                               │
│   - RedisQueue (Redis LIST)                                      │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Worker Thread                                 │
│   - Consumes batches from queue                                  │
│   - Respects batch_size and flush_interval                       │
│   - Non-blocking retry scheduling with exponential backoff       │
│   - Drains queue on shutdown                                     │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Sink                                          │
│   - ElasticsearchSink (bulk API + ILM)                           │
│   - OpenSearchSink (bulk API + ISM)                              │
│   - Custom sinks                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Overflow Behavior

When a queue reaches its `max_size`:

- **InMemoryQueue / DequeQueue**: Oldest records are dropped, a warning is logged every 100 drops
- **RedisQueue**: List is trimmed to `max_size`, keeping newest records

## Graceful Shutdown

Call `handler.close()` to ensure remaining logs are flushed:

```python
import atexit

handler = EezyHandler(sink=sink)
atexit.register(handler.close)  # Optional: auto-close on exit
```

## Log Record Format

Each log record is serialized to a dictionary with a clean structure. Debugging context (hostname, line numbers, thread info, etc.) is grouped under `metadata`:

```python
{
    "@timestamp": "2026-01-06T12:00:00.000000+00:00",
    "message": "User logged in",
    "level": "INFO",
    "logger": "myapp.auth",
    "metadata": {
        "hostname": "myapp-pod-abc123",  # Useful for K8s/distributed systems
        "levelno": 20,
        "pathname": "/app/auth.py",
        "filename": "auth.py",
        "module": "auth",
        "funcName": "login",
        "lineno": 42,
        "process": 12345,
        "processName": "MainProcess",
        "thread": 140234567890,
        "threadName": "MainThread",
    },
    # Extra fields from logger.info(..., extra={...})
    "user_id": 123,
    "ip": "192.168.1.1",
    # Exception info (if present)
    "exc_info": "Traceback (most recent call last):...",
}
```

## License

MIT
