"""eezy-logging: A non-blocking, queue-based Python logging library.

eezy-logging extends Python's standard logging module to ship logs to
various destinations like Elasticsearch without blocking your application.

Key Features:
    - Non-blocking: Logs are queued and processed in a background thread
    - Queue-based: Use in-memory queue or Redis for persistence
    - Extensible: Easy to create custom sinks and queues
    - Elasticsearch/OpenSearch support: With ILM and index templates

Basic Usage:
    import logging
    from eezy_logging import EezyHandler
    from eezy_logging.sinks import ElasticsearchSink

    # Create handler
    sink = ElasticsearchSink(index_prefix="myapp-logs")
    handler = EezyHandler(sink=sink)

    # Attach to logger
    logger = logging.getLogger("myapp")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Log normally
    logger.info("Hello world", extra={"user_id": 123})

    # Clean shutdown
    handler.close()

With Redis Queue (for persistence):
    from eezy_logging import EezyHandler
    from eezy_logging.queues import RedisQueue
    from eezy_logging.sinks import ElasticsearchSink

    queue = RedisQueue(key="myapp:logs")
    sink = ElasticsearchSink(index_prefix="myapp-logs")
    handler = EezyHandler(sink=sink, queue=queue)

Custom Sink:
    from eezy_logging.sinks import Sink

    class MySink(Sink):
        def write_batch(self, records):
            for record in records:
                print(record["message"])

        def close(self):
            pass
"""

from eezy_logging.handler import EezyHandler
from eezy_logging.queues import DequeQueue, InMemoryQueue, Queue, RedisQueue
from eezy_logging.serializer import serialize_record
from eezy_logging.sinks import ElasticsearchSink, Sink
from eezy_logging.worker import Worker

__version__ = "0.1.0"

__all__ = [
    # Main handler
    "EezyHandler",
    # Queue interface and implementations
    "Queue",
    "InMemoryQueue",
    "DequeQueue",
    "RedisQueue",
    # Sink interface and implementations
    "Sink",
    "ElasticsearchSink",
    # Utilities
    "Worker",
    "serialize_record",
    # Version
    "__version__",
]
