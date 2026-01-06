"""Sink implementations for eezy-logging."""

from eezy_logging.sinks.base import Sink
from eezy_logging.sinks.elasticsearch_sink import ElasticsearchSink, ILMPolicy
from eezy_logging.sinks.opensearch_sink import ISMPolicy, OpenSearchSink

__all__ = [
    "Sink",
    "ElasticsearchSink",
    "ILMPolicy",
    "OpenSearchSink",
    "ISMPolicy",
]
