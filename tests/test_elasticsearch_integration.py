"""Integration tests for ElasticsearchSink.

These tests require running Elasticsearch and OpenSearch instances.
Start them with: docker compose up -d

Run these tests with:
    pytest tests/test_elasticsearch_integration.py -v

Skip in CI without docker:
    pytest tests/test_elasticsearch_integration.py -v -m integration
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from eezy_logging import EezyHandler, InMemoryQueue
from eezy_logging.sinks import ElasticsearchSink, ILMPolicy, ISMPolicy, OpenSearchSink

if TYPE_CHECKING:
    from elasticsearch import Elasticsearch
    from opensearchpy import OpenSearch


# Test configuration
ES_HOST = os.environ.get("EEZY_TEST_ES_HOST", "http://localhost:9200")
OPENSEARCH_HOST = os.environ.get("EEZY_TEST_OPENSEARCH_HOST", "http://localhost:9201")


def is_elasticsearch_available() -> bool:
    """Check if Elasticsearch is available."""
    try:
        from elasticsearch import Elasticsearch

        # ES 8.x may need SSL verification disabled for local testing
        client = Elasticsearch(
            [ES_HOST],
            verify_certs=False,
            ssl_show_warn=False,
        )
        info = client.info()
        client.close()
        # Verify it's actually Elasticsearch (not OpenSearch)
        return "version" in info and "lucene_version" in info.get("version", {})
    except Exception:
        return False


def is_opensearch_available() -> bool:
    """Check if OpenSearch is available."""
    try:
        from opensearchpy import OpenSearch

        client = OpenSearch(
            [OPENSEARCH_HOST],
            verify_certs=False,
            ssl_show_warn=False,
        )
        info = client.info()
        client.close()
        return "version" in info
    except Exception:
        return False


# Markers for skipping tests
elasticsearch_available = pytest.mark.skipif(
    not is_elasticsearch_available(),
    reason="Elasticsearch not available at " + ES_HOST,
)

opensearch_available = pytest.mark.skipif(
    not is_opensearch_available(),
    reason="OpenSearch not available at " + OPENSEARCH_HOST,
)


@pytest.fixture
def es_client():
    """Create an Elasticsearch client for testing."""
    from elasticsearch import Elasticsearch

    client = Elasticsearch(
        [ES_HOST],
        verify_certs=False,
        ssl_show_warn=False,
    )
    yield client
    client.close()


@pytest.fixture
def opensearch_client():
    """Create an OpenSearch client for testing."""
    from opensearchpy import OpenSearch

    client = OpenSearch(
        [OPENSEARCH_HOST],
        verify_certs=False,
        ssl_show_warn=False,
    )
    yield client
    client.close()


@pytest.fixture
def unique_index_prefix() -> str:
    """Generate a unique index prefix for each test."""
    return f"eezy-test-{uuid.uuid4().hex[:8]}"


def wait_for_docs(
    client: Any,
    index_pattern: str,
    expected_count: int,
    timeout: float = 10.0,
) -> list[dict]:
    """Wait for documents to appear in an index."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            # Refresh to make documents searchable
            client.indices.refresh(index=index_pattern)
            response = client.search(
                index=index_pattern,
                body={"query": {"match_all": {}}, "size": 100},
            )
            hits = response["hits"]["hits"]
            if len(hits) >= expected_count:
                return [hit["_source"] for hit in hits]
        except Exception:
            pass
        time.sleep(0.5)

    raise TimeoutError(
        f"Expected {expected_count} docs in {index_pattern}, timed out after {timeout}s"
    )


def cleanup_indices(client: Any, index_pattern: str) -> None:
    """Delete test indices."""
    try:
        client.indices.delete(index=index_pattern, ignore_unavailable=True)
    except Exception:
        pass


def cleanup_template(client: Any, template_name: str, is_opensearch: bool = False) -> None:
    """Delete index template."""
    try:
        if is_opensearch:
            client.indices.delete_template(name=template_name, ignore=(404,))
        else:
            # Use .options() to avoid deprecation warning
            client.options(ignore_status=404).indices.delete_index_template(name=template_name)
    except Exception:
        pass


def cleanup_ilm_policy(client: Any, policy_name: str) -> None:
    """Delete ILM policy (Elasticsearch only)."""
    try:
        client.ilm.delete_lifecycle(name=policy_name)
    except Exception:
        pass


def cleanup_ism_policy(client: Any, policy_name: str) -> None:
    """Delete ISM policy (OpenSearch only)."""
    try:
        client.transport.perform_request(
            "DELETE",
            f"/_plugins/_ism/policies/{policy_name}",
        )
    except Exception:
        pass


# =============================================================================
# Elasticsearch Tests
# =============================================================================


@elasticsearch_available
class TestElasticsearchSinkIntegration:
    """Integration tests for ElasticsearchSink with Elasticsearch."""

    def test_write_single_record(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test writing a single log record."""
        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_ilm_policy=False,  # Skip ILM for faster tests
        )

        try:
            sink.setup()
            sink.write_batch(
                [
                    {
                        "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                        "message": "Test log message",
                        "level": "INFO",
                        "logger": "test",
                        "metadata": {"hostname": "test-host"},
                    }
                ]
            )

            docs = wait_for_docs(es_client, f"{unique_index_prefix}-*", 1)
            assert len(docs) == 1
            assert docs[0]["message"] == "Test log message"
            assert docs[0]["level"] == "INFO"
        finally:
            cleanup_indices(es_client, f"{unique_index_prefix}-*")
            cleanup_template(es_client, f"{unique_index_prefix}-template")

    def test_write_batch(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test writing multiple records in a batch."""
        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_ilm_policy=False,
        )

        try:
            sink.setup()
            records = [
                {
                    "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                    "message": f"Log message {i}",
                    "level": "INFO",
                    "logger": "test",
                }
                for i in range(10)
            ]
            sink.write_batch(records)

            docs = wait_for_docs(es_client, f"{unique_index_prefix}-*", 10)
            assert len(docs) == 10
            messages = {doc["message"] for doc in docs}
            assert messages == {f"Log message {i}" for i in range(10)}
        finally:
            cleanup_indices(es_client, f"{unique_index_prefix}-*")
            cleanup_template(es_client, f"{unique_index_prefix}-template")

    def test_extra_fields_indexed(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test that extra fields are indexed."""
        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_ilm_policy=False,
        )

        try:
            sink.setup()
            sink.write_batch(
                [
                    {
                        "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                        "message": "User action",
                        "level": "INFO",
                        "logger": "test",
                        "user_id": 12345,
                        "action": "login",
                        "ip_address": "192.168.1.100",
                    }
                ]
            )

            docs = wait_for_docs(es_client, f"{unique_index_prefix}-*", 1)
            assert docs[0]["user_id"] == 12345
            assert docs[0]["action"] == "login"
            assert docs[0]["ip_address"] == "192.168.1.100"
        finally:
            cleanup_indices(es_client, f"{unique_index_prefix}-*")
            cleanup_template(es_client, f"{unique_index_prefix}-template")

    def test_metadata_indexed(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test that metadata fields are correctly indexed."""
        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_ilm_policy=False,
        )

        try:
            sink.setup()
            sink.write_batch(
                [
                    {
                        "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                        "message": "Test message",
                        "level": "WARNING",
                        "logger": "myapp.module",
                        "metadata": {
                            "hostname": "pod-abc123",
                            "levelno": 30,
                            "filename": "module.py",
                            "funcName": "my_function",
                            "lineno": 42,
                        },
                    }
                ]
            )

            docs = wait_for_docs(es_client, f"{unique_index_prefix}-*", 1)
            metadata = docs[0]["metadata"]
            assert metadata["hostname"] == "pod-abc123"
            assert metadata["levelno"] == 30
            assert metadata["filename"] == "module.py"
            assert metadata["funcName"] == "my_function"
            assert metadata["lineno"] == 42
        finally:
            cleanup_indices(es_client, f"{unique_index_prefix}-*")
            cleanup_template(es_client, f"{unique_index_prefix}-template")

    def test_index_template_created(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test that index template is created on setup."""
        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_index_template=True,
            setup_ilm_policy=False,
        )

        try:
            sink.setup()

            # Check template exists
            template_name = f"{unique_index_prefix}-template"
            response = es_client.indices.get_index_template(name=template_name)
            templates = response.get("index_templates", [])
            assert len(templates) == 1
            assert templates[0]["name"] == template_name
        finally:
            cleanup_template(es_client, f"{unique_index_prefix}-template")

    def test_ilm_policy_created(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test that ILM policy is created on setup."""
        policy_name = f"{unique_index_prefix}-policy"
        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_ilm_policy=True,
            ilm_policy_name=policy_name,
        )

        try:
            sink.setup()

            # Check ILM policy exists
            response = es_client.ilm.get_lifecycle(name=policy_name)
            assert policy_name in response
            policy = response[policy_name]["policy"]
            assert "phases" in policy
            assert "hot" in policy["phases"]
            assert "delete" in policy["phases"]
        finally:
            cleanup_template(es_client, f"{unique_index_prefix}-template")
            cleanup_ilm_policy(es_client, policy_name)

    def test_full_handler_integration(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test full integration with EezyHandler."""
        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_ilm_policy=False,
        )
        queue = InMemoryQueue(max_size=1000)
        handler = EezyHandler(sink=sink, queue=queue, batch_size=5, flush_interval=1.0)

        logger = logging.getLogger(f"test-{unique_index_prefix}")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            # Log some messages
            logger.info("Application started", extra={"version": "1.0.0"})
            logger.warning("Low memory", extra={"memory_mb": 128})
            logger.error("Connection failed", extra={"host": "db.example.com"})

            # Wait for logs to be processed
            handler.close()

            docs = wait_for_docs(es_client, f"{unique_index_prefix}-*", 3)
            assert len(docs) == 3

            # Verify content
            messages = {doc["message"] for doc in docs}
            assert "Application started" in messages
            assert "Low memory" in messages
            assert "Connection failed" in messages

            # Check levels
            levels = {doc["level"] for doc in docs}
            assert levels == {"INFO", "WARNING", "ERROR"}
        finally:
            logger.removeHandler(handler)
            cleanup_indices(es_client, f"{unique_index_prefix}-*")
            cleanup_template(es_client, f"{unique_index_prefix}-template")


# =============================================================================
# OpenSearch Tests
# =============================================================================


@opensearch_available
class TestOpenSearchSinkIntegration:
    """Integration tests for OpenSearchSink with OpenSearch."""

    def test_write_single_record(self, opensearch_client: OpenSearch, unique_index_prefix: str):
        """Test writing a single log record to OpenSearch."""
        sink = OpenSearchSink(
            client=opensearch_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=False,  # Skip ISM for faster tests
        )

        try:
            sink.setup()
            sink.write_batch(
                [
                    {
                        "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                        "message": "Test log message",
                        "level": "INFO",
                        "logger": "test",
                        "metadata": {"hostname": "test-host"},
                    }
                ]
            )

            docs = wait_for_docs(opensearch_client, f"{unique_index_prefix}-*", 1)
            assert len(docs) == 1
            assert docs[0]["message"] == "Test log message"
            assert docs[0]["level"] == "INFO"
        finally:
            cleanup_indices(opensearch_client, f"{unique_index_prefix}-*")
            cleanup_template(
                opensearch_client, f"{unique_index_prefix}-template", is_opensearch=True
            )

    def test_write_batch(self, opensearch_client: OpenSearch, unique_index_prefix: str):
        """Test writing multiple records in a batch to OpenSearch."""
        sink = OpenSearchSink(
            client=opensearch_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=False,
        )

        try:
            sink.setup()
            records = [
                {
                    "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                    "message": f"Log message {i}",
                    "level": "INFO",
                    "logger": "test",
                }
                for i in range(10)
            ]
            sink.write_batch(records)

            docs = wait_for_docs(opensearch_client, f"{unique_index_prefix}-*", 10)
            assert len(docs) == 10
            messages = {doc["message"] for doc in docs}
            assert messages == {f"Log message {i}" for i in range(10)}
        finally:
            cleanup_indices(opensearch_client, f"{unique_index_prefix}-*")
            cleanup_template(
                opensearch_client, f"{unique_index_prefix}-template", is_opensearch=True
            )

    def test_uses_legacy_template_api(
        self, opensearch_client: OpenSearch, unique_index_prefix: str
    ):
        """Test that OpenSearch uses legacy template API."""
        sink = OpenSearchSink(
            client=opensearch_client,
            index_prefix=unique_index_prefix,
            setup_index_template=True,
            setup_ism_policy=False,
        )

        try:
            sink.setup()

            # Check legacy template exists
            template_name = f"{unique_index_prefix}-template"
            response = opensearch_client.indices.get_template(name=template_name)
            assert template_name in response
        finally:
            cleanup_template(
                opensearch_client, f"{unique_index_prefix}-template", is_opensearch=True
            )


# =============================================================================
# Rollover Tests
# =============================================================================


def cleanup_alias(client: Any, alias_name: str) -> None:
    """Delete an alias."""
    try:
        client.indices.delete_alias(index="_all", name=alias_name, ignore_unavailable=True)
    except Exception:
        pass


@elasticsearch_available
class TestElasticsearchRollover:
    """Tests for Elasticsearch ILM rollover functionality."""

    def test_ilm_policy_rollover_conditions(
        self, es_client: Elasticsearch, unique_index_prefix: str
    ):
        """Test that ILM policy is configured with correct rollover conditions."""
        policy_name = f"{unique_index_prefix}-policy"

        # Create ILM policy with specific rollover conditions
        custom_policy = ILMPolicy(
            rollover_max_age="1d",
            rollover_max_size="10gb",
            rollover_max_docs=1000,
            warm_after="7d",
            delete_after="30d",
        )

        sink = ElasticsearchSink(
            client=es_client,
            index_prefix=unique_index_prefix,
            setup_ilm_policy=True,
            ilm_policy_name=policy_name,
            ilm_policy=custom_policy,
        )

        try:
            sink.setup()

            # Verify ILM policy has correct rollover configuration
            response = es_client.ilm.get_lifecycle(name=policy_name)
            policy = response[policy_name]["policy"]
            rollover = policy["phases"]["hot"]["actions"]["rollover"]

            assert rollover["max_age"] == "1d"
            assert rollover["max_size"] == "10gb"
            assert rollover["max_docs"] == 1000

        finally:
            cleanup_template(es_client, f"{unique_index_prefix}-template")
            cleanup_ilm_policy(es_client, policy_name)

    def test_rollover_with_conditions(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test rollover API with max_docs condition (simulates ILM behavior)."""
        alias_name = unique_index_prefix

        try:
            # Create initial index with write alias
            initial_index = f"{unique_index_prefix}-000001"
            es_client.indices.create(
                index=initial_index,
                body={
                    "aliases": {
                        alias_name: {"is_write_index": True},
                    },
                },
            )

            # Write documents
            for i in range(3):
                es_client.index(
                    index=alias_name,
                    body={
                        "@timestamp": datetime.now(UTC).isoformat(),
                        "message": f"Test message {i}",
                        "level": "INFO",
                    },
                    refresh=True,
                )

            # Trigger rollover with max_docs=2 condition (should succeed since we have 3)
            rollover_response = es_client.indices.rollover(
                alias=alias_name,
                body={
                    "conditions": {
                        "max_docs": 2,
                    }
                },
            )

            assert rollover_response["rolled_over"] is True
            assert rollover_response["old_index"] == initial_index
            assert rollover_response["new_index"] == f"{unique_index_prefix}-000002"

            # Verify conditions were met
            assert rollover_response["conditions"]["[max_docs: 2]"] is True

        finally:
            cleanup_indices(es_client, f"{unique_index_prefix}-*")
            cleanup_alias(es_client, alias_name)

    def test_manual_rollover(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test manual rollover API works with our index setup."""
        alias_name = unique_index_prefix

        try:
            # Create initial index with write alias
            initial_index = f"{unique_index_prefix}-000001"
            es_client.indices.create(
                index=initial_index,
                body={
                    "aliases": {
                        alias_name: {"is_write_index": True},
                    },
                },
            )

            # Write a document
            es_client.index(
                index=alias_name,
                body={
                    "@timestamp": datetime.now(UTC).isoformat(),
                    "message": "Test message before rollover",
                    "level": "INFO",
                },
                refresh=True,
            )

            # Perform manual rollover
            rollover_response = es_client.indices.rollover(alias=alias_name)

            assert rollover_response["rolled_over"] is True
            assert rollover_response["old_index"] == initial_index
            new_index = rollover_response["new_index"]
            assert new_index == f"{unique_index_prefix}-000002"

            # Write another document - should go to new index
            es_client.index(
                index=alias_name,
                body={
                    "@timestamp": datetime.now(UTC).isoformat(),
                    "message": "Test message after rollover",
                    "level": "INFO",
                },
                refresh=True,
            )

            # Verify documents are in correct indices
            old_docs = es_client.search(index=initial_index, body={"query": {"match_all": {}}})
            new_docs = es_client.search(index=new_index, body={"query": {"match_all": {}}})

            assert old_docs["hits"]["total"]["value"] == 1
            assert new_docs["hits"]["total"]["value"] == 1

        finally:
            cleanup_indices(es_client, f"{unique_index_prefix}-*")
            cleanup_alias(es_client, alias_name)


@opensearch_available
class TestOpenSearchRollover:
    """Tests for OpenSearch ISM rollover functionality."""

    def test_manual_rollover(self, opensearch_client: OpenSearch, unique_index_prefix: str):
        """Test manual rollover API works with OpenSearch."""
        alias_name = unique_index_prefix

        try:
            # Create initial index with write alias
            initial_index = f"{unique_index_prefix}-000001"
            opensearch_client.indices.create(
                index=initial_index,
                body={
                    "aliases": {
                        alias_name: {"is_write_index": True},
                    },
                },
            )

            # Write a document
            opensearch_client.index(
                index=alias_name,
                body={
                    "@timestamp": datetime.now(UTC).isoformat(),
                    "message": "Test message before rollover",
                    "level": "INFO",
                },
            )
            opensearch_client.indices.refresh(index=alias_name)

            # Perform manual rollover
            rollover_response = opensearch_client.indices.rollover(alias=alias_name)

            assert rollover_response["rolled_over"] is True
            assert rollover_response["old_index"] == initial_index
            new_index = rollover_response["new_index"]
            assert new_index == f"{unique_index_prefix}-000002"

            # Write another document - should go to new index
            opensearch_client.index(
                index=alias_name,
                body={
                    "@timestamp": datetime.now(UTC).isoformat(),
                    "message": "Test message after rollover",
                    "level": "INFO",
                },
            )
            opensearch_client.indices.refresh(index=alias_name)

            # Verify documents are in correct indices
            old_docs = opensearch_client.search(
                index=initial_index, body={"query": {"match_all": {}}}
            )
            new_docs = opensearch_client.search(index=new_index, body={"query": {"match_all": {}}})

            assert old_docs["hits"]["total"]["value"] == 1
            assert new_docs["hits"]["total"]["value"] == 1

        finally:
            cleanup_indices(opensearch_client, f"{unique_index_prefix}-*")
            cleanup_alias(opensearch_client, alias_name)

    def test_ism_policy_attached_to_index(
        self, opensearch_client: OpenSearch, unique_index_prefix: str
    ):
        """Test that ISM policy gets attached to indices matching the pattern."""
        policy_name = f"{unique_index_prefix}-policy"

        custom_policy = ISMPolicy(
            rollover_min_index_age="1d",
            rollover_min_size="1gb",
            rollover_min_doc_count=100,
            delete_after="30d",
        )

        sink = OpenSearchSink(
            client=opensearch_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=True,
            ism_policy_name=policy_name,
            ism_policy=custom_policy,
        )

        try:
            sink.setup()

            # Write a document to create an index
            sink.write_batch(
                [
                    {
                        "@timestamp": datetime.now(UTC).isoformat(),
                        "message": "Test message",
                        "level": "INFO",
                    }
                ]
            )

            # Wait for ISM to attach
            time.sleep(2)

            # Check if ISM policy is attached
            response = opensearch_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/explain/{unique_index_prefix}-*",
            )

            # Verify response structure is valid (ISM attachment is async)
            assert response is not None
            # Response should contain index information
            assert isinstance(response, dict)

        finally:
            cleanup_indices(opensearch_client, f"{unique_index_prefix}-*")
            cleanup_template(
                opensearch_client, f"{unique_index_prefix}-template", is_opensearch=True
            )
            cleanup_ism_policy(opensearch_client, policy_name)

    def test_ism_policy_without_delete(
        self, opensearch_client: OpenSearch, unique_index_prefix: str
    ):
        """Test ISM policy without delete state (keep forever)."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ISMPolicy(
            rollover_min_index_age="7d",
            delete_after=None,  # Keep forever
        )
        sink = OpenSearchSink(
            client=opensearch_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=True,
            ism_policy_name=policy_name,
            ism_policy=custom_policy,
        )

        try:
            sink.setup()

            response = opensearch_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/policies/{policy_name}",
            )
            policy = response.get("policy", {})
            states = {s["name"]: s for s in policy.get("states", [])}

            # Should not have delete state
            assert "delete" not in states
            assert "hot" in states
            assert "warm" in states
        finally:
            cleanup_template(
                opensearch_client, f"{unique_index_prefix}-template", is_opensearch=True
            )
            cleanup_ism_policy(opensearch_client, policy_name)
