"""Integration tests for ElasticsearchSink.

These tests require a running Elasticsearch instance.
Start it with: docker compose up -d

Run these tests with:
    pytest tests/test_elasticsearch_integration.py -v
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
from eezy_logging.sinks import ElasticsearchSink, ILMPolicy

if TYPE_CHECKING:
    from elasticsearch import Elasticsearch


# Test configuration
ES_HOST = os.environ.get("EEZY_TEST_ES_HOST", "http://localhost:9200")


def _get_es_client_major_version() -> int:
    """Get the major version of the installed elasticsearch client."""
    import elasticsearch

    version = elasticsearch.__version__
    if isinstance(version, tuple):
        return version[0]
    # Handle string version like "8.0.0"
    return int(str(version).split(".")[0])


# Cache the client major version
ES_CLIENT_MAJOR_VERSION = _get_es_client_major_version()


@pytest.fixture
def es_client():
    """Create an Elasticsearch client for testing."""
    import elasticsearch
    from elasticsearch import Elasticsearch

    client = Elasticsearch(
        [ES_HOST],
        verify_certs=False,
        ssl_show_warn=False,
    )

    # Print version info for debugging CI issues
    client_version = elasticsearch.__version__
    try:
        info = client.info()
        server_version = info["version"]["number"]
    except Exception as e:
        server_version = f"unknown (error: {e})"

    print(f"\n[ES Version Info] Client: {client_version}, Server: {server_version}")

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


def cleanup_template(client: Any, template_name: str) -> None:
    """Delete index template (both composable and legacy)."""
    # Try composable template first (ES 8.x+)
    try:
        if ES_CLIENT_MAJOR_VERSION >= 8:
            client.options(ignore_status=404).indices.delete_index_template(name=template_name)
        else:
            client.indices.delete_index_template(name=template_name, ignore=[404])
    except Exception:
        pass
    # Also try legacy template (ES 7.x)
    try:
        if ES_CLIENT_MAJOR_VERSION >= 8:
            client.options(ignore_status=404).indices.delete_template(name=template_name)
        else:
            client.indices.delete_template(name=template_name, ignore=[404])
    except Exception:
        pass


def cleanup_ilm_policy(client: Any, policy_name: str) -> None:
    """Delete ILM policy."""
    try:
        if ES_CLIENT_MAJOR_VERSION >= 8:
            client.ilm.delete_lifecycle(name=policy_name)
        else:
            client.ilm.delete_lifecycle(policy=policy_name)
    except Exception:
        pass


def get_ilm_policy(client: Any, policy_name: str) -> dict:
    """Get ILM policy, handling API differences between ES versions."""
    if ES_CLIENT_MAJOR_VERSION >= 8:
        return client.ilm.get_lifecycle(name=policy_name)
    else:
        return client.ilm.get_lifecycle(policy=policy_name)


def cleanup_alias(client: Any, alias_name: str) -> None:
    """Delete an alias."""
    try:
        client.indices.delete_alias(index="_all", name=alias_name, ignore_unavailable=True)
    except Exception:
        pass


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

            # Check template exists - try composable first (ES 8.x+), then legacy (ES 7.x)
            template_name = f"{unique_index_prefix}-template"
            try:
                response = es_client.indices.get_index_template(name=template_name)
                templates = response.get("index_templates", [])
                assert len(templates) == 1
                assert templates[0]["name"] == template_name
            except Exception:
                # Fall back to legacy template API (ES 7.x)
                response = es_client.indices.get_template(name=template_name)
                assert template_name in response
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
            response = get_ilm_policy(es_client, policy_name)
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
            response = get_ilm_policy(es_client, policy_name)
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


class TestCustomILMPolicy:
    """Tests for custom ILM policy configuration."""

    def test_custom_ilm_policy_created(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test that custom ILM policy is created with correct settings."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ILMPolicy(
            rollover_max_age="1d",
            rollover_max_size="10gb",
            rollover_max_docs=1000000,
            warm_after="7d",
            warm_shrink_shards=1,
            warm_force_merge_segments=1,
            cold_after="14d",
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

            # Check ILM policy exists with correct settings
            response = get_ilm_policy(es_client, policy_name)
            assert policy_name in response
            policy = response[policy_name]["policy"]
            phases = policy["phases"]

            # Verify hot phase
            assert "hot" in phases
            rollover = phases["hot"]["actions"]["rollover"]
            assert rollover["max_age"] == "1d"
            assert rollover["max_size"] == "10gb"
            assert rollover["max_docs"] == 1000000

            # Verify warm phase
            assert "warm" in phases
            assert phases["warm"]["min_age"] == "7d"

            # Verify cold phase
            assert "cold" in phases
            assert phases["cold"]["min_age"] == "14d"

            # Verify delete phase
            assert "delete" in phases
            assert phases["delete"]["min_age"] == "30d"
        finally:
            cleanup_template(es_client, f"{unique_index_prefix}-template")
            cleanup_ilm_policy(es_client, policy_name)

    def test_ilm_policy_without_delete(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test ILM policy without delete phase (keep forever)."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ILMPolicy(
            rollover_max_age="7d",
            delete_after=None,  # Keep forever
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

            response = get_ilm_policy(es_client, policy_name)
            policy = response[policy_name]["policy"]
            phases = policy["phases"]

            # Should not have delete phase
            assert "delete" not in phases
            assert "hot" in phases
            assert "warm" in phases
        finally:
            cleanup_template(es_client, f"{unique_index_prefix}-template")
            cleanup_ilm_policy(es_client, policy_name)

    def test_ilm_policy_without_cold(self, es_client: Elasticsearch, unique_index_prefix: str):
        """Test ILM policy without cold phase."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ILMPolicy(
            rollover_max_age="7d",
            cold_after=None,  # Skip cold phase
            delete_after="90d",
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

            response = get_ilm_policy(es_client, policy_name)
            policy = response[policy_name]["policy"]
            phases = policy["phases"]

            # Should not have cold phase
            assert "cold" not in phases
            assert "hot" in phases
            assert "warm" in phases
            assert "delete" in phases
        finally:
            cleanup_template(es_client, f"{unique_index_prefix}-template")
            cleanup_ilm_policy(es_client, policy_name)
