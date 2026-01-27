"""Integration tests for OpenSearchSink.

These tests require a running OpenSearch instance.
Start it with: docker compose up -d

Run these tests with:
    pytest tests/test_opensearch_integration.py -v
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
from eezy_logging.sinks import ISMPolicy, OpenSearchSink

if TYPE_CHECKING:
    from opensearchpy import OpenSearch


# Test configuration
OS_HOST = os.environ.get("EEZY_TEST_OPENSEARCH_HOST", "http://localhost:9201")


@pytest.fixture
def os_client():
    """Create an OpenSearch client for testing."""
    from opensearchpy import OpenSearch

    client = OpenSearch(
        hosts=[OS_HOST],
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


def cleanup_template(client: Any, template_name: str) -> None:
    """Delete legacy index template."""
    try:
        client.indices.delete_template(name=template_name, ignore=[404])
    except Exception:
        pass


def cleanup_ism_policy(client: Any, policy_name: str) -> None:
    """Delete ISM policy."""
    try:
        client.transport.perform_request(
            "DELETE",
            f"/_plugins/_ism/policies/{policy_name}",
        )
    except Exception:
        pass


def cleanup_alias(client: Any, alias_name: str) -> None:
    """Delete an alias."""
    try:
        client.indices.delete_alias(index="_all", name=alias_name, ignore=[404])
    except Exception:
        pass


class TestOpenSearchSinkIntegration:
    """Integration tests for OpenSearchSink with OpenSearch."""

    def test_write_single_record(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test writing a single log record."""
        sink = OpenSearchSink(
            client=os_client,
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

            docs = wait_for_docs(os_client, f"{unique_index_prefix}-*", 1)
            assert len(docs) == 1
            assert docs[0]["message"] == "Test log message"
            assert docs[0]["level"] == "INFO"
        finally:
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_template(os_client, f"{unique_index_prefix}-template")

    def test_write_batch(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test writing multiple records in a batch."""
        sink = OpenSearchSink(
            client=os_client,
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

            docs = wait_for_docs(os_client, f"{unique_index_prefix}-*", 10)
            assert len(docs) == 10
            messages = {doc["message"] for doc in docs}
            assert messages == {f"Log message {i}" for i in range(10)}
        finally:
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_template(os_client, f"{unique_index_prefix}-template")

    def test_uses_legacy_template_api(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that OpenSearchSink uses legacy template API."""
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_index_template=True,
            setup_ism_policy=False,
        )

        try:
            sink.setup()

            # Check legacy template exists (OpenSearch uses put_template, not put_index_template)
            template_name = f"{unique_index_prefix}-template"
            response = os_client.indices.get_template(name=template_name)
            assert template_name in response
            assert f"{unique_index_prefix}-*" in response[template_name]["index_patterns"]
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")

    def test_extra_fields_indexed(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that extra fields are indexed."""
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=False,
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

            docs = wait_for_docs(os_client, f"{unique_index_prefix}-*", 1)
            assert docs[0]["user_id"] == 12345
            assert docs[0]["action"] == "login"
            assert docs[0]["ip_address"] == "192.168.1.100"
        finally:
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_template(os_client, f"{unique_index_prefix}-template")

    def test_full_handler_integration(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test full integration with EezyHandler."""
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=False,
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

            docs = wait_for_docs(os_client, f"{unique_index_prefix}-*", 3)
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
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_template(os_client, f"{unique_index_prefix}-template")


class TestOpenSearchRollover:
    """Tests for OpenSearch rollover functionality."""

    def test_manual_rollover(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test manual rollover API works with our index setup."""
        alias_name = unique_index_prefix

        try:
            # Create initial index with write alias
            initial_index = f"{unique_index_prefix}-000001"
            os_client.indices.create(
                index=initial_index,
                body={
                    "aliases": {
                        alias_name: {"is_write_index": True},
                    },
                },
            )

            # Write a document
            os_client.index(
                index=alias_name,
                body={
                    "@timestamp": datetime.now(UTC).isoformat(),
                    "message": "Test message before rollover",
                    "level": "INFO",
                },
                refresh=True,
            )

            # Perform manual rollover
            rollover_response = os_client.indices.rollover(alias=alias_name)

            assert rollover_response["rolled_over"] is True
            assert rollover_response["old_index"] == initial_index
            new_index = rollover_response["new_index"]
            assert new_index == f"{unique_index_prefix}-000002"

            # Write another document - should go to new index
            os_client.index(
                index=alias_name,
                body={
                    "@timestamp": datetime.now(UTC).isoformat(),
                    "message": "Test message after rollover",
                    "level": "INFO",
                },
                refresh=True,
            )

            # Verify documents are in correct indices
            old_docs = os_client.search(index=initial_index, body={"query": {"match_all": {}}})
            new_docs = os_client.search(index=new_index, body={"query": {"match_all": {}}})

            assert old_docs["hits"]["total"]["value"] == 1
            assert new_docs["hits"]["total"]["value"] == 1

        finally:
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_alias(os_client, alias_name)

    def test_ism_policy_attached_to_index(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that ISM policy can be attached to an index."""
        policy_name = f"{unique_index_prefix}-policy"

        try:
            # Create ISM policy
            policy = ISMPolicy(
                rollover_min_index_age="1d",
                rollover_min_size="10gb",
                delete_after="30d",
            )
            policy_body = policy.to_policy_body(policy_name, [f"{unique_index_prefix}-*"])

            os_client.transport.perform_request(
                "PUT",
                f"/_plugins/_ism/policies/{policy_name}",
                body=policy_body,
            )

            # Verify policy was created
            response = os_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/policies/{policy_name}",
            )
            assert response["_id"] == policy_name
            assert "policy" in response

        finally:
            cleanup_ism_policy(os_client, policy_name)


class TestIndexAliases:
    """Tests for index alias configuration."""

    def test_default_alias_uses_index_prefix(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that default alias is the index_prefix."""
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=False,
        )

        try:
            sink.setup()

            # Check template includes default alias
            template_name = f"{unique_index_prefix}-template"
            response = os_client.indices.get_template(name=template_name)
            template = response[template_name]

            # Verify aliases section exists with index_prefix as alias
            assert "aliases" in template
            assert unique_index_prefix in template["aliases"]
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")

    def test_custom_aliases(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that custom aliases are applied to index template."""
        custom_aliases = ["logs", "app-logs", "production"]
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            index_aliases=custom_aliases,
            setup_ism_policy=False,
        )

        try:
            sink.setup()

            # Check template includes custom aliases
            template_name = f"{unique_index_prefix}-template"
            response = os_client.indices.get_template(name=template_name)
            template = response[template_name]

            # Verify all custom aliases are present
            assert "aliases" in template
            for alias in custom_aliases:
                assert alias in template["aliases"]

            # Verify index_prefix is NOT in aliases (since we provided custom ones)
            assert unique_index_prefix not in template["aliases"]
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")
            for alias in custom_aliases:
                cleanup_alias(os_client, alias)

    def test_empty_aliases_list_disables_aliases(
        self, os_client: OpenSearch, unique_index_prefix: str
    ):
        """Test that empty aliases list disables alias creation."""
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            index_aliases=[],
            setup_ism_policy=False,
        )

        try:
            sink.setup()

            # Check template does not include aliases section
            template_name = f"{unique_index_prefix}-template"
            response = os_client.indices.get_template(name=template_name)
            template = response[template_name]

            # Aliases section should be absent or empty
            aliases = template.get("aliases", {})
            assert len(aliases) == 0
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")

    def test_query_via_alias(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that documents can be queried using the alias."""
        alias_name = "test-logs-alias"
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            index_aliases=[alias_name],
            setup_ism_policy=False,
        )

        try:
            sink.setup()

            # Write documents
            sink.write_batch(
                [
                    {
                        "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                        "message": f"Test message {i}",
                        "level": "INFO",
                        "logger": "test",
                    }
                    for i in range(5)
                ]
            )

            # Wait for documents to be indexed
            wait_for_docs(os_client, f"{unique_index_prefix}-*", 5)

            # Query via alias name
            os_client.indices.refresh(index=alias_name)
            response = os_client.search(
                index=alias_name,
                body={"query": {"match_all": {}}, "size": 10},
            )

            # Verify we can query via alias
            assert response["hits"]["total"]["value"] == 5
            messages = {hit["_source"]["message"] for hit in response["hits"]["hits"]}
            assert messages == {f"Test message {i}" for i in range(5)}
        finally:
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_template(os_client, f"{unique_index_prefix}-template")
            cleanup_alias(os_client, alias_name)


class TestCustomIndexSettings:
    """Tests for custom index settings and mappings."""

    def test_custom_index_settings(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that custom index settings are applied."""
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=False,
            custom_index_settings={
                "number_of_shards": 2,
                "number_of_replicas": 0,
                "refresh_interval": "5s",
            },
        )

        try:
            sink.setup()

            # Write a record to create the index
            sink.write_batch(
                [
                    {
                        "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                        "message": "Test message",
                        "level": "INFO",
                        "logger": "test",
                    }
                ]
            )

            # Wait for index to be created
            wait_for_docs(os_client, f"{unique_index_prefix}-*", 1)

            # Check index settings (use wildcard to get actual index name)
            settings = os_client.indices.get_settings(index=f"{unique_index_prefix}-*")
            # Get the first (and only) index name
            index_name = next(iter(settings.keys()))
            index_settings = settings[index_name]["settings"]["index"]

            assert index_settings["number_of_shards"] == "2"
            assert index_settings["number_of_replicas"] == "0"
            assert index_settings["refresh_interval"] == "5s"
        finally:
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_template(os_client, f"{unique_index_prefix}-template")

    def test_custom_mappings(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that custom mappings completely replace default mappings."""
        custom_mappings = {
            "properties": {
                "@timestamp": {"type": "date"},
                "message": {"type": "text"},
                "level": {"type": "keyword"},
                "custom_field": {"type": "keyword"},
                "numeric_field": {"type": "integer"},
            }
        }

        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=False,
            custom_mappings=custom_mappings,
        )

        try:
            sink.setup()

            # Write a record with custom fields
            sink.write_batch(
                [
                    {
                        "@timestamp": "2026-01-06T12:00:00.000000+00:00",
                        "message": "Test message",
                        "level": "INFO",
                        "custom_field": "custom_value",
                        "numeric_field": 42,
                    }
                ]
            )

            # Wait for index to be created
            wait_for_docs(os_client, f"{unique_index_prefix}-*", 1)

            # Check index mappings (use wildcard to get actual index name)
            mappings = os_client.indices.get_mapping(index=f"{unique_index_prefix}-*")
            # Get the first (and only) index name
            index_name = next(iter(mappings.keys()))
            properties = mappings[index_name]["mappings"]["properties"]

            # Verify custom fields are mapped correctly
            assert properties["custom_field"]["type"] == "keyword"
            assert properties["numeric_field"]["type"] == "integer"

            # Verify default fields like "logger" and "metadata" are NOT present
            # (since custom_mappings replaces, not merges)
            assert "logger" not in properties
            assert "metadata" not in properties
        finally:
            cleanup_indices(os_client, f"{unique_index_prefix}-*")
            cleanup_template(os_client, f"{unique_index_prefix}-template")


class TestCustomISMPolicy:
    """Tests for custom ISM policy configuration."""

    def test_custom_ism_policy_created(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test that custom ISM policy is created with correct settings."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ISMPolicy(
            rollover_min_index_age="1d",
            rollover_min_size="10gb",
            rollover_min_doc_count=1000000,
            warm_after="7d",
            force_merge_segments=1,
            delete_after="30d",
        )
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=True,
            ism_policy_name=policy_name,
            ism_policy=custom_policy,
        )

        try:
            sink.setup()

            # Check ISM policy exists with correct settings
            response = os_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/policies/{policy_name}",
            )
            assert response["_id"] == policy_name
            policy = response["policy"]

            # Verify states
            states = {s["name"]: s for s in policy["states"]}
            assert "hot" in states
            assert "warm" in states
            assert "delete" in states

            # Verify hot state rollover conditions
            hot_actions = states["hot"]["actions"]
            rollover = hot_actions[0]["rollover"]
            assert rollover["min_index_age"] == "1d"
            assert rollover["min_size"] == "10gb"
            assert rollover["min_doc_count"] == 1000000

        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")
            cleanup_ism_policy(os_client, policy_name)

    def test_ism_policy_without_delete(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test ISM policy without delete state (keep forever)."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ISMPolicy(
            rollover_min_index_age="7d",
            delete_after=None,  # Keep forever
        )
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=True,
            ism_policy_name=policy_name,
            ism_policy=custom_policy,
        )

        try:
            sink.setup()

            response = os_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/policies/{policy_name}",
            )
            policy = response["policy"]
            states = {s["name"]: s for s in policy["states"]}

            # Should not have delete state
            assert "delete" not in states
            assert "hot" in states
            assert "warm" in states
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")
            cleanup_ism_policy(os_client, policy_name)

    def test_ism_policy_without_warm(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test ISM policy without warm phase (hot -> delete)."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ISMPolicy(
            rollover_min_index_age="1d",
            warm_after=None,  # Skip warm phase
            delete_after="30d",
        )
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=True,
            ism_policy_name=policy_name,
            ism_policy=custom_policy,
        )

        try:
            sink.setup()

            response = os_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/policies/{policy_name}",
            )
            policy = response["policy"]
            states = {s["name"]: s for s in policy["states"]}

            # Should not have warm state
            assert "warm" not in states
            assert "hot" in states
            assert "delete" in states

            # Hot state should transition directly to delete
            hot_transitions = states["hot"]["transitions"]
            assert len(hot_transitions) == 1
            assert hot_transitions[0]["state_name"] == "delete"
            assert hot_transitions[0]["conditions"]["min_index_age"] == "30d"
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")
            cleanup_ism_policy(os_client, policy_name)

    def test_ism_policy_without_warm_or_delete(
        self, os_client: OpenSearch, unique_index_prefix: str
    ):
        """Test ISM policy with only hot state (no warm, no delete)."""
        policy_name = f"{unique_index_prefix}-policy"
        custom_policy = ISMPolicy(
            rollover_min_index_age="7d",
            warm_after=None,  # Skip warm phase
            delete_after=None,  # Keep forever
        )
        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=True,
            ism_policy_name=policy_name,
            ism_policy=custom_policy,
        )

        try:
            sink.setup()

            response = os_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/policies/{policy_name}",
            )
            policy = response["policy"]
            states = {s["name"]: s for s in policy["states"]}

            # Should only have hot state
            assert "hot" in states
            assert "warm" not in states
            assert "delete" not in states

            # Hot state should have no transitions
            hot_transitions = states["hot"]["transitions"]
            assert len(hot_transitions) == 0
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")
            cleanup_ism_policy(os_client, policy_name)

    def test_ism_policy_with_custom_json(self, os_client: OpenSearch, unique_index_prefix: str):
        """Test ISM policy using custom JSON policy body."""
        policy_name = f"{unique_index_prefix}-policy"

        # Custom policy JSON (other attributes should be ignored)
        custom_json = {
            "description": "My custom ISM policy",
            "default_state": "ingest",
            "states": [
                {
                    "name": "ingest",
                    "actions": [
                        {
                            "rollover": {
                                "min_index_age": "2d",
                                "min_size": "5gb",
                            }
                        }
                    ],
                    "transitions": [
                        {
                            "state_name": "search",
                            "conditions": {
                                "min_index_age": "5d",
                            },
                        }
                    ],
                },
                {
                    "name": "search",
                    "actions": [
                        {"read_only": {}},
                    ],
                    "transitions": [
                        {
                            "state_name": "delete",
                            "conditions": {
                                "min_index_age": "60d",
                            },
                        }
                    ],
                },
                {
                    "name": "delete",
                    "actions": [{"delete": {}}],
                    "transitions": [],
                },
            ],
        }

        custom_policy = ISMPolicy(
            policy_json=custom_json,
            # These should be ignored:
            rollover_min_index_age="100d",
            warm_after="200d",
            delete_after="300d",
        )

        sink = OpenSearchSink(
            client=os_client,
            index_prefix=unique_index_prefix,
            setup_ism_policy=True,
            ism_policy_name=policy_name,
            ism_policy=custom_policy,
        )

        try:
            sink.setup()

            response = os_client.transport.perform_request(
                "GET",
                f"/_plugins/_ism/policies/{policy_name}",
            )
            policy = response["policy"]

            # Verify custom policy was used
            assert policy["description"] == "My custom ISM policy"
            assert policy["default_state"] == "ingest"

            states = {s["name"]: s for s in policy["states"]}

            # Should have custom states, not default ones
            assert "ingest" in states
            assert "search" in states
            assert "delete" in states
            assert "hot" not in states
            assert "warm" not in states

            # Verify custom rollover conditions
            ingest_actions = states["ingest"]["actions"]
            rollover = ingest_actions[0]["rollover"]
            assert rollover["min_index_age"] == "2d"
            assert rollover["min_size"] == "5gb"

            # Verify ism_template was added automatically
            assert "ism_template" in policy
            assert policy["ism_template"][0]["index_patterns"] == [f"{unique_index_prefix}-*"]
        finally:
            cleanup_template(os_client, f"{unique_index_prefix}-template")
            cleanup_ism_policy(os_client, policy_name)
