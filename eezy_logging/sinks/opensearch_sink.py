"""OpenSearch sink implementation for eezy-logging."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eezy_logging.sinks.base import Sink
from eezy_logging.sinks.elasticsearch_sink import LOG_MAPPINGS

if TYPE_CHECKING:
    from opensearchpy import OpenSearch

_logger = logging.getLogger("eezy_logging")

ENV_OS_HOSTS = "EEZY_OS_HOSTS"
ENV_OS_USERNAME = "EEZY_OS_USERNAME"
ENV_OS_PASSWORD = "EEZY_OS_PASSWORD"
ENV_OS_VERIFY_CERTS = "EEZY_OS_VERIFY_CERTS"

DEFAULT_OS_HOST = "http://localhost:9200"


@dataclass
class ISMPolicy:
    """Configuration for OpenSearch Index State Management (ISM) policy.

    OpenSearch uses ISM instead of ILM for index lifecycle management.
    This defines states and transitions for index management.

    Attributes:
        rollover_min_index_age: Min age before rolling over (e.g., "7d", "24h").
        rollover_min_size: Min size before rolling over (e.g., "50gb").
        rollover_min_doc_count: Min documents before rolling over.
        warm_after: Time after rollover to move to warm/read-only state.
        delete_after: Time after rollover to delete index. None to keep forever.
        force_merge_segments: Number of segments to force merge to.
        description: Description for the ISM policy.

    Example:
        # Keep logs for 30 days
        policy = ISMPolicy(
            rollover_min_index_age="1d",
            rollover_min_size="10gb",
            delete_after="30d",
        )
    """

    # Rollover conditions
    rollover_min_index_age: str = "7d"
    rollover_min_size: str = "50gb"
    rollover_min_doc_count: int | None = None

    # Warm/read-only state
    warm_after: str = "30d"
    force_merge_segments: int = 1

    # Delete state (optional - None means keep forever)
    delete_after: str | None = "90d"

    # ISM policy description
    description: str = "eezy-logging ISM policy for log index management"

    def to_policy_body(self, policy_id: str, index_patterns: list[str]) -> dict[str, Any]:
        """Convert to OpenSearch ISM policy body."""
        # Build rollover conditions
        rollover_conditions: dict[str, Any] = {
            "min_index_age": self.rollover_min_index_age,
            "min_size": self.rollover_min_size,
        }
        if self.rollover_min_doc_count:
            rollover_conditions["min_doc_count"] = self.rollover_min_doc_count

        # States: hot -> warm -> (delete)
        states = [
            {
                "name": "hot",
                "actions": [
                    {
                        "rollover": rollover_conditions,
                    }
                ],
                "transitions": [
                    {
                        "state_name": "warm",
                        "conditions": {
                            "min_index_age": self.warm_after,
                        },
                    }
                ],
            },
            {
                "name": "warm",
                "actions": [
                    {"read_only": {}},
                    {"force_merge": {"max_num_segments": self.force_merge_segments}},
                ],
                "transitions": [],
            },
        ]

        # Add delete transition if configured
        if self.delete_after:
            states[1]["transitions"] = [
                {
                    "state_name": "delete",
                    "conditions": {
                        "min_index_age": self.delete_after,
                    },
                }
            ]
            states.append(
                {
                    "name": "delete",
                    "actions": [{"delete": {}}],
                    "transitions": [],
                }
            )

        return {
            "policy": {
                "description": self.description,
                "default_state": "hot",
                "states": states,
                "ism_template": [
                    {
                        "index_patterns": index_patterns,
                        "priority": 100,
                    }
                ],
            }
        }


# Default policy
DEFAULT_ISM_POLICY = ISMPolicy()


def _create_opensearch_client() -> OpenSearch:
    """Create an OpenSearch client from environment variables or defaults."""
    from opensearchpy import OpenSearch

    hosts_str = os.environ.get(ENV_OS_HOSTS, DEFAULT_OS_HOST)
    hosts = [h.strip() for h in hosts_str.split(",")]

    verify_certs = os.environ.get(ENV_OS_VERIFY_CERTS, "true").lower() == "true"

    username = os.environ.get(ENV_OS_USERNAME)
    password = os.environ.get(ENV_OS_PASSWORD)
    if username and password:
        return OpenSearch(hosts=hosts, http_auth=(username, password), verify_certs=verify_certs)

    return OpenSearch(hosts=hosts, verify_certs=verify_certs)


class OpenSearchSink(Sink):
    """OpenSearch sink with ISM and index template support.

    Writes log records to OpenSearch using the bulk API for efficiency.
    Uses Index State Management (ISM) for automatic index rollover and deletion.

    Args:
        client: OpenSearch client instance. If not provided, a client will be
            created using environment variables or defaults.
        index_prefix: Prefix for index names. Defaults to "eezy-logs".
        index_date_format: Date format for index suffix. Defaults to "%Y.%m.%d".
            Set to None to disable date-based indices.
        setup_index_template: Whether to create an index template on setup.
            Defaults to True.
        setup_ism_policy: Whether to create an ISM policy on setup. Defaults to True.
        ism_policy_name: Name of the ISM policy. Defaults to "{index_prefix}-policy".
        ism_policy: Custom ISM policy configuration. Defaults to ISMPolicy().
        max_retries: Maximum number of retry attempts for failed writes. Defaults to 3.
        retry_delay: Initial delay between retries in seconds. Defaults to 1.0.

    Environment Variables (used when client is not provided):
        EEZY_OS_HOSTS: Comma-separated list of hosts (default: http://localhost:9200)
        EEZY_OS_USERNAME: Basic auth username
        EEZY_OS_PASSWORD: Basic auth password
        EEZY_OS_VERIFY_CERTS: Verify SSL certificates (default: true)

    Example:
        # Using environment variables or defaults
        sink = OpenSearchSink(index_prefix="myapp-logs")

        # Custom ISM policy
        from eezy_logging.sinks import OpenSearchSink, ISMPolicy

        policy = ISMPolicy(
            rollover_min_index_age="1d",
            rollover_min_size="10gb",
            delete_after="30d",
        )
        sink = OpenSearchSink(
            index_prefix="myapp-logs",
            ism_policy=policy,
        )

        # Disable ISM
        sink = OpenSearchSink(setup_ism_policy=False)
    """

    def __init__(
        self,
        client: OpenSearch | None = None,
        index_prefix: str = "eezy-logs",
        index_date_format: str | None = "%Y.%m.%d",
        setup_index_template: bool = True,
        setup_ism_policy: bool = True,
        ism_policy_name: str | None = None,
        ism_policy: ISMPolicy | None = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self._client = client
        self._owns_client = client is None
        self._index_prefix = index_prefix
        self._index_date_format = index_date_format
        self._setup_index_template = setup_index_template
        self._setup_ism_policy = setup_ism_policy
        self._ism_policy_name = ism_policy_name or f"{index_prefix}-policy"
        self._ism_policy = ism_policy or DEFAULT_ISM_POLICY
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def _get_client(self) -> OpenSearch:
        """Get or create the OpenSearch client."""
        if self._client is None:
            self._client = _create_opensearch_client()
        return self._client

    def setup(self) -> None:
        """Create index template and ISM policy if configured."""
        client = self._get_client()

        if self._setup_ism_policy:
            self._create_ism_policy(client)

        if self._setup_index_template:
            self._create_index_template(client)

    def _create_ism_policy(self, client: OpenSearch) -> None:
        """Create ISM policy for automatic index lifecycle management."""
        index_patterns = [f"{self._index_prefix}-*"]
        policy_body = self._ism_policy.to_policy_body(self._ism_policy_name, index_patterns)

        try:
            client.transport.perform_request(
                "PUT",
                f"/_plugins/_ism/policies/{self._ism_policy_name}",
                body=policy_body,
            )
            _logger.debug("eezy-logging: Created ISM policy '%s'", self._ism_policy_name)
        except Exception as e:
            _logger.debug("eezy-logging: Could not create ISM policy: %s", e)

    def _create_index_template(self, client: OpenSearch) -> None:
        """Create index template for log indices."""
        template_name = f"{self._index_prefix}-template"

        settings: dict[str, Any] = {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        }

        if self._setup_ism_policy:
            settings["plugins.index_state_management.rollover_alias"] = self._index_prefix

        try:
            # OpenSearch uses legacy template API
            client.indices.put_template(
                name=template_name,
                body={
                    "index_patterns": [f"{self._index_prefix}-*"],
                    "settings": settings,
                    "mappings": LOG_MAPPINGS,
                },
            )
            _logger.debug("eezy-logging: Created index template '%s'", template_name)
        except Exception as e:
            _logger.warning("eezy-logging: Could not create index template: %s", e)

    def _get_index_name(self) -> str:
        """Generate the current index name based on date format."""
        if self._index_date_format:
            date_suffix = datetime.now(UTC).strftime(self._index_date_format)
            return f"{self._index_prefix}-{date_suffix}"
        return self._index_prefix

    def write_batch(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of records to OpenSearch using bulk API."""
        if not records:
            return

        client = self._get_client()
        index_name = self._get_index_name()

        # Build bulk request body
        bulk_body: list[dict[str, Any]] = []
        for record in records:
            if "@timestamp" not in record:
                record["@timestamp"] = datetime.now(UTC).isoformat()

            bulk_body.append({"index": {"_index": index_name}})
            bulk_body.append(record)

        # Retry with exponential backoff
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = client.bulk(body=bulk_body, refresh=False)
                if response.get("errors"):
                    for item in response.get("items", []):
                        if "error" in item.get("index", {}):
                            _logger.warning(
                                "eezy-logging: Bulk index error: %s",
                                item["index"]["error"],
                            )
                return
            except Exception as e:
                last_error = e
                if attempt < self._max_retries - 1:
                    delay = self._retry_delay * (2**attempt)
                    _logger.warning(
                        "eezy-logging: Bulk write failed, retrying in %.1fs: %s",
                        delay,
                        e,
                    )
                    time.sleep(delay)

        if last_error:
            _logger.error(
                "eezy-logging: Failed to write %d records after %d attempts: %s",
                len(records),
                self._max_retries,
                last_error,
            )

    def close(self) -> None:
        """Close the OpenSearch client if we own it."""
        if self._owns_client and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
