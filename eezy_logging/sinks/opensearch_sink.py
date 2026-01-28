"""OpenSearch sink implementation for eezy-logging."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eezy_logging.sinks.base import DEFAULT_LOG_MAPPINGS, Sink, WriteResult

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
        policy_json: Custom JSON policy body. If set, all other attributes are ignored.
            This should be the complete policy definition (without the outer "policy" key).
        rollover_min_index_age: Min age before rolling over (e.g., "7d", "24h").
        rollover_min_size: Min size before rolling over (e.g., "50gb").
        rollover_min_doc_count: Min documents before rolling over.
        warm_after: Time after rollover to move to warm/read-only state.
            Set to None to skip the warm phase entirely.
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

        # Custom JSON policy
        policy = ISMPolicy(
            policy_json={
                "description": "Custom policy",
                "default_state": "hot",
                "states": [...],
            }
        )

        # Skip warm phase
        policy = ISMPolicy(
            rollover_min_index_age="1d",
            warm_after=None,
            delete_after="30d",
        )
    """

    # Custom JSON policy (overrides all other settings)
    policy_json: dict[str, Any] | None = None

    # Rollover conditions
    rollover_min_index_age: str = "7d"
    rollover_min_size: str = "50gb"
    rollover_min_doc_count: int | None = None

    # Warm/read-only state (None to skip warm phase)
    warm_after: str | None = "30d"
    force_merge_segments: int = 1

    # Delete state (optional - None means keep forever)
    delete_after: str | None = "90d"

    # ISM policy description
    description: str = "eezy-logging ISM policy for log index management"

    def to_policy_body(self, policy_id: str, index_patterns: list[str]) -> dict[str, Any]:
        """Convert to OpenSearch ISM policy body.

        If policy_json is set, it takes precedence and other attributes are ignored.
        The ism_template is automatically added to apply the policy to matching indices.
        """
        # If custom JSON policy is provided, use it directly
        if self.policy_json:
            policy_body = dict(self.policy_json)  # Make a copy
            # Ensure ism_template is present to apply to indices
            if "ism_template" not in policy_body:
                policy_body["ism_template"] = [
                    {
                        "index_patterns": index_patterns,
                        "priority": 100,
                    }
                ]
            return {"policy": policy_body}

        # Build rollover conditions
        rollover_conditions: dict[str, Any] = {
            "min_index_age": self.rollover_min_index_age,
            "min_size": self.rollover_min_size,
        }
        if self.rollover_min_doc_count:
            rollover_conditions["min_doc_count"] = self.rollover_min_doc_count

        # Start with hot state
        states = []
        hot_state: dict[str, Any] = {
            "name": "hot",
            "actions": [
                {
                    "rollover": rollover_conditions,
                }
            ],
            "transitions": [],
        }

        # Determine the next state after hot
        next_state_after_hot = None
        if self.warm_after is not None:
            next_state_after_hot = "warm"
        elif self.delete_after is not None:
            next_state_after_hot = "delete"

        # Add transition from hot to next state
        if next_state_after_hot:
            transition_age = self.warm_after if self.warm_after else self.delete_after
            hot_state["transitions"] = [
                {
                    "state_name": next_state_after_hot,
                    "conditions": {
                        "min_index_age": transition_age,
                    },
                }
            ]

        states.append(hot_state)

        if self.warm_after is not None:
            warm_state: dict[str, Any] = {
                "name": "warm",
                "actions": [
                    {"read_only": {}},
                    {"force_merge": {"max_num_segments": self.force_merge_segments}},
                ],
                "transitions": [],
            }

            # Add delete transition if configured
            if self.delete_after:
                warm_state["transitions"] = [
                    {
                        "state_name": "delete",
                        "conditions": {
                            "min_index_age": self.delete_after,
                        },
                    }
                ]

            states.append(warm_state)

        # Add delete state if configured
        if self.delete_after:
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
            Also used as the primary alias for writing logs (required for rollover).
            When using ISM rollover, indices will have numeric suffixes (-000001, -000002).
        index_aliases: Additional aliases to assign to indices. The index_prefix
            is always included as an alias (required for rollover). Specify additional
            aliases here if needed. Example: ["logs", "app-logs"].
        setup_index_template: Whether to create an index template on setup.
            Defaults to True.
        setup_ism_policy: Whether to create an ISM policy on setup. Defaults to True.
        ism_policy_name: Name of the ISM policy. Defaults to "{index_prefix}-policy".
        ism_policy: Custom ISM policy configuration. Defaults to ISMPolicy().
            Can use policy_json for complete control, or warm_after=None to skip
            the warm phase.
        custom_index_settings: Custom index settings to merge with defaults.
            These will override default settings like number_of_shards and
            number_of_replicas. Example: {"refresh_interval": "30s"}
        custom_mappings: Custom field mappings to replace the defaults.
            If provided, completely replaces DEFAULT_LOG_MAPPINGS.
            Use with a custom serializer to ensure your log records match.
            See eezy_logging.sinks.base.DEFAULT_LOG_MAPPINGS for the default.

    Note:
        Retry logic is handled by the Worker, not the sink. Configure retries
        via the Worker's max_retries and retry_base_delay parameters.

    Environment Variables (used when client is not provided):
        EEZY_OS_HOSTS: Comma-separated list of hosts (default: http://localhost:9200)
        EEZY_OS_USERNAME: Basic auth username
        EEZY_OS_PASSWORD: Basic auth password
        EEZY_OS_VERIFY_CERTS: Verify SSL certificates (default: true)

    Example:
        # Using environment variables or defaults
        sink = OpenSearchSink(index_prefix="myapp-logs")

        # Additional aliases (index_prefix is always included)
        sink = OpenSearchSink(
            index_prefix="myapp-logs",
            index_aliases=["logs", "production-logs"],  # myapp-logs also included
        )

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

        # Skip warm phase (hot -> delete)
        policy = ISMPolicy(
            rollover_min_index_age="1d",
            warm_after=None,  # Skip warm phase
            delete_after="30d",
        )
        sink = OpenSearchSink(
            index_prefix="myapp-logs",
            ism_policy=policy,
        )

        # Custom JSON policy
        policy = ISMPolicy(
            policy_json={
                "description": "Custom policy",
                "default_state": "hot",
                "states": [...],
            }
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
        index_aliases: list[str] | None = None,
        setup_index_template: bool = True,
        setup_ism_policy: bool = True,
        ism_policy_name: str | None = None,
        ism_policy: ISMPolicy | None = None,
        custom_index_settings: dict[str, Any] | None = None,
        custom_mappings: dict[str, Any] | None = None,
        # Deprecated: retry logic is now handled by Worker
        max_retries: int = 3,  # noqa: ARG002
        retry_delay: float = 1.0,  # noqa: ARG002
    ) -> None:
        self._client = client
        self._owns_client = client is None
        self._index_prefix = index_prefix

        # Always include index_prefix in aliases for rollover support
        # Additional aliases can be provided via index_aliases
        if index_aliases is None:
            self._index_aliases = [index_prefix]
        else:
            # Ensure index_prefix is always in the aliases list
            self._index_aliases = list(index_aliases)
            if index_prefix not in self._index_aliases:
                self._index_aliases.insert(0, index_prefix)

        self._setup_index_template = setup_index_template
        self._setup_ism_policy = setup_ism_policy
        self._ism_policy_name = ism_policy_name or f"{index_prefix}-policy"
        self._ism_policy = ism_policy or DEFAULT_ISM_POLICY
        self._custom_index_settings = custom_index_settings or {}
        self._custom_mappings = custom_mappings or {}

    def _get_client(self) -> OpenSearch:
        """Get or create the OpenSearch client."""
        if self._client is None:
            self._client = _create_opensearch_client()
        return self._client

    def setup(self) -> None:
        """Create index template, ISM policy, and initial index if configured."""
        client = self._get_client()

        if self._setup_ism_policy:
            self._create_ism_policy(client)

        if self._setup_index_template:
            self._create_index_template(client)

        # Create initial index with write alias if it doesn't exist
        self._create_initial_index(client)

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

        # Merge custom settings (will override defaults)
        settings.update(self._custom_index_settings)

        # Use custom mappings if provided, otherwise use defaults
        mappings = self._custom_mappings if self._custom_mappings else DEFAULT_LOG_MAPPINGS

        # Build template body
        template_body: dict[str, Any] = {
            "index_patterns": [f"{self._index_prefix}-*"],
            "settings": settings,
            "mappings": mappings,
        }

        # Add aliases if configured (excluding index_prefix which is the rollover alias)
        # The rollover alias is managed separately via _create_initial_index with is_write_index
        non_rollover_aliases = [a for a in self._index_aliases if a != self._index_prefix]
        if non_rollover_aliases:
            template_body["aliases"] = {alias: {} for alias in non_rollover_aliases}

        try:
            # OpenSearch uses legacy template API
            client.indices.put_template(
                name=template_name,
                body=template_body,
            )
            _logger.debug("eezy-logging: Created index template '%s'", template_name)
        except Exception as e:
            _logger.warning("eezy-logging: Could not create index template: %s", e)

    def _create_initial_index(self, client: OpenSearch) -> None:
        """Create initial index with write alias if it doesn't exist.

        This is required for rollover to work. The alias must point to an index
        with is_write_index=true before any documents can be written.

        Only creates an initial index if the alias doesn't exist yet.
        If the alias exists (from previous rollovers), we don't touch anything.
        """
        # Check if alias already exists - if so, setup is already complete
        try:
            if client.indices.exists_alias(name=self._index_prefix):
                _logger.debug(
                    "eezy-logging: Alias '%s' already exists, skipping initial index creation",
                    self._index_prefix,
                )
                return
        except Exception:
            pass  # Alias doesn't exist, continue with creation

        # Create initial index with -000001 suffix
        initial_index = f"{self._index_prefix}-000001"

        try:
            client.indices.create(
                index=initial_index,
                body={
                    "aliases": {
                        self._index_prefix: {"is_write_index": True},
                    }
                },
            )
            _logger.debug(
                "eezy-logging: Created initial index '%s' with write alias '%s'",
                initial_index,
                self._index_prefix,
            )
        except Exception as e:
            # Index might already exist from a previous partial setup, or other error
            _logger.error("eezy-logging: Could not create initial index: %s", e)

    def write_batch(self, records: list[dict[str, Any]]) -> WriteResult:
        """Write a batch of records to OpenSearch using bulk API.

        This method makes a single write attempt and returns immediately.
        Failed records are returned in the WriteResult for the worker to
        schedule retries without blocking.

        Records are written to the index_prefix alias, which supports rollover.
        """
        if not records:
            return WriteResult.ok()

        client = self._get_client()
        # Write to the alias (index_prefix) for rollover support
        index_name = self._index_prefix

        # Build bulk request body
        bulk_body: list[dict[str, Any]] = []
        for record in records:
            if "@timestamp" not in record:
                record["@timestamp"] = datetime.now(UTC).isoformat()

            bulk_body.append({"index": {"_index": index_name}})
            bulk_body.append(record)

        try:
            response = client.bulk(body=bulk_body)
            if response.get("errors"):
                # Collect failed records for retry
                failed_records: list[dict[str, Any]] = []
                errors: list[str] = []

                items = response.get("items", [])
                for i, item in enumerate(items):
                    index_result = item.get("index", {})
                    if "error" in index_result:
                        # Get the original record (every other item in bulk_body)
                        if i < len(records):
                            failed_records.append(records[i])
                        errors.append(str(index_result["error"]))

                if failed_records:
                    return WriteResult(failed_records=failed_records, errors=errors)

            return WriteResult.ok()

        except Exception as e:
            # Return all records as failed for retry
            return WriteResult.failure(records, str(e))

    def close(self) -> None:
        """Close the OpenSearch client if we own it."""
        if self._owns_client and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
