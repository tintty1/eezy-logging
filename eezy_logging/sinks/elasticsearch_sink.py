"""Elasticsearch sink implementation for eezy-logging."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from eezy_logging.sinks.base import Sink, WriteResult

if TYPE_CHECKING:
    from elasticsearch import Elasticsearch

_logger = logging.getLogger("eezy_logging")

ENV_ES_HOSTS = "EEZY_ES_HOSTS"
ENV_ES_USERNAME = "EEZY_ES_USERNAME"
ENV_ES_PASSWORD = "EEZY_ES_PASSWORD"
ENV_ES_API_KEY = "EEZY_ES_API_KEY"
ENV_ES_VERIFY_CERTS = "EEZY_ES_VERIFY_CERTS"

DEFAULT_ES_HOST = "http://localhost:9200"


@lru_cache(maxsize=1)
def _get_es_client_major_version() -> int:
    """Get the major version of the installed elasticsearch client.

    Result is cached since the version won't change during runtime.
    """
    try:
        import elasticsearch

        version = elasticsearch.__version__
        if isinstance(version, tuple):
            return version[0]
        # Handle string version like "8.0.0"
        return int(str(version).split(".")[0])
    except Exception:
        return 8  # Default to ES 8.x behavior


@dataclass
class ILMPolicy:
    """Configuration for Elasticsearch Index Lifecycle Management (ILM) policy.

    This defines how indices transition through lifecycle phases:
    hot -> warm -> cold -> delete

    Attributes:
        rollover_max_age: Max age before rolling over to a new index (e.g., "7d", "24h").
        rollover_max_size: Max size before rolling over (e.g., "50gb", "10gb").
        rollover_max_docs: Max documents before rolling over (e.g., 1000000).
        warm_after: Time after rollover to move to warm phase (e.g., "30d").
        warm_shrink_shards: Number of shards to shrink to in warm phase.
        warm_force_merge_segments: Number of segments to force merge to in warm phase.
        cold_after: Time after rollover to move to cold phase (e.g., "60d"). None to skip.
        delete_after: Time after rollover to delete index (e.g., "90d"). None to keep forever.

    Example:
        # Keep logs for 30 days, delete after
        policy = ILMPolicy(
            rollover_max_age="1d",
            rollover_max_size="10gb",
            delete_after="30d",
        )

        # Long retention with cold storage
        policy = ILMPolicy(
            rollover_max_age="7d",
            warm_after="14d",
            cold_after="30d",
            delete_after="365d",
        )
    """

    # Hot phase - rollover conditions
    rollover_max_age: str = "7d"
    rollover_max_size: str = "50gb"
    rollover_max_docs: int | None = None

    # Warm phase
    warm_after: str = "30d"
    warm_shrink_shards: int = 1
    warm_force_merge_segments: int = 1

    # Cold phase (optional)
    cold_after: str | None = None

    # Delete phase (optional - None means keep forever)
    delete_after: str | None = "90d"

    def to_policy_body(self) -> dict[str, Any]:
        """Convert to Elasticsearch ILM policy body."""
        # Hot phase with rollover
        rollover_conditions: dict[str, Any] = {
            "max_age": self.rollover_max_age,
            "max_size": self.rollover_max_size,
        }
        if self.rollover_max_docs:
            rollover_conditions["max_docs"] = self.rollover_max_docs

        phases: dict[str, Any] = {
            "hot": {
                "min_age": "0ms",
                "actions": {
                    "rollover": rollover_conditions,
                },
            },
        }

        phases["warm"] = {
            "min_age": self.warm_after,
            "actions": {
                "shrink": {"number_of_shards": self.warm_shrink_shards},
                "forcemerge": {"max_num_segments": self.warm_force_merge_segments},
            },
        }

        if self.cold_after:
            phases["cold"] = {
                "min_age": self.cold_after,
                "actions": {
                    "readonly": {},
                },
            }

        if self.delete_after:
            phases["delete"] = {
                "min_age": self.delete_after,
                "actions": {"delete": {}},
            }

        return {"policy": {"phases": phases}}


DEFAULT_ILM_POLICY = ILMPolicy()


def _create_es_client() -> Elasticsearch:
    """Create an Elasticsearch client from environment variables or defaults."""
    from elasticsearch import Elasticsearch

    hosts_str = os.environ.get(ENV_ES_HOSTS, DEFAULT_ES_HOST)
    hosts = [h.strip() for h in hosts_str.split(",")]

    verify_certs = os.environ.get(ENV_ES_VERIFY_CERTS, "true").lower() == "true"

    api_key = os.environ.get(ENV_ES_API_KEY)
    if api_key:
        return Elasticsearch(hosts=hosts, api_key=api_key, verify_certs=verify_certs)

    username = os.environ.get(ENV_ES_USERNAME)
    password = os.environ.get(ENV_ES_PASSWORD)
    if username and password:
        return Elasticsearch(
            hosts=hosts, basic_auth=(username, password), verify_certs=verify_certs
        )

    return Elasticsearch(hosts=hosts, verify_certs=verify_certs)


# Standard log record mappings shared between sinks
LOG_MAPPINGS: dict[str, Any] = {
    "properties": {
        "@timestamp": {"type": "date"},
        "message": {"type": "text"},
        "level": {"type": "keyword"},
        "logger": {"type": "keyword"},
        "metadata": {
            "type": "object",
            "properties": {
                "hostname": {"type": "keyword"},
                "levelno": {"type": "integer"},
                "pathname": {"type": "keyword"},
                "filename": {"type": "keyword"},
                "module": {"type": "keyword"},
                "funcName": {"type": "keyword"},
                "lineno": {"type": "integer"},
                "process": {"type": "integer"},
                "processName": {"type": "keyword"},
                "thread": {"type": "long"},
                "threadName": {"type": "keyword"},
            },
        },
        "exc_info": {"type": "text"},
        "stack_info": {"type": "text"},
    },
    "dynamic_templates": [
        {
            "strings_as_keywords": {
                "match_mapping_type": "string",
                "mapping": {"type": "keyword", "ignore_above": 1024},
            }
        }
    ],
}


class ElasticsearchSink(Sink):
    """Elasticsearch sink with ILM and index template support.

    Writes log records to Elasticsearch using the bulk API for efficiency.
    Uses Index Lifecycle Management (ILM) for automatic index rollover and deletion.

    Args:
        client: Elasticsearch client instance. If not provided, a client will be
            created using environment variables or defaults.
        index_prefix: Prefix for index names. Defaults to "eezy-logs".
        index_date_format: Date format for index suffix. Defaults to "%Y.%m.%d".
            Set to None to disable date-based indices.
        setup_index_template: Whether to create an index template on setup.
            Defaults to True.
        setup_ilm_policy: Whether to create an ILM policy on setup. Defaults to True.
        ilm_policy_name: Name of the ILM policy. Defaults to "{index_prefix}-policy".
        ilm_policy: Custom ILM policy configuration. Defaults to ILMPolicy().

    Note:
        Retry logic is handled by the Worker, not the sink. Configure retries
        via the Worker's max_retries and retry_base_delay parameters.

    Environment Variables (used when client is not provided):
        EEZY_ES_HOSTS: Comma-separated list of hosts (default: http://localhost:9200)
        EEZY_ES_USERNAME: Basic auth username
        EEZY_ES_PASSWORD: Basic auth password
        EEZY_ES_API_KEY: API key for authentication
        EEZY_ES_VERIFY_CERTS: Verify SSL certificates (default: true)

    Note:
        For Elastic Cloud deployments, create and pass your own client:
            client = Elasticsearch(cloud_id="...", api_key="...")
            sink = ElasticsearchSink(client=client)

    Example:
        # Using environment variables or defaults
        sink = ElasticsearchSink(index_prefix="myapp-logs")

        # Custom ILM policy
        from eezy_logging.sinks import ElasticsearchSink, ILMPolicy

        policy = ILMPolicy(
            rollover_max_age="1d",
            rollover_max_size="10gb",
            delete_after="30d",
        )
        sink = ElasticsearchSink(
            index_prefix="myapp-logs",
            ilm_policy=policy,
        )

        # Disable ILM
        sink = ElasticsearchSink(setup_ilm_policy=False)
    """

    def __init__(
        self,
        client: Elasticsearch | None = None,
        index_prefix: str = "eezy-logs",
        index_date_format: str | None = "%Y.%m.%d",
        setup_index_template: bool = True,
        setup_ilm_policy: bool = True,
        ilm_policy_name: str | None = None,
        ilm_policy: ILMPolicy | None = None,
        # Deprecated: retry logic is now handled by Worker
        max_retries: int = 3,  # noqa: ARG002
        retry_delay: float = 1.0,  # noqa: ARG002
    ) -> None:
        self._client = client
        self._owns_client = client is None
        self._index_prefix = index_prefix
        self._index_date_format = index_date_format
        self._setup_index_template = setup_index_template
        self._setup_ilm_policy = setup_ilm_policy
        self._ilm_policy_name = ilm_policy_name or f"{index_prefix}-policy"
        self._ilm_policy = ilm_policy or DEFAULT_ILM_POLICY

    def _get_client(self) -> Elasticsearch:
        """Get or create the Elasticsearch client."""
        if self._client is None:
            self._client = _create_es_client()
        return self._client

    def setup(self) -> None:
        """Create index template and ILM policy if configured."""
        client = self._get_client()

        if self._setup_ilm_policy:
            self._create_ilm_policy(client)

        if self._setup_index_template:
            self._create_index_template(client)

    def _create_ilm_policy(self, client: Elasticsearch) -> None:
        """Create ILM policy for automatic index lifecycle management."""
        policy_body = self._ilm_policy.to_policy_body()

        try:
            es_major_version = _get_es_client_major_version()
            if es_major_version >= 8:
                client.ilm.put_lifecycle(name=self._ilm_policy_name, body=policy_body)
            else:
                # ES 7.x uses 'policy' parameter instead of 'name'
                client.ilm.put_lifecycle(policy=self._ilm_policy_name, body=policy_body)
            _logger.debug("eezy-logging: Created ILM policy '%s'", self._ilm_policy_name)
        except Exception as e:
            _logger.debug("eezy-logging: Could not create ILM policy: %s", e)

    def _create_index_template(self, client: Elasticsearch) -> None:
        """Create index template for log indices.

        Uses composable templates (put_index_template) for ES 8.x+ and
        legacy templates (put_template) for ES 7.x.
        """
        template_name = f"{self._index_prefix}-template"

        settings: dict[str, Any] = {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        }

        if self._setup_ilm_policy:
            settings["index.lifecycle.name"] = self._ilm_policy_name

        es_major_version = _get_es_client_major_version()

        if es_major_version >= 8:
            # ES 8.x+ uses composable templates
            try:
                template_body: dict[str, Any] = {
                    "index_patterns": [f"{self._index_prefix}-*"],
                    "template": {
                        "settings": settings,
                        "mappings": LOG_MAPPINGS,
                    },
                    "priority": 100,
                }
                client.indices.put_index_template(name=template_name, body=template_body)
                _logger.debug("eezy-logging: Created index template '%s'", template_name)
            except Exception as e:
                _logger.warning("eezy-logging: Could not create index template: %s", e)
        else:
            # ES 7.x uses legacy templates
            try:
                legacy_body: dict[str, Any] = {
                    "index_patterns": [f"{self._index_prefix}-*"],
                    "settings": settings,
                    "mappings": LOG_MAPPINGS,
                }
                client.indices.put_template(name=template_name, body=legacy_body)
                _logger.debug("eezy-logging: Created legacy index template '%s'", template_name)
            except Exception as e:
                _logger.warning("eezy-logging: Could not create index template: %s", e)

    def _get_index_name(self) -> str:
        """Generate the current index name based on date format."""
        if self._index_date_format:
            date_suffix = datetime.now(UTC).strftime(self._index_date_format)
            return f"{self._index_prefix}-{date_suffix}"
        return self._index_prefix

    def write_batch(self, records: list[dict[str, Any]]) -> WriteResult:
        """Write a batch of records to Elasticsearch using bulk API.

        This method makes a single write attempt and returns immediately.
        Failed records are returned in the WriteResult for the worker to
        schedule retries without blocking.
        """
        if not records:
            return WriteResult.ok()

        client = self._get_client()
        index_name = self._get_index_name()

        # Build bulk request body
        bulk_body: list[dict[str, Any]] = []
        for record in records:
            if "@timestamp" not in record:
                record["@timestamp"] = datetime.now(UTC).isoformat()

            bulk_body.append({"index": {"_index": index_name}})
            bulk_body.append(record)

        try:
            response = client.bulk(body=bulk_body, refresh=False)
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
        """Close the Elasticsearch client if we own it."""
        if self._owns_client and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
