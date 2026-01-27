"""Unit tests for ISMPolicy configuration."""

from __future__ import annotations

import pytest

from eezy_logging.sinks import ISMPolicy


class TestISMPolicyConfiguration:
    """Tests for ISMPolicy configuration and to_policy_body method."""

    def test_default_policy(self):
        """Test default ISM policy configuration."""
        policy = ISMPolicy()
        body = policy.to_policy_body("test-policy", ["logs-*"])

        assert "policy" in body
        p = body["policy"]

        # Check basic structure
        assert p["default_state"] == "hot"
        assert p["description"] == "eezy-logging ISM policy for log index management"

        # Check states
        states = {s["name"]: s for s in p["states"]}
        assert "hot" in states
        assert "warm" in states
        assert "delete" in states

        # Check rollover conditions
        rollover = states["hot"]["actions"][0]["rollover"]
        assert rollover["min_index_age"] == "7d"
        assert rollover["min_size"] == "50gb"

        # Check transitions
        assert states["hot"]["transitions"][0]["state_name"] == "warm"
        assert states["hot"]["transitions"][0]["conditions"]["min_index_age"] == "30d"
        assert states["warm"]["transitions"][0]["state_name"] == "delete"
        assert states["warm"]["transitions"][0]["conditions"]["min_index_age"] == "90d"

    def test_policy_without_warm_phase(self):
        """Test ISM policy with warm_after=None skips warm phase."""
        policy = ISMPolicy(
            rollover_min_index_age="1d",
            warm_after=None,  # Skip warm phase
            delete_after="30d",
        )
        body = policy.to_policy_body("test-policy", ["logs-*"])

        states = {s["name"]: s for s in body["policy"]["states"]}

        # Should not have warm state
        assert "warm" not in states
        assert "hot" in states
        assert "delete" in states

        # Hot should transition directly to delete
        assert len(states["hot"]["transitions"]) == 1
        assert states["hot"]["transitions"][0]["state_name"] == "delete"
        assert states["hot"]["transitions"][0]["conditions"]["min_index_age"] == "30d"

    def test_policy_without_delete_phase(self):
        """Test ISM policy with delete_after=None keeps data forever."""
        policy = ISMPolicy(
            rollover_min_index_age="7d",
            warm_after="30d",
            delete_after=None,  # Keep forever
        )
        body = policy.to_policy_body("test-policy", ["logs-*"])

        states = {s["name"]: s for s in body["policy"]["states"]}

        # Should not have delete state
        assert "delete" not in states
        assert "hot" in states
        assert "warm" in states

        # Warm should have no transitions
        assert states["warm"]["transitions"] == []

    def test_policy_with_only_hot_state(self):
        """Test ISM policy with only hot state (no warm, no delete)."""
        policy = ISMPolicy(
            rollover_min_index_age="7d",
            warm_after=None,
            delete_after=None,
        )
        body = policy.to_policy_body("test-policy", ["logs-*"])

        states = {s["name"]: s for s in body["policy"]["states"]}

        # Should only have hot state
        assert len(states) == 1
        assert "hot" in states
        assert "warm" not in states
        assert "delete" not in states

        # Hot should have no transitions
        assert states["hot"]["transitions"] == []

    def test_policy_with_custom_json(self):
        """Test ISM policy with custom JSON overrides all other settings."""
        custom_json = {
            "description": "My custom policy",
            "default_state": "ingest",
            "states": [
                {
                    "name": "ingest",
                    "actions": [{"rollover": {"min_index_age": "2d", "min_size": "5gb"}}],
                    "transitions": [
                        {
                            "state_name": "search",
                            "conditions": {"min_index_age": "5d"},
                        }
                    ],
                },
                {
                    "name": "search",
                    "actions": [{"read_only": {}}],
                    "transitions": [],
                },
            ],
        }

        policy = ISMPolicy(
            policy_json=custom_json,
            # These should be ignored:
            rollover_min_index_age="100d",
            warm_after="200d",
            delete_after="300d",
        )
        body = policy.to_policy_body("test-policy", ["logs-*"])

        p = body["policy"]

        # Verify custom policy was used
        assert p["description"] == "My custom policy"
        assert p["default_state"] == "ingest"

        states = {s["name"]: s for s in p["states"]}
        assert "ingest" in states
        assert "search" in states
        assert "hot" not in states
        assert "warm" not in states

        # Verify custom rollover conditions
        rollover = states["ingest"]["actions"][0]["rollover"]
        assert rollover["min_index_age"] == "2d"
        assert rollover["min_size"] == "5gb"

    def test_policy_with_custom_json_adds_ism_template(self):
        """Test that ism_template is automatically added to custom JSON policies."""
        custom_json = {
            "description": "Custom policy without ism_template",
            "default_state": "hot",
            "states": [
                {
                    "name": "hot",
                    "actions": [{"rollover": {"min_index_age": "1d"}}],
                    "transitions": [],
                }
            ],
        }

        policy = ISMPolicy(policy_json=custom_json)
        body = policy.to_policy_body("test-policy", ["app-logs-*"])

        p = body["policy"]

        # ism_template should be added automatically
        assert "ism_template" in p
        assert len(p["ism_template"]) == 1
        assert p["ism_template"][0]["index_patterns"] == ["app-logs-*"]
        assert p["ism_template"][0]["priority"] == 100

    def test_policy_with_custom_json_preserves_existing_ism_template(self):
        """Test that existing ism_template in custom JSON is preserved."""
        custom_json = {
            "description": "Custom policy with ism_template",
            "default_state": "hot",
            "states": [
                {
                    "name": "hot",
                    "actions": [{"rollover": {"min_index_age": "1d"}}],
                    "transitions": [],
                }
            ],
            "ism_template": [
                {
                    "index_patterns": ["custom-*", "override-*"],
                    "priority": 200,
                }
            ],
        }

        policy = ISMPolicy(policy_json=custom_json)
        body = policy.to_policy_body("test-policy", ["app-logs-*"])

        p = body["policy"]

        # Should preserve existing ism_template (not add a new one)
        assert "ism_template" in p
        assert len(p["ism_template"]) == 1
        assert p["ism_template"][0]["index_patterns"] == ["custom-*", "override-*"]
        assert p["ism_template"][0]["priority"] == 200

    def test_policy_with_all_rollover_conditions(self):
        """Test ISM policy with all rollover conditions specified."""
        policy = ISMPolicy(
            rollover_min_index_age="1d",
            rollover_min_size="10gb",
            rollover_min_doc_count=1000000,
            warm_after="7d",
            delete_after="30d",
        )
        body = policy.to_policy_body("test-policy", ["logs-*"])

        states = {s["name"]: s for s in body["policy"]["states"]}
        rollover = states["hot"]["actions"][0]["rollover"]

        assert rollover["min_index_age"] == "1d"
        assert rollover["min_size"] == "10gb"
        assert rollover["min_doc_count"] == 1000000

    def test_policy_without_doc_count(self):
        """Test that min_doc_count is optional and not included if None."""
        policy = ISMPolicy(
            rollover_min_index_age="1d",
            rollover_min_size="10gb",
            rollover_min_doc_count=None,
        )
        body = policy.to_policy_body("test-policy", ["logs-*"])

        states = {s["name"]: s for s in body["policy"]["states"]}
        rollover = states["hot"]["actions"][0]["rollover"]

        assert "min_doc_count" not in rollover
        assert "min_index_age" in rollover
        assert "min_size" in rollover
