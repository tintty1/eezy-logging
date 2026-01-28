"""Unit tests for ILMPolicy configuration."""

from __future__ import annotations

from eezy_logging.sinks import ILMPolicy


class TestILMPolicyConfiguration:
    """Tests for ILMPolicy configuration and to_policy_body method."""

    def test_default_policy(self):
        """Test default ILM policy configuration."""
        policy = ILMPolicy()
        body = policy.to_policy_body()

        assert "policy" in body
        p = body["policy"]

        # Check basic structure
        assert "phases" in p
        phases = p["phases"]

        # Check phases exist
        assert "hot" in phases
        assert "warm" in phases
        assert "delete" in phases

        # Check rollover conditions
        rollover = phases["hot"]["actions"]["rollover"]
        assert rollover["max_age"] == "7d"
        assert rollover["max_size"] == "50gb"

        # Check warm phase
        assert phases["warm"]["min_age"] == "30d"

        # Check delete phase
        assert phases["delete"]["min_age"] == "90d"

    def test_policy_without_warm_phase(self):
        """Test ILM policy with warm_after=None skips warm phase."""
        policy = ILMPolicy(
            rollover_max_age="1d",
            warm_after=None,  # Skip warm phase
            delete_after="30d",
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]

        # Should not have warm phase
        assert "warm" not in phases
        assert "hot" in phases
        assert "delete" in phases

    def test_policy_without_delete_phase(self):
        """Test ILM policy with delete_after=None keeps data forever."""
        policy = ILMPolicy(
            rollover_max_age="7d",
            warm_after="30d",
            delete_after=None,  # Keep forever
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]

        # Should not have delete phase
        assert "delete" not in phases
        assert "hot" in phases
        assert "warm" in phases

    def test_policy_with_only_hot_phase(self):
        """Test ILM policy with only hot phase (no warm, no cold, no delete)."""
        policy = ILMPolicy(
            rollover_max_age="7d",
            warm_after=None,
            cold_after=None,
            delete_after=None,
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]

        # Should only have hot phase
        assert len(phases) == 1
        assert "hot" in phases
        assert "warm" not in phases
        assert "cold" not in phases
        assert "delete" not in phases

    def test_policy_with_custom_json(self):
        """Test ILM policy with custom JSON overrides all other settings."""
        custom_json = {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {"max_age": "2d", "max_size": "5gb"},
                    },
                },
                "delete": {
                    "min_age": "14d",
                    "actions": {"delete": {}},
                },
            }
        }

        policy = ILMPolicy(
            policy_json=custom_json,
            # These should be ignored:
            rollover_max_age="100d",
            warm_after="200d",
            delete_after="300d",
        )
        body = policy.to_policy_body()

        p = body["policy"]

        # Verify custom policy was used
        assert "phases" in p
        phases = p["phases"]

        assert "hot" in phases
        assert "delete" in phases
        assert "warm" not in phases

        # Verify custom rollover conditions
        rollover = phases["hot"]["actions"]["rollover"]
        assert rollover["max_age"] == "2d"
        assert rollover["max_size"] == "5gb"

        # Verify custom delete phase
        assert phases["delete"]["min_age"] == "14d"

    def test_policy_with_all_rollover_conditions(self):
        """Test ILM policy with all rollover conditions specified."""
        policy = ILMPolicy(
            rollover_max_age="1d",
            rollover_max_size="10gb",
            rollover_max_docs=1000000,
            warm_after="7d",
            delete_after="30d",
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]
        rollover = phases["hot"]["actions"]["rollover"]

        assert rollover["max_age"] == "1d"
        assert rollover["max_size"] == "10gb"
        assert rollover["max_docs"] == 1000000

    def test_policy_without_doc_count(self):
        """Test that max_docs is optional and not included if None."""
        policy = ILMPolicy(
            rollover_max_age="1d",
            rollover_max_size="10gb",
            rollover_max_docs=None,
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]
        rollover = phases["hot"]["actions"]["rollover"]

        assert "max_docs" not in rollover
        assert "max_age" in rollover
        assert "max_size" in rollover

    def test_policy_with_cold_phase(self):
        """Test ILM policy with cold phase enabled."""
        policy = ILMPolicy(
            rollover_max_age="7d",
            warm_after="14d",
            cold_after="30d",
            delete_after="90d",
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]

        # All phases should exist
        assert "hot" in phases
        assert "warm" in phases
        assert "cold" in phases
        assert "delete" in phases

        # Check cold phase configuration
        assert phases["cold"]["min_age"] == "30d"
        assert "readonly" in phases["cold"]["actions"]

    def test_policy_skip_warm_with_cold(self):
        """Test ILM policy that skips warm but has cold phase."""
        policy = ILMPolicy(
            rollover_max_age="7d",
            warm_after=None,  # Skip warm
            cold_after="30d",
            delete_after="90d",
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]

        # Should have hot, cold, delete but not warm
        assert "hot" in phases
        assert "warm" not in phases
        assert "cold" in phases
        assert "delete" in phases

    def test_policy_description_field(self):
        """Test ILM policy with custom description."""
        policy = ILMPolicy(
            description="My custom ILM policy for production logs",
        )
        # Note: ILM policy doesn't include description in the body like ISM
        # but we store it for reference. The description attribute exists
        # on the dataclass.
        assert policy.description == "My custom ILM policy for production logs"

    def test_warm_phase_configuration(self):
        """Test warm phase has correct shrink and force_merge settings."""
        policy = ILMPolicy(
            warm_after="14d",
            warm_shrink_shards=2,
            warm_force_merge_segments=5,
        )
        body = policy.to_policy_body()

        phases = body["policy"]["phases"]
        warm = phases["warm"]

        assert warm["min_age"] == "14d"
        assert warm["actions"]["shrink"]["number_of_shards"] == 2
        assert warm["actions"]["forcemerge"]["max_num_segments"] == 5
