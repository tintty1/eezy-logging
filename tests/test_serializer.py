"""Tests for log record serialization."""

import logging

from eezy_logging.serializer import serialize_record


class TestSerializeRecord:
    """Tests for serialize_record function."""

    def test_basic_serialization(self):
        """Test serialization of a basic log record."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        data = serialize_record(record)

        assert data["message"] == "Test message"
        assert data["level"] == "INFO"
        assert data["logger"] == "test.logger"
        assert "@timestamp" in data

        # Metadata should be nested
        assert "metadata" in data
        assert data["metadata"]["levelno"] == logging.INFO
        assert data["metadata"]["lineno"] == 42

    def test_message_with_args(self):
        """Test serialization of record with message arguments."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Hello %s, you have %d messages",
            args=("Alice", 5),
            exc_info=None,
        )

        data = serialize_record(record)
        assert data["message"] == "Hello Alice, you have 5 messages"

    def test_extra_fields_at_top_level(self):
        """Test that extra fields are included at top level, not in metadata."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        # Add extra fields
        record.user_id = 123
        record.request_id = "abc-123"
        record.custom_metadata = {"key": "value"}

        data = serialize_record(record)

        # Extra fields should be at top level
        assert data["user_id"] == 123
        assert data["request_id"] == "abc-123"
        assert data["custom_metadata"] == {"key": "value"}

        # Not inside metadata
        assert "user_id" not in data["metadata"]
        assert "request_id" not in data["metadata"]

    def test_exception_info(self):
        """Test serialization of exception info."""
        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname="/path/to/file.py",
            lineno=42,
            msg="An error occurred",
            args=(),
            exc_info=exc_info,
        )

        data = serialize_record(record)

        assert "exc_info" in data
        assert "ValueError: Test error" in data["exc_info"]
        assert "Traceback" in data["exc_info"]

    def test_timestamp_format(self):
        """Test that timestamp is in ISO format."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )

        data = serialize_record(record)

        # Should be ISO 8601 format with timezone
        timestamp = data["@timestamp"]
        assert "T" in timestamp
        assert "+" in timestamp or "Z" in timestamp

    def test_non_serializable_extra_field(self):
        """Test that non-serializable extra fields are converted to string."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )

        # Add a non-serializable field
        class CustomObject:
            def __str__(self):
                return "custom_object_str"

        record.custom = CustomObject()

        data = serialize_record(record)

        assert data["custom"] == "custom_object_str"

    def test_metadata_contains_all_debug_fields(self):
        """Test that metadata contains all debugging context fields."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.WARNING,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )

        data = serialize_record(record)

        metadata_fields = [
            "hostname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "funcName",
            "lineno",
            "process",
            "processName",
            "thread",
            "threadName",
        ]

        for field in metadata_fields:
            assert field in data["metadata"], f"Missing metadata field: {field}"

        # hostname should be a non-empty string
        assert isinstance(data["metadata"]["hostname"], str)
        assert len(data["metadata"]["hostname"]) > 0

    def test_top_level_fields(self):
        """Test that top-level structure is clean."""
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )

        data = serialize_record(record)

        # These should be at top level
        assert "@timestamp" in data
        assert "message" in data
        assert "level" in data
        assert "logger" in data
        assert "metadata" in data

        # These should NOT be at top level (they're in metadata)
        assert "levelno" not in data
        assert "lineno" not in data
        assert "pathname" not in data
        assert "thread" not in data
