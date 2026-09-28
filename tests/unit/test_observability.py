import json
import logging
from datetime import datetime, timezone
from io import StringIO
from uuid import UUID

import pytest

from shopee_quality import observability


def test_log_event_writes_json_with_safe_fields() -> None:
    stream = StringIO()
    observability.configure_logging(stream=stream)
    batch_id = UUID("11111111-1111-1111-1111-111111111111")

    observability.log_event(
        "pipeline_succeeded",
        batch_id=batch_id,
        extracted_count=2,
        loaded_count=1,
        rejected_count=1,
        source_record={"phone": "private-phone"},
        password="private-password",
    )

    payload = json.loads(stream.getvalue())
    assert payload["event"] == "pipeline_succeeded"
    assert payload["level"] == "INFO"
    assert payload["batch_id"] == str(batch_id)
    assert payload["extracted_count"] == 2
    assert payload["loaded_count"] == 1
    assert payload["rejected_count"] == 1
    assert datetime.fromisoformat(payload["timestamp"]).tzinfo is not None
    assert "source_record" not in payload
    assert "password" not in payload
    assert "private-phone" not in stream.getvalue()
    assert "private-password" not in stream.getvalue()


def test_configure_logging_does_not_duplicate_managed_handler() -> None:
    stream = StringIO()
    observability.configure_logging(stream=stream)
    observability.configure_logging(stream=stream)

    observability.log_event("pipeline_started")

    assert len(stream.getvalue().splitlines()) == 1


def test_log_event_records_error_type_without_exception_message() -> None:
    stream = StringIO()
    observability.configure_logging(stream=stream)

    observability.log_event(
        "pipeline_failed",
        level=logging.ERROR,
        error_type="ValueError",
        exception_message="private source value",
    )

    payload = json.loads(stream.getvalue())
    assert payload["level"] == "ERROR"
    assert payload["error_type"] == "ValueError"
    assert "exception_message" not in payload
    assert "private source value" not in stream.getvalue()


def test_log_event_rejects_empty_event() -> None:
    with pytest.raises(ValueError, match="event"):
        observability.log_event(" ")
