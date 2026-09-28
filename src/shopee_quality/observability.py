import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional, TextIO


LOGGER_NAME = "shopee_quality"
SAFE_LOG_FIELDS = frozenset(
    {
        "batch_id",
        "source_name",
        "window_start",
        "window_end",
        "schema_column_count",
        "extracted_count",
        "loaded_count",
        "duplicate_count",
        "rejected_count",
        "quality_warning_count",
        "duration_seconds",
        "error_type",
    }
)


def _json_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


class JsonEventFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(
            record.created,
            tz=timezone.utc,
        ).isoformat()
        payload = {
            "timestamp": timestamp,
            "level": record.levelname,
            "event": getattr(record, "event", record.getMessage()),
        }
        fields = getattr(record, "structured_fields", {})
        payload.update(
            {
                key: _json_value(value)
                for key, value in fields.items()
                if key in SAFE_LOG_FIELDS and value is not None
            }
        )
        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


def configure_logging(
    stream: Optional[TextIO] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False

    for handler in list(logger.handlers):
        if getattr(handler, "shopee_quality_handler", False):
            logger.removeHandler(handler)

    handler = logging.StreamHandler(stream or sys.stderr)
    handler.shopee_quality_handler = True
    handler.setFormatter(JsonEventFormatter())
    logger.addHandler(handler)
    return logger


def log_event(
    event: str,
    level: int = logging.INFO,
    **fields,
) -> None:
    if not isinstance(event, str) or not event.strip():
        raise ValueError("event must be a non-empty string")

    safe_fields = {
        key: value
        for key, value in fields.items()
        if key in SAFE_LOG_FIELDS
    }
    logging.getLogger(LOGGER_NAME).log(
        level,
        event,
        extra={
            "event": event,
            "structured_fields": safe_fields,
        },
    )
