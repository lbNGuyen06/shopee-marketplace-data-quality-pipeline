from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


LATEST_COMMITTED_WATERMARK_SQL = """
SELECT committed_watermark
FROM monitoring.pipeline_runs
WHERE source_name = %s
  AND status = 'succeeded'
  AND committed_watermark IS NOT NULL
ORDER BY committed_watermark DESC
LIMIT 1
"""


@dataclass(frozen=True)
class ExtractionWindow:
    start_synced_at: datetime
    end_synced_at: datetime


def build_extraction_window(
    committed_watermark: Optional[datetime],
    extraction_end: datetime,
    overlap_minutes: int,
    initial_start: Optional[datetime] = None,
) -> ExtractionWindow:
    if (
        isinstance(overlap_minutes, bool)
        or not isinstance(overlap_minutes, int)
        or overlap_minutes < 0
    ):
        raise ValueError(
            "overlap_minutes must be a nonnegative integer"
        )

    if (
        not isinstance(extraction_end, datetime)
        or extraction_end.tzinfo is None
        or extraction_end.utcoffset() is None
    ):
        raise ValueError(
            "extraction_end must be a timezone-aware datetime"
        )

    if committed_watermark is None:
        if initial_start is None:
            raise ValueError(
                "initial_start is required when no watermark exists"
            )

        if (
            not isinstance(initial_start, datetime)
            or initial_start.tzinfo is None
            or initial_start.utcoffset() is None
        ):
            raise ValueError(
                "initial_start must be a timezone-aware datetime"
            )

        start_synced_at = initial_start
    else:
        if (
            not isinstance(committed_watermark, datetime)
            or committed_watermark.tzinfo is None
            or committed_watermark.utcoffset() is None
        ):
            raise ValueError(
                "committed_watermark must be a timezone-aware datetime"
            )

        start_synced_at = (
            committed_watermark
            - timedelta(minutes=overlap_minutes)
        )

    if start_synced_at > extraction_end:
        raise ValueError(
            "extraction window start cannot be after extraction_end"
        )

    return ExtractionWindow(
        start_synced_at=start_synced_at,
        end_synced_at=extraction_end,
    )


def get_latest_committed_watermark(
    connection,
    source_name: str,
) -> Optional[datetime]:
    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError(
            "source_name must be a non-empty string"
        )

    row = connection.execute(
        LATEST_COMMITTED_WATERMARK_SQL,
        (source_name,),
    ).fetchone()

    if row is None:
        return None

    return row[0]


def resolve_extraction_window(
    connection,
    source_name: str,
    extraction_end: datetime,
    overlap_minutes: int,
    initial_start: datetime,
) -> ExtractionWindow:
    committed_watermark = get_latest_committed_watermark(
        connection=connection,
        source_name=source_name,
    )

    return build_extraction_window(
        committed_watermark=committed_watermark,
        extraction_end=extraction_end,
        overlap_minutes=overlap_minutes,
        initial_start=initial_start,
    )
