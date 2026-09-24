from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from psycopg.types.json import Jsonb


INSERT_QUALITY_TEST_RESULT_SQL = """
INSERT INTO monitoring.quality_test_results (
    batch_id,
    rule_code,
    severity,
    status,
    observed_value,
    threshold_value,
    affected_row_count,
    details
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

BATCH_EXACT_DUPLICATE_COUNT_SQL = """
SELECT count(*) - count(DISTINCT source_row_hash)
FROM raw.shopee_observations
WHERE batch_id = %s
"""

BATCH_SYNCED_AT_WINDOW_VIOLATION_COUNT_SQL = """
SELECT count(*)
FROM raw.shopee_observations AS observation
JOIN monitoring.pipeline_runs AS run
  ON run.batch_id = observation.batch_id
WHERE observation.batch_id = %s
  AND (
      observation.synced_at < run.overlap_start_synced_at
      OR observation.synced_at > run.extraction_end_synced_at
  )
"""

VALID_SEVERITIES = frozenset(
    {"info", "warning", "error"}
)
VALID_STATUSES = frozenset(
    {"passed", "failed"}
)


def record_quality_test_result(
    connection,
    batch_id: UUID,
    rule_code: str,
    severity: str,
    status: str,
    observed_value: Optional[Decimal],
    threshold_value: Optional[Decimal],
    affected_row_count: int,
    details: dict,
) -> None:
    if not isinstance(rule_code, str) or not rule_code.strip():
        raise ValueError(
            "rule_code must be a non-empty string"
        )

    if severity not in VALID_SEVERITIES:
        raise ValueError(
            f"Invalid quality severity: {severity!r}"
        )

    if status not in VALID_STATUSES:
        raise ValueError(
            f"Invalid quality status: {status!r}"
        )

    if (
        isinstance(affected_row_count, bool)
        or not isinstance(affected_row_count, int)
        or affected_row_count < 0
    ):
        raise ValueError(
            "affected_row_count must be a nonnegative integer"
        )

    if not isinstance(details, dict):
        raise TypeError(
            "details must be a dictionary"
        )

    connection.execute(
        INSERT_QUALITY_TEST_RESULT_SQL,
        (
            batch_id,
            rule_code,
            severity,
            status,
            observed_value,
            threshold_value,
            affected_row_count,
            Jsonb(details),
        ),
    )


def evaluate_batch_reconciliation(
    extracted_count: int,
    loaded_count: int,
    duplicate_count: int,
    rejected_count: int,
) -> dict:
    counts = (
        extracted_count,
        loaded_count,
        duplicate_count,
        rejected_count,
    )

    if any(
        isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        for count in counts
    ):
        raise ValueError(
            "reconciliation counts must be nonnegative integers"
        )

    accounted_count = (
        loaded_count
        + duplicate_count
        + rejected_count
    )
    difference = extracted_count - accounted_count

    return {
        "rule_code": "batch_row_count_reconciliation",
        "severity": "error",
        "status": (
            "passed"
            if difference == 0
            else "failed"
        ),
        "observed_value": Decimal(difference),
        "threshold_value": Decimal(0),
        "affected_row_count": abs(difference),
        "details": {
            "extracted_count": extracted_count,
            "loaded_count": loaded_count,
            "duplicate_count": duplicate_count,
            "rejected_count": rejected_count,
            "accounted_count": accounted_count,
        },
    }


def count_batch_exact_duplicates(connection, batch_id: UUID) -> int:
    row = connection.execute(
        BATCH_EXACT_DUPLICATE_COUNT_SQL,
        (batch_id,),
    ).fetchone()
    return row[0]


def evaluate_batch_exact_duplicates(duplicate_count: int) -> dict:
    if (
        isinstance(duplicate_count, bool)
        or not isinstance(duplicate_count, int)
        or duplicate_count < 0
    ):
        raise ValueError(
            "duplicate_count must be a nonnegative integer"
        )

    return {
        "rule_code": "batch_exact_duplicate_count",
        "severity": "warning",
        "status": "passed" if duplicate_count == 0 else "failed",
        "observed_value": Decimal(duplicate_count),
        "threshold_value": Decimal(0),
        "affected_row_count": duplicate_count,
        "details": {
            "duplicate_definition": (
                "repeated source_row_hash within one raw batch"
            ),
        },
    }


def evaluate_source_freshness(
    extraction_started_at: datetime,
    source_max_synced_at: datetime,
    threshold_minutes: int,
) -> dict:
    for field_name, value in (
        ("extraction_started_at", extraction_started_at),
        ("source_max_synced_at", source_max_synced_at),
    ):
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError(
                f"{field_name} must be a timezone-aware datetime"
            )

    if (
        isinstance(threshold_minutes, bool)
        or not isinstance(threshold_minutes, int)
        or threshold_minutes < 0
    ):
        raise ValueError(
            "threshold_minutes must be a nonnegative integer"
        )

    lag_minutes = Decimal(
        str(
            (
                extraction_started_at - source_max_synced_at
            ).total_seconds()
        )
    ) / Decimal(60)
    threshold = Decimal(threshold_minutes)
    passed = lag_minutes <= threshold

    return {
        "rule_code": "source_freshness_lag_minutes",
        "severity": "warning",
        "status": "passed" if passed else "failed",
        "observed_value": lag_minutes,
        "threshold_value": threshold,
        "affected_row_count": 0 if passed else 1,
        "details": {
            "extraction_started_at": extraction_started_at.isoformat(),
            "source_max_synced_at": source_max_synced_at.isoformat(),
        },
    }


def count_batch_synced_at_window_violations(
    connection,
    batch_id: UUID,
) -> int:
    row = connection.execute(
        BATCH_SYNCED_AT_WINDOW_VIOLATION_COUNT_SQL,
        (batch_id,),
    ).fetchone()
    return row[0]


def evaluate_batch_synced_at_window_violations(
    violation_count: int,
) -> dict:
    if (
        isinstance(violation_count, bool)
        or not isinstance(violation_count, int)
        or violation_count < 0
    ):
        raise ValueError(
            "violation_count must be a nonnegative integer"
        )

    return {
        "rule_code": "batch_synced_at_window_violation_count",
        "severity": "error",
        "status": "passed" if violation_count == 0 else "failed",
        "observed_value": Decimal(violation_count),
        "threshold_value": Decimal(0),
        "affected_row_count": violation_count,
        "details": {
            "valid_range": (
                "overlap_start_synced_at <= synced_at "
                "<= extraction_end_synced_at"
            ),
        },
    }
