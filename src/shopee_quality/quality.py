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
