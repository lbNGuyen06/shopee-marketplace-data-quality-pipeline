from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock
from uuid import UUID

import pytest
from psycopg.types.json import Jsonb

from shopee_quality import quality


def test_record_quality_test_result() -> None:
    connection = Mock()
    batch_id = UUID("11111111-1111-1111-1111-111111111111")

    quality.record_quality_test_result(
        connection=connection,
        batch_id=batch_id,
        rule_code="batch_row_count_reconciliation",
        severity="error",
        status="passed",
        observed_value=Decimal("0"),
        threshold_value=Decimal("0"),
        affected_row_count=0,
        details={"extracted": 1, "loaded": 1},
    )

    sql, parameters = connection.execute.call_args.args

    assert "INSERT INTO monitoring.quality_test_results" in sql
    assert parameters[:7] == (
        batch_id,
        "batch_row_count_reconciliation",
        "error",
        "passed",
        Decimal("0"),
        Decimal("0"),
        0,
    )
    assert isinstance(parameters[7], Jsonb)
    assert parameters[7].obj == {
        "extracted": 1,
        "loaded": 1,
    }
    connection.commit.assert_not_called()


@pytest.mark.parametrize(
    ("severity", "status"),
    [
        ("critical", "passed"),
        ("error", "unknown"),
    ],
)
def test_record_quality_test_result_rejects_invalid_enum(
    severity: str,
    status: str,
) -> None:
    connection = Mock()

    with pytest.raises(ValueError):
        quality.record_quality_test_result(
            connection=connection,
            batch_id=UUID(
                "11111111-1111-1111-1111-111111111111"
            ),
            rule_code="test_rule",
            severity=severity,
            status=status,
            observed_value=None,
            threshold_value=None,
            affected_row_count=0,
            details={},
        )

    connection.execute.assert_not_called()


@pytest.mark.parametrize(
    ("rule_code", "affected_row_count"),
    [
        ("", 0),
        ("   ", 0),
        ("test_rule", -1),
    ],
)
def test_record_quality_test_result_rejects_invalid_identity(
    rule_code: str,
    affected_row_count: int,
) -> None:
    connection = Mock()

    with pytest.raises(ValueError):
        quality.record_quality_test_result(
            connection=connection,
            batch_id=UUID(
                "11111111-1111-1111-1111-111111111111"
            ),
            rule_code=rule_code,
            severity="error",
            status="failed",
            observed_value=None,
            threshold_value=None,
            affected_row_count=affected_row_count,
            details={},
        )

    connection.execute.assert_not_called()


def test_evaluate_batch_reconciliation_passes() -> None:
    result = quality.evaluate_batch_reconciliation(
        extracted_count=3,
        loaded_count=2,
        duplicate_count=1,
        rejected_count=0,
    )

    assert result == {
        "rule_code": "batch_row_count_reconciliation",
        "severity": "error",
        "status": "passed",
        "observed_value": Decimal("0"),
        "threshold_value": Decimal("0"),
        "affected_row_count": 0,
        "details": {
            "extracted_count": 3,
            "loaded_count": 2,
            "duplicate_count": 1,
            "rejected_count": 0,
            "accounted_count": 3,
        },
    }


def test_evaluate_batch_reconciliation_fails() -> None:
    result = quality.evaluate_batch_reconciliation(
        extracted_count=3,
        loaded_count=1,
        duplicate_count=0,
        rejected_count=0,
    )

    assert result["status"] == "failed"
    assert result["observed_value"] == Decimal("2")
    assert result["threshold_value"] == Decimal("0")
    assert result["affected_row_count"] == 2
    assert result["details"]["accounted_count"] == 1


@pytest.mark.parametrize(
    (
        "extracted_count",
        "loaded_count",
        "duplicate_count",
        "rejected_count",
    ),
    [
        (-1, 0, 0, 0),
        (1, True, 0, 0),
        (1, 1, 0, 0.5),
    ],
)
def test_evaluate_batch_reconciliation_rejects_invalid_counts(
    extracted_count,
    loaded_count,
    duplicate_count,
    rejected_count,
) -> None:
    with pytest.raises(
        ValueError,
        match="nonnegative integers",
    ):
        quality.evaluate_batch_reconciliation(
            extracted_count=extracted_count,
            loaded_count=loaded_count,
            duplicate_count=duplicate_count,
            rejected_count=rejected_count,
        )


def test_record_quality_test_result_rejects_non_mapping_details() -> None:
    connection = Mock()

    with pytest.raises(TypeError, match="dictionary"):
        quality.record_quality_test_result(
            connection=connection,
            batch_id=UUID(
                "11111111-1111-1111-1111-111111111111"
            ),
            rule_code="test_rule",
            severity="error",
            status="failed",
            observed_value=None,
            threshold_value=None,
            affected_row_count=0,
            details=[],
        )

    connection.execute.assert_not_called()


def test_count_batch_exact_duplicates_reads_raw_batch() -> None:
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = (2,)
    batch_id = UUID("11111111-1111-1111-1111-111111111111")

    actual = quality.count_batch_exact_duplicates(
        connection=connection,
        batch_id=batch_id,
    )

    assert actual == 2
    sql, parameters = connection.execute.call_args.args
    assert "count(DISTINCT source_row_hash)" in sql
    assert "FROM raw.shopee_observations" in sql
    assert parameters == (batch_id,)
    connection.commit.assert_not_called()


@pytest.mark.parametrize(
    ("duplicate_count", "expected_status"),
    [(0, "passed"), (2, "failed")],
)
def test_evaluate_batch_exact_duplicates(
    duplicate_count: int,
    expected_status: str,
) -> None:
    result = quality.evaluate_batch_exact_duplicates(duplicate_count)

    assert result == {
        "rule_code": "batch_exact_duplicate_count",
        "severity": "warning",
        "status": expected_status,
        "observed_value": Decimal(duplicate_count),
        "threshold_value": Decimal(0),
        "affected_row_count": duplicate_count,
        "details": {
            "duplicate_definition": (
                "repeated source_row_hash within one raw batch"
            ),
        },
    }


@pytest.mark.parametrize("duplicate_count", [-1, True, 1.5])
def test_evaluate_batch_exact_duplicates_rejects_invalid_count(
    duplicate_count,
) -> None:
    with pytest.raises(ValueError, match="nonnegative integer"):
        quality.evaluate_batch_exact_duplicates(duplicate_count)


@pytest.mark.parametrize(
    ("lag_minutes", "expected_status", "affected_row_count"),
    [
        (60, "passed", 0),
        (1440, "passed", 0),
        (1441, "failed", 1),
    ],
)
def test_evaluate_source_freshness(
    lag_minutes: int,
    expected_status: str,
    affected_row_count: int,
) -> None:
    extraction_started_at = datetime(
        2026, 9, 24, 10, 0, tzinfo=timezone.utc
    )
    source_max_synced_at = extraction_started_at - timedelta(
        minutes=lag_minutes
    )

    result = quality.evaluate_source_freshness(
        extraction_started_at=extraction_started_at,
        source_max_synced_at=source_max_synced_at,
        threshold_minutes=1440,
    )

    assert result["rule_code"] == "source_freshness_lag_minutes"
    assert result["severity"] == "warning"
    assert result["status"] == expected_status
    assert result["observed_value"] == Decimal(lag_minutes)
    assert result["threshold_value"] == Decimal(1440)
    assert result["affected_row_count"] == affected_row_count


@pytest.mark.parametrize(
    ("started_at", "source_max", "threshold", "message"),
    [
        (
            datetime(2026, 9, 24, 10, 0),
            datetime(2026, 9, 24, 9, 0, tzinfo=timezone.utc),
            1440,
            "extraction_started_at",
        ),
        (
            datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 9, 24, 9, 0),
            1440,
            "source_max_synced_at",
        ),
        (
            datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 9, 24, 9, 0, tzinfo=timezone.utc),
            -1,
            "threshold_minutes",
        ),
    ],
)
def test_evaluate_source_freshness_rejects_invalid_input(
    started_at,
    source_max,
    threshold,
    message,
) -> None:
    with pytest.raises(ValueError, match=message):
        quality.evaluate_source_freshness(
            extraction_started_at=started_at,
            source_max_synced_at=source_max,
            threshold_minutes=threshold,
        )


def test_count_batch_synced_at_window_violations() -> None:
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = (2,)
    batch_id = UUID("11111111-1111-1111-1111-111111111111")

    actual = quality.count_batch_synced_at_window_violations(
        connection=connection,
        batch_id=batch_id,
    )

    assert actual == 2
    sql, parameters = connection.execute.call_args.args
    assert "observation.synced_at <" in sql
    assert "observation.synced_at >" in sql
    assert parameters == (batch_id,)


@pytest.mark.parametrize(
    ("violation_count", "expected_status"),
    [(0, "passed"), (2, "failed")],
)
def test_evaluate_batch_synced_at_window_violations(
    violation_count: int,
    expected_status: str,
) -> None:
    result = quality.evaluate_batch_synced_at_window_violations(
        violation_count
    )

    assert result["rule_code"] == (
        "batch_synced_at_window_violation_count"
    )
    assert result["severity"] == "error"
    assert result["status"] == expected_status
    assert result["observed_value"] == Decimal(violation_count)
    assert result["threshold_value"] == Decimal(0)
    assert result["affected_row_count"] == violation_count


@pytest.mark.parametrize("violation_count", [-1, True, 1.5])
def test_evaluate_window_violations_rejects_invalid_count(
    violation_count,
) -> None:
    with pytest.raises(ValueError, match="nonnegative integer"):
        quality.evaluate_batch_synced_at_window_violations(
            violation_count
        )
