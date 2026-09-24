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
