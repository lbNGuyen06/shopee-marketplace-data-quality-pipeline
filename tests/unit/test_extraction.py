from datetime import datetime, timezone
from decimal import Decimal

import pytest

from shopee_quality import extraction
from shopee_quality.hashing import (
    CANONICAL_FIELDS,
    DATETIME_FIELDS,
    DECIMAL_FIELDS,
    INTEGER_FIELDS,
    compute_source_row_hash,
)
from shopee_quality.incremental import ExtractionWindow


def test_build_source_query_uses_parameterized_synced_at_window() -> None:
    window = ExtractionWindow(
        start_synced_at=datetime(
            2026,
            9,
            18,
            9,
            50,
            tzinfo=timezone.utc,
        ),
        end_synced_at=datetime(
            2026,
            9,
            18,
            10,
            30,
            tzinfo=timezone.utc,
        ),
    )

    sql, parameters = extraction.build_source_query(window)

    assert "FROM [vietnam_ecommerce].[shopee_orders]" in sql
    assert "WHERE [synced_at] >= ?" in sql
    assert "AND [synced_at] <= ?" in sql
    assert "ORDER BY [synced_at]" in sql
    assert sql.count("?") == 2
    assert parameters == (
        window.start_synced_at,
        window.end_synced_at,
    )


def test_build_source_query_selects_hash_contract_fields() -> None:
    window = ExtractionWindow(
        start_synced_at=datetime.now(timezone.utc),
        end_synced_at=datetime.now(timezone.utc),
    )

    sql, _ = extraction.build_source_query(window)

    assert "[pkId]" in sql
    assert "[synced_at]" in sql
    assert "[is_primary_row]" in sql
    assert "SELECT *" not in sql.upper()


def test_source_query_preserves_datetime2_7_precision() -> None:
    for field_name in DATETIME_FIELDS:
        assert (
            f"CONVERT(varchar(19), [{field_name}], 126) + '.' + "
            f"RIGHT('0000000' + CONVERT(varchar(7), "
            f"DATEPART(NANOSECOND, [{field_name}]) / 100), 7) "
            f"AS [{field_name}]"
        ) in extraction.SOURCE_QUERY


def build_source_row() -> tuple:
    values = []

    for field_name in CANONICAL_FIELDS:
        if field_name in DATETIME_FIELDS:
            values.append("2026-01-02T07:04:05.7654321")
        elif field_name in DECIMAL_FIELDS:
            values.append(Decimal("100.230"))
        elif field_name in INTEGER_FIELDS:
            values.append(1)
        else:
            values.append(f"value-{field_name}")

    return tuple(values)


def test_source_row_to_record_maps_all_contract_fields() -> None:
    source_record = extraction.source_row_to_record(build_source_row())

    assert tuple(source_record) == CANONICAL_FIELDS
    assert len(source_record) == 84
    assert source_record["original_price"] == "100.230"
    assert source_record["synced_at"] == (
        "2026-01-02T07:04:05.7654321"
    )


def test_source_row_to_record_produces_stable_hash() -> None:
    source_record = extraction.source_row_to_record(build_source_row())

    first_hash = compute_source_row_hash(source_record)
    second_hash = compute_source_row_hash(source_record)

    assert len(first_hash) == 64
    assert first_hash == second_hash


def test_source_row_to_record_preserves_null() -> None:
    row = list(build_source_row())
    row[CANONICAL_FIELDS.index("pay_time")] = None
    row[CANONICAL_FIELDS.index("model_discounted_price")] = None

    source_record = extraction.source_row_to_record(row)

    assert source_record["pay_time"] is None
    assert source_record["model_discounted_price"] is None


def test_source_row_to_record_rejects_wrong_field_count() -> None:
    with pytest.raises(ValueError, match="expected 84, observed 83"):
        extraction.source_row_to_record(build_source_row()[:-1])


def test_source_row_to_record_rejects_non_decimal_value() -> None:
    row = list(build_source_row())
    row[CANONICAL_FIELDS.index("original_price")] = "100.00"

    with pytest.raises(TypeError, match="original_price"):
        extraction.source_row_to_record(row)
