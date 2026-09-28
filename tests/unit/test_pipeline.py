from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from shopee_quality.hashing import (
    CANONICAL_FIELDS,
    DATETIME_FIELDS,
    DECIMAL_FIELDS,
    INTEGER_FIELDS,
)
from shopee_quality.incremental import ExtractionWindow
from shopee_quality.pipeline import (
    QualityGateError,
    ingest_extraction_window,
    parse_source_synced_at,
)
from shopee_quality.schema_contract import SOURCE_SCHEMA_CONTRACT


BATCH_ID = UUID("11111111-1111-1111-1111-111111111111")
STARTED_AT = datetime(2026, 9, 23, 10, 0, tzinfo=timezone.utc)
ENDED_AT = datetime(2026, 9, 23, 10, 1, tzinfo=timezone.utc)
WINDOW = ExtractionWindow(
    start_synced_at=datetime(
        2026, 9, 23, 9, 50, tzinfo=timezone.utc
    ),
    end_synced_at=datetime(
        2026, 9, 23, 10, 0, tzinfo=timezone.utc
    ),
)


def build_source_row(pk_id: str = "source-1") -> tuple:
    values = []

    for field_name in CANONICAL_FIELDS:
        if field_name == "pkId":
            values.append(pk_id)
        elif field_name in DATETIME_FIELDS:
            values.append("2026-09-23T09:55:00.1234567")
        elif field_name in DECIMAL_FIELDS:
            values.append(Decimal("100.00"))
        elif field_name in INTEGER_FIELDS:
            values.append(1)
        else:
            values.append(f"value-{field_name}")

    return tuple(values)


def build_connections(rows):
    source_connection = MagicMock()
    source_connection.execute.return_value.fetchall.return_value = rows
    destination_connection = MagicMock()
    destination_connection.execute.return_value.rowcount = 1
    destination_connection.execute.return_value.fetchone.return_value = (0,)
    return source_connection, destination_connection


def clock():
    clock.call_count += 1
    return STARTED_AT if clock.call_count == 1 else ENDED_AT


def test_ingest_extraction_window_commits_raw_and_watermark() -> None:
    clock.call_count = 0
    source_connection, destination_connection = build_connections(
        [build_source_row(), build_source_row("source-2")]
    )

    result = ingest_extraction_window(
        source_connection=source_connection,
        destination_connection=destination_connection,
        source_name="xomdb.shopee_orders",
        window=WINDOW,
        source_schema_metadata=SOURCE_SCHEMA_CONTRACT,
        batch_id=BATCH_ID,
        clock=clock,
    )

    assert result.batch_id == BATCH_ID
    assert result.extracted_count == 2
    assert result.loaded_count == 2
    assert result.rejected_count == 0
    assert result.committed_watermark == WINDOW.end_synced_at
    assert destination_connection.commit.call_count == 2
    destination_connection.rollback.assert_not_called()

    raw_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "INSERT INTO raw.shopee_observations" in call.args[0]
    ]
    assert [call.args[1][1] for call in raw_calls] == [1, 2]
    assert all(len(call.args[1][2]) == 64 for call in raw_calls)


def test_ingest_extraction_window_isolates_invalid_source_record() -> None:
    clock.call_count = 0
    invalid_row = list(build_source_row("invalid-source"))
    invalid_value = "private-invalid-synced-at"
    invalid_row[CANONICAL_FIELDS.index("synced_at")] = invalid_value
    source_connection, destination_connection = build_connections(
        [build_source_row(), tuple(invalid_row)]
    )

    result = ingest_extraction_window(
        source_connection=source_connection,
        destination_connection=destination_connection,
        source_name="xomdb.shopee_orders",
        window=WINDOW,
        source_schema_metadata=SOURCE_SCHEMA_CONTRACT,
        batch_id=BATCH_ID,
        clock=clock,
    )

    assert result.extracted_count == 2
    assert result.loaded_count == 1
    assert result.rejected_count == 1
    destination_connection.rollback.assert_not_called()

    raw_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "INSERT INTO raw.shopee_observations" in call.args[0]
    ]
    rejected_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "INSERT INTO monitoring.rejected_records" in call.args[0]
    ]
    complete_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "status = 'succeeded'" in call.args[0]
    ]

    assert len(raw_calls) == 1
    assert len(rejected_calls) == 1
    rejected_parameters = rejected_calls[0].args[1]
    assert rejected_parameters[1:5] == (
        2,
        None,
        "source_record_contract_validation",
        "Source record failed canonical validation",
    )
    assert invalid_value not in rejected_parameters[4]
    assert complete_calls[0].args[1][2:6] == (2, 1, 0, 1)


def test_ingest_extraction_window_rolls_back_before_marking_failed() -> None:
    clock.call_count = 0
    invalid_row = list(build_source_row())
    invalid_row[CANONICAL_FIELDS.index("original_price")] = "100.00"
    source_connection, destination_connection = build_connections(
        [invalid_row]
    )

    with pytest.raises(TypeError, match="original_price"):
        ingest_extraction_window(
            source_connection=source_connection,
            destination_connection=destination_connection,
            source_name="xomdb.shopee_orders",
            window=WINDOW,
            source_schema_metadata=SOURCE_SCHEMA_CONTRACT,
            batch_id=BATCH_ID,
            clock=clock,
        )

    destination_connection.rollback.assert_called_once_with()
    assert destination_connection.commit.call_count == 2

    complete_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "status = 'succeeded'" in call.args[0]
    ]
    failed_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "status = 'failed'" in call.args[0]
    ]
    assert complete_calls == []
    assert len(failed_calls) == 1


def test_window_quality_gate_rolls_back_and_persists_failure() -> None:
    clock.call_count = 0
    source_connection, destination_connection = build_connections(
        [build_source_row()]
    )

    def execute(sql, parameters=None):
        result = MagicMock()
        result.rowcount = 1
        if "count(*) - count(DISTINCT source_row_hash)" in sql:
            result.fetchone.return_value = (0,)
        elif "WITH touched_pkids" in sql:
            result.fetchone.return_value = (0,)
        elif "observation.synced_at <" in sql:
            result.fetchone.return_value = (1,)
        return result

    destination_connection.execute.side_effect = execute

    with pytest.raises(QualityGateError, match="outside"):
        ingest_extraction_window(
            source_connection=source_connection,
            destination_connection=destination_connection,
            source_name="xomdb.shopee_orders",
            window=WINDOW,
            source_schema_metadata=SOURCE_SCHEMA_CONTRACT,
            batch_id=BATCH_ID,
            clock=clock,
        )

    destination_connection.rollback.assert_called_once_with()
    assert destination_connection.commit.call_count == 2
    quality_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "INSERT INTO monitoring.quality_test_results" in call.args[0]
        and call.args[1][1]
        == "batch_synced_at_window_violation_count"
    ]
    assert len(quality_calls) == 2
    assert quality_calls[-1].args[1][3] == "failed"

    complete_calls = [
        call
        for call in destination_connection.execute.call_args_list
        if "status = 'succeeded'" in call.args[0]
    ]
    assert complete_calls == []


def test_parse_source_synced_at_assumes_source_is_utc() -> None:
    actual = parse_source_synced_at(
        "2026-09-23T09:55:00.1234567"
    )

    assert actual == datetime(
        2026,
        9,
        23,
        9,
        55,
        0,
        123456,
        tzinfo=timezone.utc,
    )


def test_parse_source_synced_at_rejects_timezone_suffix() -> None:
    with pytest.raises(ValueError, match="must not contain a timezone"):
        parse_source_synced_at("2026-09-23T09:55:00.123456+00:00")
