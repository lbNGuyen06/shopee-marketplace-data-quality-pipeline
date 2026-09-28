from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import UUID

import pytest
from psycopg.types.json import Jsonb

from shopee_quality import ingestion
from shopee_quality.hashing import HASH_CONTRACT_VERSION


def test_start_pipeline_run_inserts_running_record() -> None:
    connection = Mock()
    batch_id = UUID("11111111-1111-1111-1111-111111111111")
    extraction_started_at = datetime(
        2026,
        9,
        16,
        10,
        0,
        tzinfo=timezone.utc,
    )
    overlap_start = datetime(
        2026,
        9,
        16,
        9,
        50,
        tzinfo=timezone.utc,
    )
    extraction_end = datetime(
        2026,
        9,
        16,
        10,
        0,
        tzinfo=timezone.utc,
    )

    ingestion.start_pipeline_run(
        connection=connection,
        batch_id=batch_id,
        source_name="xomdb.vietnam_ecommerce.shopee_orders",
        extraction_started_at=extraction_started_at,
        overlap_start_synced_at=overlap_start,
        extraction_end_synced_at=extraction_end,
    )

    sql, parameters = connection.execute.call_args.args

    assert "INSERT INTO monitoring.pipeline_runs" in sql
    assert "'running'" in sql
    assert parameters == (
        batch_id,
        "xomdb.vietnam_ecommerce.shopee_orders",
        HASH_CONTRACT_VERSION,
        extraction_started_at,
        overlap_start,
        extraction_end,
    )
    connection.commit.assert_not_called()


def test_insert_raw_observation_inserts_json_record() -> None:
    connection = Mock()
    batch_id = UUID("11111111-1111-1111-1111-111111111111")
    synced_at = datetime(
        2026,
        9,
        16,
        10,
        0,
        tzinfo=timezone.utc,
    )
    source_record = {
        "pkId": "PK-TEST-001",
        "order_sn": "ORDER-TEST-001",
    }
    source_row_hash = "a" * 64

    ingestion.insert_raw_observation(
        connection=connection,
        batch_id=batch_id,
        source_row_number=1,
        source_row_hash=source_row_hash,
        synced_at=synced_at,
        source_record=source_record,
    )

    sql, parameters = connection.execute.call_args.args

    assert "INSERT INTO raw.shopee_observations" in sql
    assert parameters[:4] == (
        batch_id,
        1,
        source_row_hash,
        synced_at,
    )
    assert isinstance(parameters[4], Jsonb)
    assert parameters[4].obj == source_record
    connection.commit.assert_not_called()


def test_insert_rejected_record_inserts_json_without_hash() -> None:
    connection = Mock()
    batch_id = UUID("11111111-1111-1111-1111-111111111111")
    source_record = {
        "pkId": "PK-TEST-001",
        "synced_at": "invalid-datetime",
    }

    ingestion.insert_rejected_record(
        connection=connection,
        batch_id=batch_id,
        source_row_number=2,
        source_record=source_record,
        rule_code="source_record_contract_validation",
        reason="Source record failed canonical validation",
    )

    sql, parameters = connection.execute.call_args.args
    assert "INSERT INTO monitoring.rejected_records" in sql
    assert parameters[:5] == (
        batch_id,
        2,
        None,
        "source_record_contract_validation",
        "Source record failed canonical validation",
    )
    assert isinstance(parameters[5], Jsonb)
    assert parameters[5].obj == source_record
    connection.commit.assert_not_called()


@pytest.mark.parametrize(
    ("source_row_number", "source_row_hash", "rule_code", "reason"),
    [
        (0, None, "rule", "reason"),
        (1, "invalid", "rule", "reason"),
        (1, None, "", "reason"),
        (1, None, "rule", ""),
    ],
)
def test_insert_rejected_record_rejects_invalid_metadata(
    source_row_number,
    source_row_hash,
    rule_code,
    reason,
) -> None:
    connection = Mock()

    with pytest.raises((TypeError, ValueError)):
        ingestion.insert_rejected_record(
            connection=connection,
            batch_id=UUID(
                "11111111-1111-1111-1111-111111111111"
            ),
            source_row_number=source_row_number,
            source_record={"pkId": "PK-TEST-001"},
            source_row_hash=source_row_hash,
            rule_code=rule_code,
            reason=reason,
        )

    connection.execute.assert_not_called()


@pytest.mark.parametrize(
    ("source_row_number", "source_row_hash"),
    [
        (0, "a" * 64),
        (-1, "a" * 64),
        (1, "a" * 63),
        (1, "a" * 65),
        (1, "A" * 64),
        (1, "g" * 64),
    ],
)
def test_insert_raw_observation_rejects_invalid_identity(
    source_row_number: int,
    source_row_hash: str,
) -> None:
    connection = Mock()

    with pytest.raises(ValueError):
        ingestion.insert_raw_observation(
            connection=connection,
            batch_id=UUID(
                "11111111-1111-1111-1111-111111111111"
            ),
            source_row_number=source_row_number,
            source_row_hash=source_row_hash,
            synced_at=datetime(
                2026,
                9,
                16,
                10,
                0,
                tzinfo=timezone.utc,
            ),
            source_record={"pkId": "PK-TEST-001"},
        )

    connection.execute.assert_not_called()


def test_complete_pipeline_run_updates_reconciled_counts() -> None:
    connection = Mock()
    connection.execute.return_value.rowcount = 1
    batch_id = UUID("11111111-1111-1111-1111-111111111111")
    extraction_ended_at = datetime(
        2026,
        9,
        16,
        10,
        5,
        tzinfo=timezone.utc,
    )
    committed_watermark = datetime(
        2026,
        9,
        16,
        10,
        0,
        tzinfo=timezone.utc,
    )

    ingestion.complete_pipeline_run(
        connection=connection,
        batch_id=batch_id,
        extraction_ended_at=extraction_ended_at,
        committed_watermark=committed_watermark,
        extracted_count=3,
        loaded_count=2,
        duplicate_count=1,
        rejected_count=0,
    )

    sql, parameters = connection.execute.call_args.args

    assert "UPDATE monitoring.pipeline_runs" in sql
    assert "status = 'succeeded'" in sql
    assert parameters == (
        extraction_ended_at,
        committed_watermark,
        3,
        2,
        1,
        0,
        batch_id,
    )
    connection.commit.assert_not_called()


def test_complete_pipeline_run_rejects_unreconciled_counts() -> None:
    connection = Mock()

    with pytest.raises(
        ValueError,
        match="extracted_count must equal",
    ):
        ingestion.complete_pipeline_run(
            connection=connection,
            batch_id=UUID(
                "11111111-1111-1111-1111-111111111111"
            ),
            extraction_ended_at=datetime.now(timezone.utc),
            committed_watermark=datetime.now(timezone.utc),
            extracted_count=3,
            loaded_count=1,
            duplicate_count=1,
            rejected_count=0,
        )

    connection.execute.assert_not_called()


def test_complete_pipeline_run_rejects_non_running_batch() -> None:
    connection = Mock()
    connection.execute.return_value.rowcount = 0
    batch_id = UUID("11111111-1111-1111-1111-111111111111")

    with pytest.raises(
        LookupError,
        match=str(batch_id),
    ):
        ingestion.complete_pipeline_run(
            connection=connection,
            batch_id=batch_id,
            extraction_ended_at=datetime.now(timezone.utc),
            committed_watermark=datetime.now(timezone.utc),
            extracted_count=0,
            loaded_count=0,
            duplicate_count=0,
            rejected_count=0,
        )

    connection.commit.assert_not_called()


def test_fail_pipeline_run_records_error_without_watermark() -> None:
    connection = Mock()
    connection.execute.return_value.rowcount = 1
    batch_id = UUID("11111111-1111-1111-1111-111111111111")
    extraction_ended_at = datetime.now(timezone.utc)

    ingestion.fail_pipeline_run(
        connection=connection,
        batch_id=batch_id,
        extraction_ended_at=extraction_ended_at,
        error_summary="source connection timed out",
    )

    sql, parameters = connection.execute.call_args.args

    assert "UPDATE monitoring.pipeline_runs" in sql
    assert "status = 'failed'" in sql
    assert "committed_watermark" not in sql
    assert parameters == (
        extraction_ended_at,
        "source connection timed out",
        batch_id,
    )
    connection.commit.assert_not_called()
