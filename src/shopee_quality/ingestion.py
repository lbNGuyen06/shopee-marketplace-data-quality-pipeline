import re
from datetime import datetime
from typing import Optional
from uuid import UUID

from psycopg.types.json import Jsonb

from shopee_quality.hashing import HASH_CONTRACT_VERSION

SOURCE_ROW_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")

START_PIPELINE_RUN_SQL = """
INSERT INTO monitoring.pipeline_runs (
    batch_id,
    source_name,
    hash_contract_version,
    status,
    extraction_started_at,
    overlap_start_synced_at,
    extraction_end_synced_at
)
VALUES (%s, %s, %s, 'running', %s, %s, %s)
"""

INSERT_RAW_OBSERVATION_SQL = """
INSERT INTO raw.shopee_observations (
    batch_id,
    source_row_number,
    source_row_hash,
    synced_at,
    source_record
)
VALUES (%s, %s, %s, %s, %s)
"""

INSERT_REJECTED_RECORD_SQL = """
INSERT INTO monitoring.rejected_records (
    batch_id,
    source_row_number,
    source_row_hash,
    rule_code,
    reason,
    source_record
)
VALUES (%s, %s, %s, %s, %s, %s)
"""

COMPLETE_PIPELINE_RUN_SQL = """
UPDATE monitoring.pipeline_runs
SET
    status = 'succeeded',
    extraction_ended_at = %s,
    committed_watermark = %s,
    extracted_count = %s,
    loaded_count = %s,
    duplicate_count = %s,
    rejected_count = %s
WHERE batch_id = %s
  AND status = 'running'
"""

FAIL_PIPELINE_RUN_SQL = """
UPDATE monitoring.pipeline_runs
SET
    status = 'failed',
    extraction_ended_at = %s,
    error_summary = %s
WHERE batch_id = %s
  AND status = 'running'
"""


def start_pipeline_run(
    connection,
    batch_id: UUID,
    source_name: str,
    extraction_started_at: datetime,
    overlap_start_synced_at: datetime,
    extraction_end_synced_at: datetime,
) -> None:
    connection.execute(
        START_PIPELINE_RUN_SQL,
        (
            batch_id,
            source_name,
            HASH_CONTRACT_VERSION,
            extraction_started_at,
            overlap_start_synced_at,
            extraction_end_synced_at,
        ),
    )


def insert_raw_observation(
    connection,
    batch_id: UUID,
    source_row_number: int,
    source_row_hash: str,
    synced_at: datetime,
    source_record: dict,
) -> None:
    if (
        isinstance(source_row_number, bool)
        or not isinstance(source_row_number, int)
        or source_row_number <= 0
    ):
        raise ValueError(
            "source_row_number must be a positive integer"
        )
    if (
        not isinstance(source_row_hash, str)
        or not SOURCE_ROW_HASH_PATTERN.fullmatch(source_row_hash)
    ):
        raise ValueError(
            "source_row_hash must contain 64 lowercase hexadecimal characters"
        )
    connection.execute(
        INSERT_RAW_OBSERVATION_SQL,
        (
            batch_id,
            source_row_number,
            source_row_hash,
            synced_at,
            Jsonb(source_record),
        ),
    )


def insert_rejected_record(
    connection,
    batch_id: UUID,
    source_row_number: int,
    source_record: dict,
    rule_code: str,
    reason: str,
    source_row_hash: Optional[str] = None,
) -> None:
    if (
        isinstance(source_row_number, bool)
        or not isinstance(source_row_number, int)
        or source_row_number <= 0
    ):
        raise ValueError(
            "source_row_number must be a positive integer"
        )
    if source_row_hash is not None and (
        not isinstance(source_row_hash, str)
        or not SOURCE_ROW_HASH_PATTERN.fullmatch(source_row_hash)
    ):
        raise ValueError(
            "source_row_hash must be null or contain 64 lowercase "
            "hexadecimal characters"
        )
    if not isinstance(source_record, dict):
        raise TypeError("source_record must be a dictionary")
    if not isinstance(rule_code, str) or not rule_code.strip():
        raise ValueError("rule_code must be a non-empty string")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason must be a non-empty string")

    connection.execute(
        INSERT_REJECTED_RECORD_SQL,
        (
            batch_id,
            source_row_number,
            source_row_hash,
            rule_code,
            reason,
            Jsonb(source_record),
        ),
    )


def complete_pipeline_run(
    connection,
    batch_id: UUID,
    extraction_ended_at: datetime,
    committed_watermark: datetime,
    extracted_count: int,
    loaded_count: int,
    duplicate_count: int,
    rejected_count: int,
) -> None:
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
            "pipeline reconciliation counts must be nonnegative integers"
        )

    if extracted_count != (
        loaded_count + duplicate_count + rejected_count
    ):
        raise ValueError(
            "extracted_count must equal loaded_count, "
            "duplicate_count, and rejected_count"
        )

    result = connection.execute(
        COMPLETE_PIPELINE_RUN_SQL,
        (
            extraction_ended_at,
            committed_watermark,
            extracted_count,
            loaded_count,
            duplicate_count,
            rejected_count,
            batch_id,
        ),
    )

    if result.rowcount != 1:
        raise LookupError(
            f"Running pipeline batch was not found: {batch_id}"
        )


def fail_pipeline_run(
    connection,
    batch_id: UUID,
    extraction_ended_at: datetime,
    error_summary: str,
) -> None:
    if not isinstance(error_summary, str) or not error_summary.strip():
        raise ValueError(
            "error_summary must be a non-empty string"
        )

    result = connection.execute(
        FAIL_PIPELINE_RUN_SQL,
        (
            extraction_ended_at,
            error_summary,
            batch_id,
        ),
    )

    if result.rowcount != 1:
        raise LookupError(
            f"Running pipeline batch was not found: {batch_id}"
        )
