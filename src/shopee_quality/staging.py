import re
from datetime import datetime
from uuid import UUID

from psycopg.types.json import Jsonb


SOURCE_ROW_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")

UPSERT_STAGING_RECORD_SQL = """
INSERT INTO staging.shopee_records (
    source_row_hash,
    first_seen_batch_id,
    last_seen_batch_id,
    first_seen_at,
    last_seen_at,
    synced_at,
    standardized_record
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source_row_hash) DO UPDATE
SET
    last_seen_batch_id = EXCLUDED.last_seen_batch_id,
    last_seen_at = EXCLUDED.last_seen_at,
    synced_at = EXCLUDED.synced_at,
    standardized_record = EXCLUDED.standardized_record
"""


def upsert_staging_record(
    connection,
    batch_id: UUID,
    source_row_hash: str,
    observed_at: datetime,
    synced_at: datetime,
    standardized_record: dict,
) -> None:
    if (
        not isinstance(source_row_hash, str)
        or not SOURCE_ROW_HASH_PATTERN.fullmatch(source_row_hash)
    ):
        raise ValueError(
            "source_row_hash must contain 64 lowercase hexadecimal characters"
        )
    if not isinstance(standardized_record, dict):
        raise TypeError("standardized_record must be a dictionary")

    connection.execute(
        UPSERT_STAGING_RECORD_SQL,
        (
            source_row_hash,
            batch_id,
            batch_id,
            observed_at,
            observed_at,
            synced_at,
            Jsonb(standardized_record),
        ),
    )
