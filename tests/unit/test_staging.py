from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from psycopg.types.json import Jsonb

from shopee_quality import staging


def test_upsert_staging_record_preserves_first_seen_on_conflict() -> None:
    connection = MagicMock()
    batch_id = UUID("11111111-1111-1111-1111-111111111111")
    observed_at = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)
    synced_at = datetime(2026, 9, 7, 5, 0, tzinfo=timezone.utc)
    record = {"pkId": "source-1"}

    staging.upsert_staging_record(
        connection=connection,
        batch_id=batch_id,
        source_row_hash="a" * 64,
        observed_at=observed_at,
        synced_at=synced_at,
        standardized_record=record,
    )

    sql, parameters = connection.execute.call_args.args
    assert "INSERT INTO staging.shopee_records" in sql
    assert "ON CONFLICT (source_row_hash) DO UPDATE" in sql
    assert "first_seen_batch_id =" not in sql.split("DO UPDATE", 1)[1]
    assert "first_seen_at =" not in sql.split("DO UPDATE", 1)[1]
    assert "last_seen_batch_id = EXCLUDED.last_seen_batch_id" in sql
    assert "last_seen_at = EXCLUDED.last_seen_at" in sql
    assert parameters[:-1] == (
        "a" * 64,
        batch_id,
        batch_id,
        observed_at,
        observed_at,
        synced_at,
    )
    assert isinstance(parameters[-1], Jsonb)
    connection.commit.assert_not_called()


@pytest.mark.parametrize(
    "source_row_hash",
    [None, "", "A" * 64, "a" * 63, "g" * 64],
)
def test_upsert_staging_record_rejects_invalid_hash(
    source_row_hash,
) -> None:
    with pytest.raises(ValueError, match="64 lowercase"):
        staging.upsert_staging_record(
            connection=MagicMock(),
            batch_id=UUID("11111111-1111-1111-1111-111111111111"),
            source_row_hash=source_row_hash,
            observed_at=datetime.now(timezone.utc),
            synced_at=datetime.now(timezone.utc),
            standardized_record={},
        )


def test_upsert_staging_record_rejects_non_dictionary_record() -> None:
    with pytest.raises(TypeError, match="dictionary"):
        staging.upsert_staging_record(
            connection=MagicMock(),
            batch_id=UUID("11111111-1111-1111-1111-111111111111"),
            source_row_hash="a" * 64,
            observed_at=datetime.now(timezone.utc),
            synced_at=datetime.now(timezone.utc),
            standardized_record=[],
        )
