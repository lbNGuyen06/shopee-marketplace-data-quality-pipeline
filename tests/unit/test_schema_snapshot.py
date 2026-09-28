from unittest.mock import MagicMock
from uuid import UUID

import pytest
from psycopg.types.json import Jsonb

from shopee_quality import schema_snapshot
from shopee_quality.schema_contract import (
    SOURCE_SCHEMA_CONTRACT,
    SourceColumnMetadata,
)


def test_schema_hash_is_stable_and_has_sha256_length() -> None:
    first_hash = schema_snapshot.compute_schema_hash(
        SOURCE_SCHEMA_CONTRACT
    )
    second_hash = schema_snapshot.compute_schema_hash(
        tuple(SOURCE_SCHEMA_CONTRACT)
    )

    assert first_hash == second_hash
    assert len(first_hash) == 64


def test_schema_hash_changes_with_metadata() -> None:
    changed = list(SOURCE_SCHEMA_CONTRACT)
    original = changed[0]
    changed[0] = SourceColumnMetadata(
        ordinal_position=original.ordinal_position,
        column_name=original.column_name,
        data_type=original.data_type,
        character_maximum_length=51,
        numeric_precision=original.numeric_precision,
        numeric_scale=original.numeric_scale,
        datetime_precision=original.datetime_precision,
        is_nullable=original.is_nullable,
    )

    assert schema_snapshot.compute_schema_hash(changed) != (
        schema_snapshot.compute_schema_hash(SOURCE_SCHEMA_CONTRACT)
    )


def test_record_schema_snapshot_inserts_contract_metadata() -> None:
    connection = MagicMock()
    batch_id = UUID("11111111-1111-1111-1111-111111111111")

    actual_hash = schema_snapshot.record_schema_snapshot(
        connection=connection,
        batch_id=batch_id,
        columns=SOURCE_SCHEMA_CONTRACT,
    )

    sql, parameters = connection.execute.call_args.args
    assert "INSERT INTO monitoring.schema_snapshots" in sql
    assert parameters[:5] == (
        batch_id,
        "vietnam_ecommerce",
        "shopee_orders",
        actual_hash,
        84,
    )
    assert isinstance(parameters[5], Jsonb)
    assert len(parameters[5].obj) == 84
    assert set(parameters[5].obj[0]) == {
        "ordinal_position",
        "column_name",
        "data_type",
        "character_maximum_length",
        "numeric_precision",
        "numeric_scale",
        "datetime_precision",
        "is_nullable",
    }
    connection.commit.assert_not_called()


@pytest.mark.parametrize("columns", [(), ("not-metadata",)])
def test_build_schema_definition_rejects_invalid_columns(columns) -> None:
    expected_error = ValueError if not columns else TypeError

    with pytest.raises(expected_error):
        schema_snapshot.build_schema_definition(columns)
