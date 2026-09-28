import hashlib
import json
from dataclasses import asdict
from typing import Sequence
from uuid import UUID

from psycopg.types.json import Jsonb

from shopee_quality.extraction import SOURCE_SCHEMA, SOURCE_TABLE
from shopee_quality.schema_contract import SourceColumnMetadata


INSERT_SCHEMA_SNAPSHOT_SQL = """
INSERT INTO monitoring.schema_snapshots (
    batch_id,
    source_schema,
    source_table,
    schema_hash,
    column_count,
    schema_definition
)
VALUES (%s, %s, %s, %s, %s, %s)
"""


def build_schema_definition(
    columns: Sequence[SourceColumnMetadata],
) -> list:
    if not columns:
        raise ValueError("columns must not be empty")
    if any(
        not isinstance(column, SourceColumnMetadata)
        for column in columns
    ):
        raise TypeError(
            "columns must contain SourceColumnMetadata values"
        )

    return [asdict(column) for column in columns]


def compute_schema_hash(
    columns: Sequence[SourceColumnMetadata],
) -> str:
    definition = build_schema_definition(columns)
    canonical_json = json.dumps(
        definition,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def record_schema_snapshot(
    connection,
    batch_id: UUID,
    columns: Sequence[SourceColumnMetadata],
) -> str:
    definition = build_schema_definition(columns)
    schema_hash = compute_schema_hash(columns)

    connection.execute(
        INSERT_SCHEMA_SNAPSHOT_SQL,
        (
            batch_id,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            schema_hash,
            len(definition),
            Jsonb(definition),
        ),
    )
    return schema_hash
