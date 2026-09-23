from dataclasses import FrozenInstanceError

import pytest

from shopee_quality import schema_contract
from shopee_quality.hashing import CANONICAL_FIELDS


def test_validate_column_order_accepts_v1_contract() -> None:
    schema_contract.validate_column_order(
        observed_columns=CANONICAL_FIELDS,
    )


def test_validate_column_order_rejects_reordered_columns() -> None:
    observed_columns = list(CANONICAL_FIELDS)
    observed_columns[0], observed_columns[1] = (
        observed_columns[1],
        observed_columns[0],
    )

    with pytest.raises(
        schema_contract.SchemaDriftError,
        match="ordinal position",
    ):
        schema_contract.validate_column_order(
            observed_columns=observed_columns,
        )


@pytest.mark.parametrize(
    "observed_columns",
    [
        CANONICAL_FIELDS[:-1],
        CANONICAL_FIELDS + ("unexpected_column",),
    ],
)
def test_validate_column_order_rejects_column_count(
    observed_columns,
) -> None:
    with pytest.raises(
        schema_contract.SchemaDriftError,
        match="column count",
    ):
        schema_contract.validate_column_order(
            observed_columns=observed_columns,
        )


def test_build_source_schema_query_reads_full_metadata() -> None:
    sql, parameters = schema_contract.build_source_schema_query()

    assert "FROM INFORMATION_SCHEMA.COLUMNS" in sql
    assert "ORDINAL_POSITION" in sql
    assert "COLUMN_NAME" in sql
    assert "DATA_TYPE" in sql
    assert "CHARACTER_MAXIMUM_LENGTH" in sql
    assert "NUMERIC_PRECISION" in sql
    assert "NUMERIC_SCALE" in sql
    assert "DATETIME_PRECISION" in sql
    assert "IS_NULLABLE" in sql
    assert "ORDER BY ORDINAL_POSITION" in sql
    assert sql.count("?") == 2
    assert parameters == (
        "vietnam_ecommerce",
        "shopee_orders",
    )


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (
            (1, "pkId", "nvarchar", 50, None, None, None, "NO"),
            {
                "ordinal_position": 1,
                "column_name": "pkId",
                "data_type": "nvarchar",
                "character_maximum_length": 50,
                "numeric_precision": None,
                "numeric_scale": None,
                "datetime_precision": None,
                "is_nullable": False,
            },
        ),
        (
            (
                28,
                "original_price",
                "decimal",
                None,
                18,
                2,
                None,
                "NO",
            ),
            {
                "ordinal_position": 28,
                "column_name": "original_price",
                "data_type": "decimal",
                "character_maximum_length": None,
                "numeric_precision": 18,
                "numeric_scale": 2,
                "datetime_precision": None,
                "is_nullable": False,
            },
        ),
        (
            (
                7,
                "pay_time",
                "datetime2",
                None,
                None,
                None,
                7,
                "YES",
            ),
            {
                "ordinal_position": 7,
                "column_name": "pay_time",
                "data_type": "datetime2",
                "character_maximum_length": None,
                "numeric_precision": None,
                "numeric_scale": None,
                "datetime_precision": 7,
                "is_nullable": True,
            },
        ),
    ],
)
def test_source_column_metadata_from_row(
    row,
    expected,
) -> None:
    metadata = schema_contract.SourceColumnMetadata.from_row(row)

    assert metadata.ordinal_position == expected["ordinal_position"]
    assert metadata.column_name == expected["column_name"]
    assert metadata.data_type == expected["data_type"]
    assert (
        metadata.character_maximum_length
        == expected["character_maximum_length"]
    )
    assert metadata.numeric_precision == expected["numeric_precision"]
    assert metadata.numeric_scale == expected["numeric_scale"]
    assert metadata.datetime_precision == expected["datetime_precision"]
    assert metadata.is_nullable is expected["is_nullable"]


def test_source_column_metadata_is_immutable() -> None:
    metadata = schema_contract.SourceColumnMetadata.from_row(
        (1, "pkId", "nvarchar", 50, None, None, None, "NO")
    )

    with pytest.raises(FrozenInstanceError):
        metadata.column_name = "changed"


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        (
            "nvarchar(50)",
            {
                "data_type": "nvarchar",
                "character_maximum_length": 50,
                "numeric_precision": None,
                "numeric_scale": None,
                "datetime_precision": None,
            },
        ),
        (
            "decimal(18,2)",
            {
                "data_type": "decimal",
                "character_maximum_length": None,
                "numeric_precision": 18,
                "numeric_scale": 2,
                "datetime_precision": None,
            },
        ),
        (
            "datetime2(7)",
            {
                "data_type": "datetime2",
                "character_maximum_length": None,
                "numeric_precision": None,
                "numeric_scale": None,
                "datetime_precision": 7,
            },
        ),
        (
            "int",
            {
                "data_type": "int",
                "character_maximum_length": None,
                "numeric_precision": 10,
                "numeric_scale": 0,
                "datetime_precision": None,
            },
        ),
    ],
)
def test_source_column_metadata_from_contract_spec(
    source_type: str,
    expected: dict,
) -> None:
    metadata = schema_contract.SourceColumnMetadata.from_contract_spec(
        ordinal_position=1,
        column_name="test_column",
        source_type=source_type,
        is_nullable=False,
    )

    assert metadata.data_type == expected["data_type"]
    assert (
        metadata.character_maximum_length
        == expected["character_maximum_length"]
    )
    assert metadata.numeric_precision == expected["numeric_precision"]
    assert metadata.numeric_scale == expected["numeric_scale"]
    assert metadata.datetime_precision == expected["datetime_precision"]


def test_v1_schema_contract_covers_all_canonical_fields() -> None:
    contract = schema_contract.SOURCE_SCHEMA_CONTRACT

    assert len(contract) == 84
    assert tuple(
        column.column_name for column in contract
    ) == CANONICAL_FIELDS
    assert tuple(
        column.ordinal_position for column in contract
    ) == tuple(range(1, 85))


def test_v1_schema_contract_representative_metadata() -> None:
    contract = schema_contract.SOURCE_SCHEMA_CONTRACT

    assert contract[0] == schema_contract.SourceColumnMetadata(
        1,
        "pkId",
        "nvarchar",
        50,
        None,
        None,
        None,
        False,
    )
    assert contract[5].datetime_precision == 7
    assert contract[27].numeric_precision == 18
    assert contract[27].numeric_scale == 2
    assert contract[-1].column_name == "is_primary_row"
    assert contract[-1].is_nullable is False


def metadata_rows_from_contract():
    return [
        (
            column.ordinal_position,
            column.column_name,
            column.data_type,
            column.character_maximum_length,
            column.numeric_precision,
            column.numeric_scale,
            column.datetime_precision,
            "YES" if column.is_nullable else "NO",
        )
        for column in schema_contract.SOURCE_SCHEMA_CONTRACT
    ]


def test_validate_source_schema_accepts_v1_metadata() -> None:
    observed = schema_contract.validate_source_schema(
        metadata_rows_from_contract()
    )

    assert observed == schema_contract.SOURCE_SCHEMA_CONTRACT


def test_validate_source_schema_rejects_type_drift() -> None:
    rows = metadata_rows_from_contract()
    original_price = list(rows[27])
    original_price[2] = "float"
    rows[27] = tuple(original_price)

    with pytest.raises(
        schema_contract.SchemaDriftError,
        match="original_price.*data_type",
    ):
        schema_contract.validate_source_schema(rows)


def test_validate_source_schema_rejects_metadata_column_count() -> None:
    rows = metadata_rows_from_contract()[:-1]

    with pytest.raises(
        schema_contract.SchemaDriftError,
        match="column count",
    ):
        schema_contract.validate_source_schema(rows)
