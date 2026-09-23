import re
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

from shopee_quality.extraction import (
    SOURCE_SCHEMA,
    SOURCE_TABLE,
)
from shopee_quality.hashing import CANONICAL_FIELDS


SOURCE_SCHEMA_QUERY = """
SELECT
    ORDINAL_POSITION,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    DATETIME_PRECISION,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ?
  AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"""

NVARCHAR_PATTERN = re.compile(r"^nvarchar\((\d+)\)$")
DECIMAL_PATTERN = re.compile(r"^decimal\((\d+),(\d+)\)$")
DATETIME2_PATTERN = re.compile(r"^datetime2\((\d+)\)$")


@dataclass(frozen=True)
class SourceColumnMetadata:
    ordinal_position: int
    column_name: str
    data_type: str
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    datetime_precision: Optional[int]
    is_nullable: bool

    @classmethod
    def from_row(
        cls,
        row: Sequence[object],
    ) -> "SourceColumnMetadata":
        if len(row) != 8:
            raise ValueError(
                "Source metadata row must contain exactly 8 values"
            )

        (
            ordinal_position,
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            nullable_text,
        ) = row

        if nullable_text not in {"YES", "NO"}:
            raise ValueError(
                "IS_NULLABLE must be either 'YES' or 'NO'"
            )

        return cls(
            ordinal_position=ordinal_position,
            column_name=column_name,
            data_type=data_type,
            character_maximum_length=character_maximum_length,
            numeric_precision=numeric_precision,
            numeric_scale=numeric_scale,
            datetime_precision=datetime_precision,
            is_nullable=(nullable_text == "YES"),
        )

    @classmethod
    def from_contract_spec(
        cls,
        ordinal_position: int,
        column_name: str,
        source_type: str,
        is_nullable: bool,
    ) -> "SourceColumnMetadata":
        nvarchar_match = NVARCHAR_PATTERN.fullmatch(source_type)
        decimal_match = DECIMAL_PATTERN.fullmatch(source_type)
        datetime2_match = DATETIME2_PATTERN.fullmatch(source_type)

        if nvarchar_match:
            return cls(
                ordinal_position=ordinal_position,
                column_name=column_name,
                data_type="nvarchar",
                character_maximum_length=int(nvarchar_match.group(1)),
                numeric_precision=None,
                numeric_scale=None,
                datetime_precision=None,
                is_nullable=is_nullable,
            )

        if decimal_match:
            return cls(
                ordinal_position=ordinal_position,
                column_name=column_name,
                data_type="decimal",
                character_maximum_length=None,
                numeric_precision=int(decimal_match.group(1)),
                numeric_scale=int(decimal_match.group(2)),
                datetime_precision=None,
                is_nullable=is_nullable,
            )

        if datetime2_match:
            return cls(
                ordinal_position=ordinal_position,
                column_name=column_name,
                data_type="datetime2",
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                datetime_precision=int(datetime2_match.group(1)),
                is_nullable=is_nullable,
            )

        if source_type == "int":
            return cls(
                ordinal_position=ordinal_position,
                column_name=column_name,
                data_type="int",
                character_maximum_length=None,
                numeric_precision=10,
                numeric_scale=0,
                datetime_precision=None,
                is_nullable=is_nullable,
            )

        raise ValueError(
            f"Unsupported SQL Server source type: {source_type!r}"
        )


SOURCE_COLUMN_SPECS = (
    ("pkId", "nvarchar(50)", False),
    ("user_id", "nvarchar(30)", False),
    ("shop_id", "nvarchar(10)", False),
    ("order_sn", "nvarchar(20)", False),
    ("order_status", "nvarchar(20)", False),
    ("create_time", "datetime2(7)", False),
    ("pay_time", "datetime2(7)", True),
    ("shipped_time", "datetime2(7)", True),
    ("completed_time", "datetime2(7)", True),
    ("cancel_reason", "nvarchar(100)", True),
    ("buyer_remark", "nvarchar(50)", True),
    ("shipping_carrier", "nvarchar(100)", True),
    ("payment_method", "nvarchar(30)", False),
    ("estimated_shipping_date", "nvarchar(50)", True),
    ("buyer_username", "nvarchar(30)", True),
    ("recipient_name", "nvarchar(10)", False),
    ("phone", "nvarchar(10)", False),
    ("full_address", "nvarchar(10)", False),
    ("state", "nvarchar(10)", False),
    ("city", "nvarchar(10)", False),
    ("district", "nvarchar(10)", False),
    ("country", "nvarchar(10)", False),
    ("item_name", "nvarchar(150)", False),
    ("item_sku", "nvarchar(20)", False),
    ("model_name", "nvarchar(30)", True),
    ("model_sku", "nvarchar(50)", False),
    ("quantity", "int", False),
    ("original_price", "decimal(18,2)", False),
    ("model_discounted_price", "decimal(18,2)", True),
    ("item_weight", "decimal(18,3)", False),
    ("total_order_value", "decimal(18,2)", True),
    ("buyer_total_amount", "decimal(18,2)", False),
    ("seller_discount", "decimal(18,2)", False),
    ("shopee_discount", "decimal(18,2)", False),
    ("voucher_from_seller", "decimal(18,2)", False),
    ("voucher_from_shopee", "decimal(18,2)", False),
    ("commission_fee", "decimal(18,2)", False),
    ("service_fee", "decimal(18,2)", False),
    ("transaction_fee", "decimal(18,2)", False),
    ("escrow_amount", "decimal(18,2)", False),
    ("actual_shipping_fee", "decimal(18,2)", False),
    ("shop_name", "nvarchar(30)", False),
    ("connection_id", "nvarchar(30)", False),
    ("region", "nvarchar(10)", True),
    ("synced_at", "datetime2(7)", False),
    ("raw_payload", "nvarchar(50)", True),
    ("currency", "nvarchar(10)", False),
    ("order_status_raw", "nvarchar(20)", False),
    ("cancel_time", "datetime2(7)", True),
    ("fulfillment_flag", "nvarchar(30)", False),
    ("cod", "nvarchar(5)", True),
    ("buyer_user_id", "nvarchar(20)", False),
    ("ship_by_date", "datetime2(7)", True),
    ("note", "nvarchar(50)", True),
    ("shipping_method", "nvarchar(50)", True),
    ("package_number", "nvarchar(20)", True),
    ("recipient_town", "nvarchar(10)", True),
    ("returned_quantity", "int", True),
    ("total_weight", "decimal(18,3)", True),
    ("estimated_shipping_fee", "decimal(18,2)", True),
    ("return_shipping_fee", "decimal(18,2)", True),
    ("item_id", "nvarchar(20)", True),
    ("model_id", "nvarchar(20)", True),
    ("zipcode", "nvarchar(10)", True),
    ("update_time", "datetime2(7)", True),
    ("buyer_cancel_reason", "nvarchar(100)", True),
    ("cancel_by", "nvarchar(10)", True),
    ("product_location_id", "nvarchar(10)", True),
    ("promotion_type", "nvarchar(30)", True),
    ("promotion_id", "nvarchar(20)", True),
    ("active_qty", "int", True),
    ("cancel_requested_qty", "int", True),
    ("cancelled_qty", "int", True),
    ("return_requested_qty", "int", True),
    ("wholesale", "nvarchar(5)", True),
    ("add_on_deal", "nvarchar(5)", True),
    ("main_item", "nvarchar(5)", True),
    ("add_on_deal_id", "nvarchar(20)", True),
    ("can_full_cancel_order", "nvarchar(5)", True),
    ("can_partial_cancel_order", "nvarchar(50)", True),
    (
        "buyer_preference_for_partial_cancellation",
        "int",
        True,
    ),
    ("booking_sn", "nvarchar(50)", True),
    ("advance_package", "nvarchar(50)", True),
    ("is_primary_row", "nvarchar(5)", False),
)

SOURCE_SCHEMA_CONTRACT = tuple(
    SourceColumnMetadata.from_contract_spec(
        ordinal_position=ordinal_position,
        column_name=column_name,
        source_type=source_type,
        is_nullable=is_nullable,
    )
    for ordinal_position, (
        column_name,
        source_type,
        is_nullable,
    ) in enumerate(SOURCE_COLUMN_SPECS, start=1)
)

SCHEMA_METADATA_FIELDS = (
    "ordinal_position",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "datetime_precision",
    "is_nullable",
)


class SchemaDriftError(ValueError):
    pass


def build_source_schema_query() -> Tuple[str, tuple]:
    return (
        SOURCE_SCHEMA_QUERY,
        (
            SOURCE_SCHEMA,
            SOURCE_TABLE,
        ),
    )


def validate_column_order(
    observed_columns: Iterable[str],
) -> None:
    observed = tuple(observed_columns)

    if len(observed) != len(CANONICAL_FIELDS):
        raise SchemaDriftError(
            "Source column count does not match contract: "
            f"expected {len(CANONICAL_FIELDS)}, "
            f"observed {len(observed)}"
        )

    for ordinal_position, (expected, actual) in enumerate(
        zip(CANONICAL_FIELDS, observed),
        start=1,
    ):
        if actual != expected:
            raise SchemaDriftError(
                "Source column mismatch at ordinal position "
                f"{ordinal_position}: expected {expected!r}, "
                f"observed {actual!r}"
            )


def validate_source_schema(
    observed_rows: Iterable[Sequence[object]],
) -> Tuple[SourceColumnMetadata, ...]:
    observed = tuple(
        SourceColumnMetadata.from_row(row)
        for row in observed_rows
    )

    if len(observed) != len(SOURCE_SCHEMA_CONTRACT):
        raise SchemaDriftError(
            "Source column count does not match contract: "
            f"expected {len(SOURCE_SCHEMA_CONTRACT)}, "
            f"observed {len(observed)}"
        )

    for expected, actual in zip(SOURCE_SCHEMA_CONTRACT, observed):
        for field_name in SCHEMA_METADATA_FIELDS:
            expected_value = getattr(expected, field_name)
            actual_value = getattr(actual, field_name)

            if actual_value != expected_value:
                raise SchemaDriftError(
                    "Source schema drift at ordinal position "
                    f"{expected.ordinal_position} for column "
                    f"{expected.column_name!r}: {field_name} "
                    f"expected {expected_value!r}, "
                    f"observed {actual_value!r}"
                )

    return observed
