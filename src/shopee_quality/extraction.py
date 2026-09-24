from decimal import Decimal
from typing import Sequence, Tuple

from shopee_quality.hashing import (
    CANONICAL_FIELDS,
    DATETIME_FIELDS,
    DECIMAL_FIELDS,
)
from shopee_quality.incremental import ExtractionWindow


SOURCE_SCHEMA = "vietnam_ecommerce"
SOURCE_TABLE = "shopee_orders"

def source_column_expression(field_name: str) -> str:
    if field_name in DATETIME_FIELDS:
        return (
            f"CONVERT(varchar(19), [{field_name}], 126) + '.' + "
            f"RIGHT('0000000' + CONVERT(varchar(7), "
            f"DATEPART(NANOSECOND, [{field_name}]) / 100), 7) "
            f"AS [{field_name}]"
        )

    return f"[{field_name}]"


SOURCE_COLUMNS_SQL = ",\n    ".join(
    source_column_expression(field_name)
    for field_name in CANONICAL_FIELDS
)

SOURCE_QUERY = f"""
SELECT
    {SOURCE_COLUMNS_SQL}
FROM [{SOURCE_SCHEMA}].[{SOURCE_TABLE}]
WHERE [synced_at] >= ?
  AND [synced_at] <= ?
ORDER BY [synced_at],
    [pkId],
    [order_sn],
    [item_id],
    [model_id]
"""


def build_source_query(
    window: ExtractionWindow,
) -> Tuple[str, tuple]:
    if not isinstance(window, ExtractionWindow):
        raise TypeError(
            "window must be an ExtractionWindow"
        )

    return (
        SOURCE_QUERY,
        (
            window.start_synced_at,
            window.end_synced_at,
        ),
    )


def source_row_to_record(row: Sequence[object]) -> dict:
    if len(row) != len(CANONICAL_FIELDS):
        raise ValueError(
            "Source row field count does not match contract: "
            f"expected {len(CANONICAL_FIELDS)}, "
            f"observed {len(row)}"
        )

    source_record = {}

    for field_name, value in zip(CANONICAL_FIELDS, row):
        if value is not None and field_name in DECIMAL_FIELDS:
            if not isinstance(value, Decimal):
                raise TypeError(
                    f"{field_name} must be returned as Decimal"
                )

            value = format(value, "f")

        source_record[field_name] = value

    return source_record
