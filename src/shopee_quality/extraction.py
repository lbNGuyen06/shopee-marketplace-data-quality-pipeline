from typing import Tuple

from shopee_quality.hashing import CANONICAL_FIELDS
from shopee_quality.incremental import ExtractionWindow


SOURCE_SCHEMA = "vietnam_ecommerce"
SOURCE_TABLE = "shopee_orders"

SOURCE_COLUMNS_SQL = ",\n    ".join(
    f"[{field_name}]"
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
