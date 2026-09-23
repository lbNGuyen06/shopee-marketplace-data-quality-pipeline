from datetime import datetime, timezone

from shopee_quality import extraction
from shopee_quality.incremental import ExtractionWindow


def test_build_source_query_uses_parameterized_synced_at_window() -> None:
    window = ExtractionWindow(
        start_synced_at=datetime(
            2026,
            9,
            18,
            9,
            50,
            tzinfo=timezone.utc,
        ),
        end_synced_at=datetime(
            2026,
            9,
            18,
            10,
            30,
            tzinfo=timezone.utc,
        ),
    )

    sql, parameters = extraction.build_source_query(window)

    assert "FROM [vietnam_ecommerce].[shopee_orders]" in sql
    assert "WHERE [synced_at] >= ?" in sql
    assert "AND [synced_at] <= ?" in sql
    assert "ORDER BY [synced_at]" in sql
    assert sql.count("?") == 2
    assert parameters == (
        window.start_synced_at,
        window.end_synced_at,
    )


def test_build_source_query_selects_hash_contract_fields() -> None:
    window = ExtractionWindow(
        start_synced_at=datetime.now(timezone.utc),
        end_synced_at=datetime.now(timezone.utc),
    )

    sql, _ = extraction.build_source_query(window)

    assert "[pkId]" in sql
    assert "[synced_at]" in sql
    assert "[is_primary_row]" in sql
    assert "SELECT *" not in sql.upper()
