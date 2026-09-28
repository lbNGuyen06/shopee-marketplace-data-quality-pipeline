from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from shopee_quality import reporting


def health_row():
    return (
        date(2026, 9, 28),
        "xomdb.vietnam_ecommerce.shopee_orders",
        3,
        2,
        1,
        100,
        98,
        2,
        4,
        3,
        1,
        datetime(2026, 9, 28, 9, 0, tzinfo=timezone.utc),
        "a" * 64,
    )


def test_fetch_daily_source_health_uses_parameterized_query() -> None:
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = [health_row()]

    rows = reporting.fetch_daily_source_health(
        connection=connection,
        days=7,
        source_name="xomdb.vietnam_ecommerce.shopee_orders",
    )

    sql, parameters = connection.execute.call_args.args
    assert "FROM marts.daily_source_health" in sql
    assert parameters == (
        7,
        "xomdb.vietnam_ecommerce.shopee_orders",
    )
    assert rows == [reporting.DailySourceHealth(*health_row())]


@pytest.mark.parametrize(
    ("days", "source_name", "message"),
    [
        (0, "source", "days"),
        (1, " ", "source_name"),
    ],
)
def test_fetch_daily_source_health_rejects_invalid_filters(
    days,
    source_name,
    message,
) -> None:
    connection = MagicMock()

    with pytest.raises(ValueError, match=message):
        reporting.fetch_daily_source_health(
            connection=connection,
            days=days,
            source_name=source_name,
        )

    connection.execute.assert_not_called()
