from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from shopee_quality import incremental


def test_build_extraction_window_applies_overlap() -> None:
    committed_watermark = datetime(
        2026,
        9,
        18,
        10,
        0,
        tzinfo=timezone.utc,
    )
    extraction_end = datetime(
        2026,
        9,
        18,
        10,
        30,
        tzinfo=timezone.utc,
    )

    window = incremental.build_extraction_window(
        committed_watermark=committed_watermark,
        extraction_end=extraction_end,
        overlap_minutes=10,
    )

    assert window.start_synced_at == datetime(
        2026,
        9,
        18,
        9,
        50,
        tzinfo=timezone.utc,
    )
    assert window.end_synced_at == extraction_end


def test_build_extraction_window_uses_initial_start_without_watermark() -> None:
    initial_start = datetime(
        2026,
        1,
        1,
        0,
        0,
        tzinfo=timezone.utc,
    )
    extraction_end = datetime(
        2026,
        9,
        18,
        10,
        30,
        tzinfo=timezone.utc,
    )

    window = incremental.build_extraction_window(
        committed_watermark=None,
        extraction_end=extraction_end,
        overlap_minutes=10,
        initial_start=initial_start,
    )

    assert window.start_synced_at == initial_start
    assert window.end_synced_at == extraction_end


@pytest.mark.parametrize(
    "overlap_minutes",
    [-1, True, 1.5],
)
def test_build_extraction_window_rejects_invalid_overlap(
    overlap_minutes,
) -> None:
    watermark = datetime(
        2026,
        9,
        18,
        10,
        0,
        tzinfo=timezone.utc,
    )

    with pytest.raises(
        ValueError,
        match="nonnegative integer",
    ):
        incremental.build_extraction_window(
            committed_watermark=watermark,
            extraction_end=watermark,
            overlap_minutes=overlap_minutes,
        )


def test_build_extraction_window_requires_initial_start() -> None:
    extraction_end = datetime.now(timezone.utc)

    with pytest.raises(
        ValueError,
        match="initial_start is required",
    ):
        incremental.build_extraction_window(
            committed_watermark=None,
            extraction_end=extraction_end,
            overlap_minutes=10,
        )


def test_build_extraction_window_rejects_reversed_window() -> None:
    extraction_end = datetime(
        2026,
        9,
        18,
        10,
        0,
        tzinfo=timezone.utc,
    )
    initial_start = datetime(
        2026,
        9,
        18,
        11,
        0,
        tzinfo=timezone.utc,
    )

    with pytest.raises(
        ValueError,
        match="start cannot be after",
    ):
        incremental.build_extraction_window(
            committed_watermark=None,
            extraction_end=extraction_end,
            overlap_minutes=10,
            initial_start=initial_start,
        )


def test_get_latest_committed_watermark() -> None:
    connection = Mock()
    expected_watermark = datetime(
        2026,
        9,
        18,
        10,
        0,
        tzinfo=timezone.utc,
    )
    connection.execute.return_value.fetchone.return_value = (
        expected_watermark,
    )

    actual = incremental.get_latest_committed_watermark(
        connection=connection,
        source_name="xomdb.vietnam_ecommerce.shopee_orders",
    )

    sql, parameters = connection.execute.call_args.args

    assert "FROM monitoring.pipeline_runs" in sql
    assert "status = 'succeeded'" in sql
    assert "committed_watermark IS NOT NULL" in sql
    assert "ORDER BY committed_watermark DESC" in sql
    assert parameters == (
        "xomdb.vietnam_ecommerce.shopee_orders",
    )
    assert actual == expected_watermark
    connection.commit.assert_not_called()


def test_get_latest_committed_watermark_returns_none_without_run() -> None:
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = None

    actual = incremental.get_latest_committed_watermark(
        connection=connection,
        source_name="new-source",
    )

    assert actual is None
    connection.commit.assert_not_called()


def test_resolve_extraction_window_from_committed_watermark() -> None:
    connection = Mock()
    committed_watermark = datetime(
        2026,
        9,
        18,
        10,
        0,
        tzinfo=timezone.utc,
    )
    extraction_end = datetime(
        2026,
        9,
        18,
        10,
        30,
        tzinfo=timezone.utc,
    )
    connection.execute.return_value.fetchone.return_value = (
        committed_watermark,
    )

    window = incremental.resolve_extraction_window(
        connection=connection,
        source_name="xomdb.vietnam_ecommerce.shopee_orders",
        extraction_end=extraction_end,
        overlap_minutes=10,
        initial_start=datetime(
            2026,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    )

    assert window.start_synced_at == datetime(
        2026,
        9,
        18,
        9,
        50,
        tzinfo=timezone.utc,
    )
    assert window.end_synced_at == extraction_end


def test_resolve_extraction_window_for_initial_run() -> None:
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = None
    initial_start = datetime(
        2026,
        1,
        1,
        tzinfo=timezone.utc,
    )
    extraction_end = datetime(
        2026,
        9,
        18,
        10,
        30,
        tzinfo=timezone.utc,
    )

    window = incremental.resolve_extraction_window(
        connection=connection,
        source_name="xomdb.vietnam_ecommerce.shopee_orders",
        extraction_end=extraction_end,
        overlap_minutes=10,
        initial_start=initial_start,
    )

    assert window.start_synced_at == initial_start
    assert window.end_synced_at == extraction_end
