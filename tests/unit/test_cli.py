import argparse
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from shopee_quality import cli
from shopee_quality.pipeline import BatchResult
from shopee_quality.reporting import DailySourceHealth
from shopee_quality.schema_contract import SOURCE_SCHEMA_CONTRACT


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
        for column in SOURCE_SCHEMA_CONTRACT
    ]


def test_timezone_aware_datetime_normalizes_to_utc() -> None:
    actual = cli.timezone_aware_datetime("2026-09-23T17:00:00+07:00")

    assert actual == datetime(2026, 9, 23, 10, 0, tzinfo=timezone.utc)


def test_timezone_aware_datetime_rejects_naive_value() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="timezone"):
        cli.timezone_aware_datetime("2026-09-23T10:00:00")


@pytest.mark.parametrize("value", ["0", "-1", "not-a-number"])
def test_positive_integer_rejects_invalid_value(value) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="positive"):
        cli.positive_integer(value)


def test_run_batch_validates_schema_and_closes_connections(
    monkeypatch,
) -> None:
    source_connection = MagicMock()
    source_connection.execute.side_effect = [
        MagicMock(fetchall=MagicMock(return_value=metadata_rows_from_contract())),
        MagicMock(fetchone=MagicMock(
            return_value=("2026-09-23T10:00:00.0000000",)
        )),
    ]
    destination_connection = MagicMock()
    monkeypatch.setattr(
        cli,
        "sqlserver_connection_string",
        MagicMock(return_value="test-source-connection"),
    )
    monkeypatch.setattr(
        cli,
        "postgres_connection_kwargs",
        MagicMock(return_value={"dbname": "test-database"}),
    )
    monkeypatch.setattr(
        cli.pyodbc,
        "connect",
        MagicMock(return_value=source_connection),
    )
    monkeypatch.setattr(
        cli.psycopg,
        "connect",
        MagicMock(return_value=destination_connection),
    )
    window = SimpleNamespace(
        start_synced_at=datetime(
            2026, 9, 23, 9, 50, tzinfo=timezone.utc
        ),
        end_synced_at=datetime(
            2026, 9, 23, 10, 0, tzinfo=timezone.utc
        ),
    )
    monkeypatch.setattr(
        cli,
        "resolve_extraction_window",
        MagicMock(return_value=window),
    )
    monkeypatch.setattr(
        cli,
        "ingest_extraction_window",
        MagicMock(
            return_value=BatchResult(
                batch_id=UUID(
                    "11111111-1111-1111-1111-111111111111"
                ),
                extracted_count=2,
                loaded_count=2,
                committed_watermark=window.end_synced_at,
                rejected_count=0,
            )
        ),
    )
    output = MagicMock()

    cli.run_batch(
        SimpleNamespace(
            initial_start=datetime(
                2026, 1, 1, tzinfo=timezone.utc
            ),
            overlap_minutes=10,
            freshness_threshold_minutes=1440,
        ),
        output=output,
    )

    cli.ingest_extraction_window.assert_called_once()
    source_connection.close.assert_called_once_with()
    destination_connection.close.assert_called_once_with()
    messages = [call.args[0] for call in output.call_args_list]
    assert "status=succeeded" in messages
    assert "extracted_count=2" in messages
    assert "rejected_count=0" in messages


def test_run_batch_stops_on_schema_drift_and_closes_connections(
    monkeypatch,
) -> None:
    source_connection = MagicMock()
    source_connection.execute.return_value.fetchall.return_value = []
    destination_connection = MagicMock()
    monkeypatch.setattr(
        cli,
        "sqlserver_connection_string",
        MagicMock(return_value="test-source-connection"),
    )
    monkeypatch.setattr(
        cli,
        "postgres_connection_kwargs",
        MagicMock(return_value={"dbname": "test-database"}),
    )
    monkeypatch.setattr(
        cli.pyodbc,
        "connect",
        MagicMock(return_value=source_connection),
    )
    monkeypatch.setattr(
        cli.psycopg,
        "connect",
        MagicMock(return_value=destination_connection),
    )
    ingest = MagicMock()
    monkeypatch.setattr(cli, "ingest_extraction_window", ingest)

    with pytest.raises(ValueError, match="column count"):
        cli.run_batch(
            SimpleNamespace(
                initial_start=datetime(
                    2026, 1, 1, tzinfo=timezone.utc
                ),
                overlap_minutes=10,
                freshness_threshold_minutes=1440,
            )
        )

    ingest.assert_not_called()
    source_connection.close.assert_called_once_with()
    destination_connection.close.assert_called_once_with()


def test_main_returns_nonzero_without_exposing_environment(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        cli,
        "run_batch",
        MagicMock(side_effect=RuntimeError("connection unavailable")),
    )

    exit_code = cli.main(
        [
            "run-batch",
            "--initial-start",
            "2026-01-01T00:00:00+00:00",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "connection unavailable" in captured.err
    assert "SOURCE_DB_PASSWORD" not in captured.err


def test_show_health_outputs_rows_and_closes_connection(
    monkeypatch,
) -> None:
    connection = MagicMock()
    watermark = datetime(2026, 9, 28, 9, 0, tzinfo=timezone.utc)
    row = DailySourceHealth(
        metric_date=datetime(2026, 9, 28).date(),
        source_name=cli.SOURCE_NAME,
        run_count=3,
        succeeded_run_count=2,
        failed_run_count=1,
        extracted_record_count=100,
        loaded_record_count=98,
        rejected_record_count=2,
        failed_quality_rule_count=4,
        warning_quality_failure_count=3,
        error_quality_failure_count=1,
        latest_committed_watermark=watermark,
        latest_schema_hash="a" * 64,
    )
    monkeypatch.setattr(
        cli,
        "postgres_connection_kwargs",
        MagicMock(return_value={"dbname": "test-database"}),
    )
    monkeypatch.setattr(
        cli.psycopg,
        "connect",
        MagicMock(return_value=connection),
    )
    monkeypatch.setattr(
        cli,
        "fetch_daily_source_health",
        MagicMock(return_value=[row]),
    )
    output = MagicMock()

    actual = cli.show_health(
        SimpleNamespace(days=7, source_name=cli.SOURCE_NAME),
        output=output,
    )

    assert actual == [row]
    cli.fetch_daily_source_health.assert_called_once_with(
        connection=connection,
        days=7,
        source_name=cli.SOURCE_NAME,
    )
    connection.close.assert_called_once_with()
    messages = [call.args[0] for call in output.call_args_list]
    assert messages[0].startswith("metric_date\truns")
    assert "2026-09-28\t3\t2\t1\t100\t98" in messages[1]


def test_show_health_handles_empty_result_and_closes_connection(
    monkeypatch,
) -> None:
    connection = MagicMock()
    monkeypatch.setattr(
        cli,
        "postgres_connection_kwargs",
        MagicMock(return_value={"dbname": "test-database"}),
    )
    monkeypatch.setattr(
        cli.psycopg,
        "connect",
        MagicMock(return_value=connection),
    )
    monkeypatch.setattr(
        cli,
        "fetch_daily_source_health",
        MagicMock(return_value=[]),
    )
    output = MagicMock()

    actual = cli.show_health(
        SimpleNamespace(days=7, source_name=cli.SOURCE_NAME),
        output=output,
    )

    assert actual == []
    output.assert_called_once_with("No source-health records found.")
    connection.close.assert_called_once_with()


def test_main_dispatches_show_health(monkeypatch) -> None:
    show_health = MagicMock(return_value=[])
    monkeypatch.setattr(cli, "show_health", show_health)

    exit_code = cli.main(["show-health", "--days", "3"])

    assert exit_code == 0
    assert show_health.call_args.args[0].days == 3
