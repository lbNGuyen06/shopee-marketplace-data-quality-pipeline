import argparse
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from shopee_quality import cli
from shopee_quality.pipeline import BatchResult
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
