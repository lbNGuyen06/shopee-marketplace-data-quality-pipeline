from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pyodbc
import pytest

from shopee_quality.database import (
    postgres_connection_kwargs,
    sqlserver_connection_string,
)
from shopee_quality.incremental import ExtractionWindow
from shopee_quality.hashing import (
    CANONICAL_FIELDS,
    DATETIME_FIELDS,
    DECIMAL_FIELDS,
    INTEGER_FIELDS,
)
from shopee_quality.pipeline import (
    QualityGateError,
    ingest_extraction_window,
    parse_source_synced_at,
)


TEST_DATABASE = "shopee_quality_e2e_test"


def build_source_row(synced_at: str) -> tuple:
    values = []

    for field_name in CANONICAL_FIELDS:
        if field_name == "synced_at":
            values.append(synced_at)
        elif field_name in DATETIME_FIELDS:
            values.append("2026-09-23T09:55:00.0000000")
        elif field_name in DECIMAL_FIELDS:
            values.append(Decimal("100.00"))
        elif field_name in INTEGER_FIELDS:
            values.append(1)
        else:
            values.append(f"value-{field_name}")

    return tuple(values)


def test_end_to_end_pipeline() -> None:
    postgres_config = postgres_connection_kwargs()
    assert postgres_config["dbname"] == TEST_DATABASE, (
        "End-to-end test must run against the isolated test database"
    )

    batch_id = uuid4()
    source_connection = pyodbc.connect(
        sqlserver_connection_string()
    )
    destination_connection = psycopg.connect(**postgres_config)

    try:
        existing_staging_count = destination_connection.execute(
            "SELECT count(*) FROM staging.shopee_records"
        ).fetchone()[0]
        assert existing_staging_count == 0, (
            "End-to-end test requires an empty staging table"
        )
        destination_connection.rollback()

        maximum_synced_at = source_connection.execute(
            """
            SELECT CONVERT(varchar(27), MAX([synced_at]), 126)
            FROM [vietnam_ecommerce].[shopee_orders]
            """
        ).fetchone()[0]

        assert maximum_synced_at is not None
        extraction_end = parse_source_synced_at(maximum_synced_at)
        window = ExtractionWindow(
            start_synced_at=extraction_end - timedelta(minutes=10),
            end_synced_at=extraction_end,
        )

        result = ingest_extraction_window(
            source_connection=source_connection,
            destination_connection=destination_connection,
            source_name="xomdb.vietnam_ecommerce.shopee_orders",
            window=window,
            batch_id=batch_id,
        )

        run = destination_connection.execute(
            """
            SELECT
                status,
                committed_watermark,
                extracted_count,
                loaded_count,
                duplicate_count,
                rejected_count
            FROM monitoring.pipeline_runs
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()
        raw_count = destination_connection.execute(
            """
            SELECT count(*)
            FROM raw.shopee_observations
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()[0]
        raw_contract = destination_connection.execute(
            """
            SELECT
                length(source_row_hash),
                (
                    SELECT count(*)
                    FROM jsonb_object_keys(source_record)
                )
            FROM raw.shopee_observations
            WHERE batch_id = %s
            ORDER BY source_row_number
            LIMIT 1
            """,
            (batch_id,),
        ).fetchone()
        quality_results = destination_connection.execute(
            """
            SELECT rule_code, severity, status, affected_row_count
            FROM monitoring.quality_test_results
            WHERE batch_id = %s
            ORDER BY rule_code
            """,
            (batch_id,),
        ).fetchall()

        assert result.batch_id == batch_id
        assert result.extracted_count > 0
        assert run == (
            "succeeded",
            window.end_synced_at,
            result.extracted_count,
            result.loaded_count,
            0,
            0,
        )
        assert raw_count == result.loaded_count
        assert raw_contract == (64, 84)
        quality_by_rule = {
            rule_code: (severity, status, affected_row_count)
            for (
                rule_code,
                severity,
                status,
                affected_row_count,
            ) in quality_results
        }
        assert quality_by_rule[
            "batch_exact_duplicate_count"
        ] == ("warning", "passed", 0)
        assert quality_by_rule[
            "batch_row_count_reconciliation"
        ] == ("error", "passed", 0)
        assert quality_by_rule[
            "batch_synced_at_window_violation_count"
        ] == ("error", "passed", 0)
        assert quality_by_rule[
            "source_freshness_lag_minutes"
        ] == ("warning", "failed", 1)

        masked_collision = quality_by_rule[
            "masked_pkid_collision_count"
        ]
        assert masked_collision[0] == "warning"
        assert masked_collision[1] in {"passed", "failed"}
        assert masked_collision[2] >= 0
        assert masked_collision[1] == (
            "passed" if masked_collision[2] == 0 else "failed"
        )
    finally:
        destination_connection.rollback()
        destination_connection.execute(
            """
            DELETE FROM staging.shopee_records
            WHERE first_seen_batch_id = %s
              AND last_seen_batch_id = %s
            """,
            (batch_id, batch_id),
        )
        destination_connection.execute(
            """
            DELETE FROM monitoring.quality_test_results
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        destination_connection.execute(
            """
            DELETE FROM monitoring.rejected_records
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        destination_connection.execute(
            """
            DELETE FROM monitoring.schema_snapshots
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        destination_connection.execute(
            """
            DELETE FROM raw.shopee_observations
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        destination_connection.execute(
            """
            DELETE FROM monitoring.pipeline_runs
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        destination_connection.commit()
        destination_connection.close()
        source_connection.close()


def test_window_violation_fails_batch_without_raw_or_watermark() -> None:
    postgres_config = postgres_connection_kwargs()
    assert postgres_config["dbname"] == TEST_DATABASE

    batch_id = uuid4()
    source_connection = MagicMock()
    source_connection.execute.return_value.fetchall.return_value = [
        build_source_row("2026-09-23T09:49:59.9999999")
    ]
    destination_connection = psycopg.connect(**postgres_config)
    window = ExtractionWindow(
        start_synced_at=datetime(
            2026, 9, 23, 9, 50, tzinfo=timezone.utc
        ),
        end_synced_at=datetime(
            2026, 9, 23, 10, 0, tzinfo=timezone.utc
        ),
    )

    try:
        with pytest.raises(QualityGateError, match="outside"):
            ingest_extraction_window(
                source_connection=source_connection,
                destination_connection=destination_connection,
                source_name="integration.window-violation",
                window=window,
                batch_id=batch_id,
            )

        run = destination_connection.execute(
            """
            SELECT status, committed_watermark
            FROM monitoring.pipeline_runs
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()
        raw_count = destination_connection.execute(
            """
            SELECT count(*)
            FROM raw.shopee_observations
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()[0]
        staging_count = destination_connection.execute(
            """
            SELECT count(*)
            FROM staging.shopee_records
            WHERE first_seen_batch_id = %s
               OR last_seen_batch_id = %s
            """,
            (batch_id, batch_id),
        ).fetchone()[0]
        quality_result = destination_connection.execute(
            """
            SELECT status, affected_row_count
            FROM monitoring.quality_test_results
            WHERE batch_id = %s
              AND rule_code = %s
            """,
            (
                batch_id,
                "batch_synced_at_window_violation_count",
            ),
        ).fetchone()

        assert run == ("failed", None)
        assert raw_count == 0
        assert staging_count == 0
        assert quality_result == ("failed", 1)
    finally:
        destination_connection.rollback()
        destination_connection.execute(
            """
            DELETE FROM monitoring.quality_test_results
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        destination_connection.execute(
            """
            DELETE FROM monitoring.pipeline_runs
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        destination_connection.commit()
        destination_connection.close()
