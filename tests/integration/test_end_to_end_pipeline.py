from datetime import timedelta
from uuid import uuid4

import psycopg
import pyodbc

from shopee_quality.database import (
    postgres_connection_kwargs,
    sqlserver_connection_string,
)
from shopee_quality.incremental import ExtractionWindow
from shopee_quality.pipeline import (
    ingest_extraction_window,
    parse_source_synced_at,
)


TEST_DATABASE = "shopee_quality_e2e_test"


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
        assert quality_results == [
            (
                "batch_exact_duplicate_count",
                "warning",
                "passed",
                0,
            ),
            (
                "batch_row_count_reconciliation",
                "error",
                "passed",
                0,
            ),
        ]
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
