from datetime import datetime, timezone
from uuid import uuid4

import psycopg

from shopee_quality.database import postgres_connection_kwargs
from shopee_quality.ingestion import (
    complete_pipeline_run,
    insert_raw_observation,
    start_pipeline_run,
)
from shopee_quality.quality import (
    evaluate_batch_reconciliation,
    record_quality_test_result,
)


def test_postgres_ingestion_transaction() -> None:
    connection = psycopg.connect(
        **postgres_connection_kwargs()
    )
    batch_id = uuid4()
    watermark = datetime.now(timezone.utc)

    try:
        start_pipeline_run(
            connection=connection,
            batch_id=batch_id,
            source_name="integration-test",
            extraction_started_at=watermark,
            overlap_start_synced_at=watermark,
            extraction_end_synced_at=watermark,
        )

        insert_raw_observation(
            connection=connection,
            batch_id=batch_id,
            source_row_number=1,
            source_row_hash="a" * 64,
            synced_at=watermark,
            source_record={"pkId": "integration-test"},
        )

        quality_result = evaluate_batch_reconciliation(
            extracted_count=1,
            loaded_count=1,
            duplicate_count=0,
            rejected_count=0,
        )

        record_quality_test_result(
            connection=connection,
            batch_id=batch_id,
            **quality_result,
        )

        complete_pipeline_run(
            connection=connection,
            batch_id=batch_id,
            extraction_ended_at=datetime.now(timezone.utc),
            committed_watermark=watermark,
            extracted_count=1,
            loaded_count=1,
            duplicate_count=0,
            rejected_count=0,
        )

        run = connection.execute(
            """
            SELECT status, committed_watermark, loaded_count
            FROM monitoring.pipeline_runs
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()

        raw_count = connection.execute(
            """
            SELECT count(*)
            FROM raw.shopee_observations
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()[0]

        recorded_quality = connection.execute(
            """
            SELECT status, affected_row_count
            FROM monitoring.quality_test_results
            WHERE batch_id = %s
              AND rule_code = %s
            """,
            (
                batch_id,
                "batch_row_count_reconciliation",
            ),
        ).fetchone()

        assert run == ("succeeded", watermark, 1)
        assert raw_count == 1
        assert recorded_quality == ("passed", 0)
    finally:
        connection.rollback()
        connection.close()
