BEGIN;

CREATE VIEW marts.daily_source_health AS
WITH quality_by_batch AS (
    SELECT
        batch_id,
        COUNT(*) FILTER (
            WHERE status = 'failed'
        ) AS failed_quality_rule_count,
        COUNT(*) FILTER (
            WHERE status = 'failed'
              AND severity = 'warning'
        ) AS warning_quality_failure_count,
        COUNT(*) FILTER (
            WHERE status = 'failed'
              AND severity = 'error'
        ) AS error_quality_failure_count
    FROM monitoring.quality_test_results
    GROUP BY batch_id
),
schema_by_batch AS (
    SELECT
        batch_id,
        MAX(schema_hash::text) AS schema_hash
    FROM monitoring.schema_snapshots
    GROUP BY batch_id
)
SELECT
    (
        pipeline.extraction_started_at AT TIME ZONE 'UTC'
    )::date AS metric_date,
    pipeline.source_name,
    COUNT(*) AS run_count,
    COUNT(*) FILTER (
        WHERE pipeline.status = 'succeeded'
    ) AS succeeded_run_count,
    COUNT(*) FILTER (
        WHERE pipeline.status = 'failed'
    ) AS failed_run_count,
    COALESCE(SUM(pipeline.extracted_count), 0) AS extracted_record_count,
    COALESCE(SUM(pipeline.loaded_count), 0) AS loaded_record_count,
    COALESCE(SUM(pipeline.rejected_count), 0) AS rejected_record_count,
    COALESCE(SUM(quality.failed_quality_rule_count), 0)
        AS failed_quality_rule_count,
    COALESCE(SUM(quality.warning_quality_failure_count), 0)
        AS warning_quality_failure_count,
    COALESCE(SUM(quality.error_quality_failure_count), 0)
        AS error_quality_failure_count,
    MAX(pipeline.committed_watermark) AS latest_committed_watermark,
    (
        ARRAY_AGG(
            schema_snapshot.schema_hash
            ORDER BY
                pipeline.extraction_started_at DESC,
                pipeline.batch_id DESC
        ) FILTER (
            WHERE schema_snapshot.schema_hash IS NOT NULL
        )
    )[1] AS latest_schema_hash
FROM monitoring.pipeline_runs AS pipeline
LEFT JOIN quality_by_batch AS quality
    ON quality.batch_id = pipeline.batch_id
LEFT JOIN schema_by_batch AS schema_snapshot
    ON schema_snapshot.batch_id = pipeline.batch_id
GROUP BY
    (
        pipeline.extraction_started_at AT TIME ZONE 'UTC'
    )::date,
    pipeline.source_name;

COMMIT;
