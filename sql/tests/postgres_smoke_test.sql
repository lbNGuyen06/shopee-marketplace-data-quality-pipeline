DO $$
DECLARE
    missing_schema_count integer;
    missing_relation_count integer;
    missing_constraint_count integer;
    missing_index_count integer;
BEGIN
    SELECT COUNT(*)
    INTO missing_schema_count
    FROM (
        VALUES ('raw'), ('staging'), ('monitoring'), ('marts')
    ) AS expected(schema_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_namespace
        WHERE nspname = expected.schema_name
    );

    IF missing_schema_count > 0 THEN
        RAISE EXCEPTION '% expected PostgreSQL schemas are missing',
            missing_schema_count;
    END IF;

    SELECT COUNT(*)
    INTO missing_relation_count
    FROM (
        VALUES
            ('monitoring.pipeline_runs'),
            ('raw.shopee_observations'),
            ('staging.shopee_records'),
            ('monitoring.rejected_records'),
            ('monitoring.quality_test_results'),
            ('monitoring.schema_snapshots'),
            ('marts.daily_source_health')
    ) AS expected(relation_name)
    WHERE to_regclass(relation_name) IS NULL;

    IF missing_relation_count > 0 THEN
        RAISE EXCEPTION '% expected PostgreSQL relations are missing',
            missing_relation_count;
    END IF;

    SELECT COUNT(*)
    INTO missing_constraint_count
    FROM (
        VALUES
            ('pipeline_runs_status_check'),
            ('pipeline_runs_extraction_time_order_check'),
            ('pipeline_runs_nonnegative_counts_check'),
            ('pipeline_runs_reconciliation_check'),
            ('pipeline_runs_watermark_check'),
            ('pipeline_runs_success_check'),
            ('pipeline_runs_hash_contract_version_check')
    ) AS expected(constraint_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'monitoring.pipeline_runs'::regclass
        AND conname = expected.constraint_name
    );

    IF missing_constraint_count > 0 THEN
        RAISE EXCEPTION '% expected pipeline_runs constraints are missing',
            missing_constraint_count;
    END IF;

    SELECT COUNT(*)
    INTO missing_index_count
    FROM (
        VALUES
            ('raw.shopee_observations_synced_at_idx'),
            ('raw.shopee_observations_source_row_hash_idx'),
            ('staging.shopee_records_synced_at_idx'),
            ('staging.shopee_records_first_seen_batch_idx'),
            ('staging.shopee_records_last_seen_batch_idx'),
            ('monitoring.schema_snapshots_source_history_idx')
    ) AS expected(index_name)
    WHERE to_regclass(expected.index_name) IS NULL;

    IF missing_index_count > 0 THEN
        RAISE EXCEPTION '% expected PostgreSQL indexes are missing',
            missing_index_count;
    END IF;
END
$$;

SELECT
    expected.schema_name,
    COUNT(pg_tables.tablename) AS table_count
FROM (
    VALUES ('raw'), ('staging'), ('monitoring'), ('marts')
) AS expected(schema_name)
LEFT JOIN pg_tables
    ON pg_tables.schemaname = expected.schema_name
GROUP BY expected.schema_name
ORDER BY expected.schema_name;

BEGIN;

INSERT INTO monitoring.pipeline_runs (
    batch_id,
    source_name,
    hash_contract_version,
    status,
    extraction_started_at,
    extraction_ended_at,
    overlap_start_synced_at,
    extraction_end_synced_at,
    committed_watermark,
    extracted_count,
    loaded_count,
    duplicate_count,
    rejected_count
)
VALUES
    (
        '10000000-0000-0000-0000-000000000001',
        'smoke-test-daily-source-health',
        1,
        'succeeded',
        '2026-09-28 01:00:00+00',
        '2026-09-28 01:01:00+00',
        '2026-09-28 00:50:00+00',
        '2026-09-28 01:00:00+00',
        '2026-09-28 01:00:00+00',
        10,
        8,
        2,
        0
    ),
    (
        '10000000-0000-0000-0000-000000000002',
        'smoke-test-daily-source-health',
        1,
        'failed',
        '2026-09-28 02:00:00+00',
        '2026-09-28 02:01:00+00',
        '2026-09-28 01:50:00+00',
        '2026-09-28 02:00:00+00',
        NULL,
        2,
        1,
        0,
        1
    );

INSERT INTO monitoring.quality_test_results (
    batch_id,
    rule_code,
    severity,
    status,
    affected_row_count
)
VALUES
    (
        '10000000-0000-0000-0000-000000000001',
        'smoke_warning',
        'warning',
        'failed',
        1
    ),
    (
        '10000000-0000-0000-0000-000000000001',
        'smoke_passed_error',
        'error',
        'passed',
        0
    ),
    (
        '10000000-0000-0000-0000-000000000002',
        'smoke_error',
        'error',
        'failed',
        1
    );

INSERT INTO monitoring.schema_snapshots (
    batch_id,
    source_schema,
    source_table,
    schema_hash,
    column_count,
    schema_definition
)
VALUES
    (
        '10000000-0000-0000-0000-000000000001',
        'vietnam_ecommerce',
        'shopee_orders',
        repeat('a', 64),
        84,
        '[]'::jsonb
    ),
    (
        '10000000-0000-0000-0000-000000000002',
        'vietnam_ecommerce',
        'shopee_orders',
        repeat('b', 64),
        84,
        '[]'::jsonb
    );

DO $$
DECLARE
    health_record record;
BEGIN
    SELECT *
    INTO health_record
    FROM marts.daily_source_health
    WHERE metric_date = DATE '2026-09-28'
      AND source_name = 'smoke-test-daily-source-health';

    IF health_record.run_count <> 2
       OR health_record.succeeded_run_count <> 1
       OR health_record.failed_run_count <> 1
       OR health_record.extracted_record_count <> 12
       OR health_record.loaded_record_count <> 9
       OR health_record.rejected_record_count <> 1
       OR health_record.failed_quality_rule_count <> 2
       OR health_record.warning_quality_failure_count <> 1
       OR health_record.error_quality_failure_count <> 1
       OR health_record.latest_committed_watermark <>
            '2026-09-28 01:00:00+00'::timestamptz
       OR health_record.latest_schema_hash <> repeat('b', 64)
    THEN
        RAISE EXCEPTION
            'daily_source_health returned unexpected aggregate: %',
            row_to_json(health_record);
    END IF;
END
$$;

ROLLBACK;
