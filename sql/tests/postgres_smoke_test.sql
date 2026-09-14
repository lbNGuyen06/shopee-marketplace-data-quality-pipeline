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
            ('monitoring.schema_snapshots')
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
            ('pipeline_runs_success_check')
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