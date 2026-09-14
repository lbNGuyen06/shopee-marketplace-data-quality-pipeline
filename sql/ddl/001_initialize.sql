BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs(
    batch_id uuid PRIMARY KEY,
    source_name text NOT NULL,
    status text NOT NULL,
    extraction_started_at timestamptz NOT NULL ,
    extraction_ended_at timestamptz,
    overlap_start_synced_at timestamptz,
    extraction_end_synced_at timestamptz,
    committed_watermark timestamptz,
    extracted_count bigint,
    loaded_count bigint,
    duplicate_count bigint,
    rejected_count bigint,
    error_summary text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pipeline_runs_status_check
        CHECK(status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT pipeline_runs_extraction_time_order_check
        CHECK( extraction_ended_at IS NULL
               OR extraction_ended_at >= extraction_started_at ),
    CONSTRAINT pipeline_runs_nonnegative_counts_check
        CHECK (
            (extracted_count IS NULL OR extracted_count >= 0)
            AND (loaded_count IS NULL OR loaded_count >= 0)
            AND (duplicate_count IS NULL OR duplicate_count >= 0)
            AND (rejected_count IS NULL OR rejected_count >= 0)
        ),
    CONSTRAINT pipeline_runs_reconciliation_check
        CHECK (
            (
                extracted_count IS NULL
                AND loaded_count IS NULL
                AND duplicate_count IS NULL
                AND rejected_count IS NULL
            )
            OR (
                extracted_count IS NOT NULL
                AND loaded_count IS NOT NULL
                AND duplicate_count IS NOT NULL
                AND rejected_count IS NOT NULL
                AND extracted_count =
                    loaded_count + duplicate_count + rejected_count
            )
    ),
    CONSTRAINT pipeline_runs_watermark_check
        CHECK (
            committed_watermark IS NULL
            OR status = 'succeeded'
        ),
    CONSTRAINT pipeline_runs_success_check
        CHECK (
            status <> 'succeeded'
            OR (
                extraction_ended_at IS NOT NULL
                AND committed_watermark IS NOT NULL
                AND extracted_count IS NOT NULL
            )
        )
);
CREATE TABLE IF NOT EXISTS raw.shopee_observations(
    ingestion_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id uuid NOT NULL
        REFERENCES monitoring.pipeline_runs(batch_id),
    source_row_number bigint NOT NULL,
    source_row_hash char(64) NOT NULL,
    synced_at timestamptz NOT NULL ,
    observed_at timestamptz NOT NULL DEFAULT now(),
    source_record jsonb NOT NULL,
    CONSTRAINT shopee_observations_source_row_check
        CHECK (source_row_number > 0),
    CONSTRAINT shopee_observations_source_hash_check
        CHECK (source_row_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT shopee_observations_batch_unique
        UNIQUE (batch_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS staging.shopee_records
(
    source_row_hash char(64) PRIMARY KEY ,
    first_seen_batch_id uuid NOT NULL
        REFERENCES monitoring.pipeline_runs (batch_id),
    last_seen_batch_id uuid NOT NULL
        REFERENCES monitoring.pipeline_runs (batch_id),
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    synced_at timestamptz NOT NULL,
    standardized_record jsonb NOT NULL,
    CONSTRAINT shopee_records_hash_format_check
        CHECK (source_row_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT shopee_records_seen_order_check
        CHECK (last_seen_at >= first_seen_at)
);

CREATE TABLE IF NOT EXISTS monitoring.rejected_records(
    rejection_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id uuid NOT NULL
        REFERENCES monitoring.pipeline_runs(batch_id),
    source_row_number bigint NOT NULL,
    source_row_hash char(64),
    rejected_at timestamptz NOT NULL DEFAULT now(),
    rule_code text NOT NULL,
    reason text NOT NULL,
    source_record jsonb NOT NULL,
    CONSTRAINT rejected_records_row_number_check
        CHECK (source_row_number > 0),
    CONSTRAINT rejected_records_hash_format_check
        CHECK (
            source_row_hash IS NULL
            OR source_row_hash ~ '^[0-9a-f]{64}$'
        ),
    CONSTRAINT rejected_records_batch_row_unique
        UNIQUE (batch_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS monitoring.quality_test_results(
    quality_test_result_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id uuid NOT NULL
        REFERENCES monitoring.pipeline_runs(batch_id),
    rule_code text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL,
    observed_value numeric,
    threshold_value numeric,
    affected_row_count bigint NOT NULL DEFAULT 0,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    evaluated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT quality_test_results_severity_check
        CHECK (severity IN ('info', 'warning', 'error')),
    CONSTRAINT quality_test_results_status_check
        CHECK (status IN ('passed', 'failed')),
    CONSTRAINT quality_test_results_affected_rows_check
        CHECK (affected_row_count >= 0),
    CONSTRAINT quality_test_results_batch_rule_unique
        UNIQUE (batch_id, rule_code)
);

CREATE TABLE IF NOT EXISTS monitoring.schema_snapshots(
    schema_snapshot_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id uuid NOT NULL
        REFERENCES monitoring.pipeline_runs(batch_id),
    source_schema text NOT NULL,
    source_table text NOT NULL,
    schema_hash char(64) NOT NULL,
    column_count integer NOT NULL,
    schema_definition jsonb NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT schema_snapshots_hash_format_check
        CHECK (schema_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT schema_snapshots_column_count_check
        CHECK (column_count > 0),
    CONSTRAINT schema_snapshots_batch_source_unique
        UNIQUE (batch_id, source_schema, source_table)
);
CREATE INDEX IF NOT EXISTS shopee_observations_synced_at_idx
    ON raw.shopee_observations (synced_at);

CREATE INDEX IF NOT EXISTS shopee_observations_source_row_hash_idx
    ON raw.shopee_observations (source_row_hash);

CREATE INDEX IF NOT EXISTS shopee_records_synced_at_idx
    ON staging.shopee_records (synced_at);

CREATE INDEX IF NOT EXISTS shopee_records_first_seen_batch_idx
    ON staging.shopee_records (first_seen_batch_id);

CREATE INDEX IF NOT EXISTS shopee_records_last_seen_batch_idx
    ON staging.shopee_records (last_seen_batch_id);

CREATE INDEX IF NOT EXISTS schema_snapshots_source_history_idx
    ON monitoring.schema_snapshots (
        source_schema,
        source_table,
        captured_at DESC
    );
COMMIT;
