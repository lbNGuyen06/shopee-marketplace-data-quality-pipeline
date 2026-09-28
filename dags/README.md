# Airflow DAGs

This directory contains orchestration definitions only. Business logic must
remain in the `src` package so it can be tested without running Airflow.

`shopee_marketplace_quality.py` targets Apache Airflow 3.3.2. It schedules one
incremental ingestion followed by the read-only source-health report. The DAG
does not contain database credentials or transformation logic.

Required worker environment:

- `PIPELINE_INITIAL_START`
- SQL Server variables documented in `.env.example`
- PostgreSQL variables documented in `.env.example`

Optional scheduling variables:

- `AIRFLOW_DAG_SCHEDULE` defaults to `0 2 * * *` (02:00 UTC daily).
- `PIPELINE_RETRY_COUNT` defaults to `3`.
- `PIPELINE_RETRY_DELAY_SECONDS` defaults to `300`.
- `HEALTH_REPORT_DAYS` defaults to `7`.

The DAG disables catchup and allows only one active DAG run so concurrent runs
cannot race while resolving and committing the PostgreSQL watermark.
