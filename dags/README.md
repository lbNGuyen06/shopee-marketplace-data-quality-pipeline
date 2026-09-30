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

## Local Airflow runtime

The Compose `airflow` profile runs Airflow standalone for local development.
It uses an isolated SQLite metadata database in the `airflow_data` volume; the
pipeline continues to write analytical data to the PostgreSQL service.

Build and start the runtime:

```shell
docker compose --profile airflow build airflow
docker compose --profile airflow up -d --wait airflow
```

Validate the DAG without triggering a pipeline run:

```shell
docker compose --profile airflow exec -T airflow airflow dags list
docker compose --profile airflow exec -T airflow \
  airflow dags list-import-errors
docker compose --profile airflow exec -T airflow \
  airflow tasks list shopee_marketplace_quality
```

The local UI is available on port `8080` by default. Read the generated local
administrator credentials from the Airflow container logs. Standalone mode is
for local development and portfolio demonstration, not production deployment.
