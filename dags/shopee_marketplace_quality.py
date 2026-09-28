import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


DAG_ID = "shopee_marketplace_quality"
DEFAULT_SCHEDULE = "0 2 * * *"

default_args = {
    "owner": "data-platform",
    "retries": int(os.environ.get("PIPELINE_RETRY_COUNT", "3")),
    "retry_delay": timedelta(
        seconds=int(
            os.environ.get("PIPELINE_RETRY_DELAY_SECONDS", "300")
        )
    ),
}

with DAG(
    dag_id=DAG_ID,
    description=(
        "Incrementally ingest Shopee marketplace observations and publish "
        "data-quality health metrics"
    ),
    default_args=default_args,
    schedule=os.environ.get(
        "AIRFLOW_DAG_SCHEDULE",
        DEFAULT_SCHEDULE,
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["shopee", "data-quality", "incremental"],
) as dag:
    run_incremental_batch = BashOperator(
        task_id="run_incremental_batch",
        bash_command=(
            "set -euo pipefail\n"
            "shopee-quality run-batch "
            "--initial-start "
            "\"${PIPELINE_INITIAL_START:?must be set}\" "
            "--overlap-minutes "
            "\"${INCREMENTAL_OVERLAP_MINUTES:-10}\" "
            "--freshness-threshold-minutes "
            "\"${SOURCE_FRESHNESS_THRESHOLD_MINUTES:-1440}\""
        ),
        append_env=True,
        execution_timeout=timedelta(minutes=90),
    )

    show_health_report = BashOperator(
        task_id="show_health_report",
        bash_command=(
            "set -euo pipefail\n"
            "shopee-quality show-health "
            "--days \"${HEALTH_REPORT_DAYS:-7}\""
        ),
        append_env=True,
        execution_timeout=timedelta(minutes=5),
    )

    run_incremental_batch >> show_health_report
