from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


DAILY_SOURCE_HEALTH_QUERY = """
SELECT
    metric_date,
    source_name,
    run_count,
    succeeded_run_count,
    failed_run_count,
    extracted_record_count,
    loaded_record_count,
    rejected_record_count,
    failed_quality_rule_count,
    warning_quality_failure_count,
    error_quality_failure_count,
    latest_committed_watermark,
    latest_schema_hash
FROM marts.daily_source_health
WHERE metric_date >= CURRENT_DATE - (%s - 1)
  AND source_name = %s
ORDER BY metric_date DESC
"""


@dataclass(frozen=True)
class DailySourceHealth:
    metric_date: date
    source_name: str
    run_count: int
    succeeded_run_count: int
    failed_run_count: int
    extracted_record_count: int
    loaded_record_count: int
    rejected_record_count: int
    failed_quality_rule_count: int
    warning_quality_failure_count: int
    error_quality_failure_count: int
    latest_committed_watermark: Optional[datetime]
    latest_schema_hash: Optional[str]


def fetch_daily_source_health(
    connection,
    days: int,
    source_name: str,
) -> list[DailySourceHealth]:
    if days < 1:
        raise ValueError("days must be at least 1")
    if not source_name.strip():
        raise ValueError("source_name must not be empty")

    rows = connection.execute(
        DAILY_SOURCE_HEALTH_QUERY,
        (days, source_name),
    ).fetchall()
    return [DailySourceHealth(*row) for row in rows]
