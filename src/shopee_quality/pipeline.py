from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional
from uuid import UUID, uuid4

from shopee_quality.extraction import (
    build_source_query,
    source_row_to_record,
)
from shopee_quality.hashing import compute_source_row_hash
from shopee_quality.incremental import ExtractionWindow
from shopee_quality.ingestion import (
    complete_pipeline_run,
    fail_pipeline_run,
    insert_raw_observation,
    start_pipeline_run,
)
from shopee_quality.quality import (
    count_batch_exact_duplicates,
    evaluate_batch_exact_duplicates,
    evaluate_batch_reconciliation,
    record_quality_test_result,
)
from shopee_quality.staging import upsert_staging_record


@dataclass(frozen=True)
class BatchResult:
    batch_id: UUID
    extracted_count: int
    loaded_count: int
    committed_watermark: datetime


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_source_synced_at(value: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError("synced_at must be a canonical datetime string")

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid canonical synced_at value: {value!r}"
        ) from exc

    if parsed.tzinfo is not None:
        raise ValueError("Source synced_at must not contain a timezone")

    return parsed.replace(tzinfo=timezone.utc)


def ingest_extraction_window(
    source_connection,
    destination_connection,
    source_name: str,
    window: ExtractionWindow,
    batch_id: Optional[UUID] = None,
    clock: Callable[[], datetime] = utc_now,
) -> BatchResult:
    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("source_name must be a non-empty string")
    if not isinstance(window, ExtractionWindow):
        raise TypeError("window must be an ExtractionWindow")

    resolved_batch_id = batch_id or uuid4()
    extraction_started_at = clock()

    start_pipeline_run(
        connection=destination_connection,
        batch_id=resolved_batch_id,
        source_name=source_name,
        extraction_started_at=extraction_started_at,
        overlap_start_synced_at=window.start_synced_at,
        extraction_end_synced_at=window.end_synced_at,
    )
    destination_connection.commit()

    try:
        query, parameters = build_source_query(window)
        source_rows = source_connection.execute(
            query,
            parameters,
        ).fetchall()

        for source_row_number, row in enumerate(source_rows, start=1):
            source_record = source_row_to_record(row)
            source_row_hash = compute_source_row_hash(source_record)
            synced_at = parse_source_synced_at(
                source_record["synced_at"]
            )

            insert_raw_observation(
                connection=destination_connection,
                batch_id=resolved_batch_id,
                source_row_number=source_row_number,
                source_row_hash=source_row_hash,
                synced_at=synced_at,
                source_record=source_record,
            )
            upsert_staging_record(
                connection=destination_connection,
                batch_id=resolved_batch_id,
                source_row_hash=source_row_hash,
                observed_at=extraction_started_at,
                synced_at=synced_at,
                standardized_record=source_record,
            )

        exact_duplicate_count = count_batch_exact_duplicates(
            connection=destination_connection,
            batch_id=resolved_batch_id,
        )
        duplicate_quality_result = evaluate_batch_exact_duplicates(
            exact_duplicate_count
        )
        record_quality_test_result(
            connection=destination_connection,
            batch_id=resolved_batch_id,
            **duplicate_quality_result,
        )

        extracted_count = len(source_rows)
        quality_result = evaluate_batch_reconciliation(
            extracted_count=extracted_count,
            loaded_count=extracted_count,
            duplicate_count=0,
            rejected_count=0,
        )
        record_quality_test_result(
            connection=destination_connection,
            batch_id=resolved_batch_id,
            **quality_result,
        )
        extraction_ended_at = clock()
        complete_pipeline_run(
            connection=destination_connection,
            batch_id=resolved_batch_id,
            extraction_ended_at=extraction_ended_at,
            committed_watermark=window.end_synced_at,
            extracted_count=extracted_count,
            loaded_count=extracted_count,
            duplicate_count=0,
            rejected_count=0,
        )
        destination_connection.commit()
    except Exception as exc:
        destination_connection.rollback()
        fail_pipeline_run(
            connection=destination_connection,
            batch_id=resolved_batch_id,
            extraction_ended_at=clock(),
            error_summary=(
                f"{type(exc).__name__}: {exc}"
            )[:2000],
        )
        destination_connection.commit()
        raise

    return BatchResult(
        batch_id=resolved_batch_id,
        extracted_count=extracted_count,
        loaded_count=extracted_count,
        committed_watermark=window.end_synced_at,
    )
