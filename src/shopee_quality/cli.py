import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Optional, Sequence

import psycopg
import pyodbc
from dotenv import load_dotenv

from shopee_quality.database import (
    postgres_connection_kwargs,
    sqlserver_connection_string,
)
from shopee_quality.incremental import resolve_extraction_window
from shopee_quality.pipeline import (
    ingest_extraction_window,
    parse_source_synced_at,
)
from shopee_quality.schema_contract import (
    build_source_schema_query,
    validate_source_schema,
)


SOURCE_NAME = "xomdb.vietnam_ecommerce.shopee_orders"
SOURCE_MAX_SYNCED_AT_QUERY = """
SELECT
    CONVERT(varchar(19), MAX([synced_at]), 126) + '.' +
    RIGHT(
        '0000000' + CONVERT(
            varchar(7),
            DATEPART(NANOSECOND, MAX([synced_at])) / 100
        ),
        7
    )
FROM [vietnam_ecommerce].[shopee_orders]
"""


def timezone_aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be an ISO 8601 datetime"
        ) from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "must include a timezone offset"
        )

    return parsed.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shopee-quality",
        description="Shopee Marketplace Data Quality Pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser(
        "run-batch",
        help="validate the source schema and ingest one incremental batch",
    )
    run_parser.add_argument(
        "--initial-start",
        required=True,
        type=timezone_aware_datetime,
        help="initial UTC-aware ISO 8601 watermark",
    )
    run_parser.add_argument(
        "--overlap-minutes",
        type=int,
        default=int(os.environ.get("INCREMENTAL_OVERLAP_MINUTES", "10")),
    )
    run_parser.add_argument(
        "--freshness-threshold-minutes",
        type=int,
        default=int(
            os.environ.get(
                "SOURCE_FRESHNESS_THRESHOLD_MINUTES",
                "1440",
            )
        ),
    )
    return parser


def run_batch(args, output=print):
    source_connection = pyodbc.connect(sqlserver_connection_string())
    destination_connection = None

    try:
        destination_connection = psycopg.connect(
            **postgres_connection_kwargs()
        )
        schema_query, schema_parameters = build_source_schema_query()
        schema_rows = source_connection.execute(
            schema_query,
            schema_parameters,
        ).fetchall()
        validate_source_schema(schema_rows)

        maximum_synced_at = source_connection.execute(
            SOURCE_MAX_SYNCED_AT_QUERY
        ).fetchone()[0]
        if maximum_synced_at is None:
            raise RuntimeError("Source table has no synced_at value")

        extraction_end = parse_source_synced_at(maximum_synced_at)
        window = resolve_extraction_window(
            connection=destination_connection,
            source_name=SOURCE_NAME,
            extraction_end=extraction_end,
            overlap_minutes=args.overlap_minutes,
            initial_start=args.initial_start,
        )
        result = ingest_extraction_window(
            source_connection=source_connection,
            destination_connection=destination_connection,
            source_name=SOURCE_NAME,
            window=window,
            freshness_threshold_minutes=(
                args.freshness_threshold_minutes
            ),
        )

        output(f"batch_id={result.batch_id}")
        output("status=succeeded")
        output(f"window_start={window.start_synced_at.isoformat()}")
        output(f"window_end={window.end_synced_at.isoformat()}")
        output(f"extracted_count={result.extracted_count}")
        output(f"loaded_count={result.loaded_count}")
        return result
    finally:
        if destination_connection is not None:
            destination_connection.close()
        source_connection.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv(override=False)
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "run-batch":
            run_batch(args)
            return 0
    except Exception as exc:
        print(
            f"error={type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1

    parser.error(f"unsupported command: {args.command}")
    return 2
