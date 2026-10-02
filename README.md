# Shopee Marketplace Data Quality Pipeline

[![CI](https://github.com/lbNGuyen06/shopee-marketplace-data-quality-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/lbNGuyen06/shopee-marketplace-data-quality-pipeline/actions/workflows/ci.yml)

A reproducible batch pipeline that incrementally extracts anonymized Shopee
operational records, preserves immutable source snapshots, validates data
quality, monitors identifier collisions, and publishes source-health metrics
to PostgreSQL.

The upstream dataset intentionally masks operational and personal identifiers.
Because the masking does not preserve order-level uniqueness, this project does
not report unique-order or customer-level metrics. Its primary purpose is
reliable ingestion, observability, reconciliation, and privacy-aware data
processing.

> Project status: complete portfolio implementation with local orchestration,
> automated quality controls, and CI validation.

## Business Problem

Downstream analysts need to know whether marketplace records are complete,
fresh, structurally valid, and safely reproducible before using them.

The source contains anonymized operational identifiers. Profiling found that
masked identifiers can collide, so treating them as unique order or order-item
keys would produce unreliable analytics.

This pipeline addresses that risk by:

- Preserving immutable source observations.
- Detecting identifier collisions and exact duplicate records.
- Monitoring freshness, null rates, schema changes, and batch volume.
- Reconciling extracted, loaded, rejected, and duplicate records.
- Preventing personally identifiable information from appearing in logs,
  tests, and public documentation.

## Architecture

See [the architecture document](docs/architecture.md).

## Data Model

The source grain cannot be reliably identified as one unique order or order
item because upstream masking creates identifier collisions.

The pipeline therefore uses an append-only observation model:

- `raw`: immutable source observations with batch metadata.
- `staging`: technically standardized records and record fingerprints.
- `monitoring`: pipeline runs, quality-test results, schema snapshots, and
  identifier-collision measurements.
- `marts`: source-health, freshness, status-distribution, and SKU-activity
  summaries.

A generated `ingestion_id` is used as the physical primary key. A SHA-256
`source_row_hash` detects identical record content but is not presented as a
business order identifier.

## Reliability and Data Quality

The pipeline implements:

- Incremental extraction using `synced_at` with a configurable overlap window.
- Idempotent loading and safe reprocessing.
- Schema, null, uniqueness, range, and lifecycle validation.
- Source-to-target row-count reconciliation.
- Rejected-record isolation.
- Structured logging without personally identifiable information.

## Technology Decisions

| Technology     | Purpose                   | Reason                                                      |
| -------------- | ------------------------- | ----------------------------------------------------------- |
| Python         | Extraction and validation | Appropriate for API/database I/O and a small dataset        |
| PostgreSQL     | Analytical storage        | Supports SQL transformations, constraints, and upserts      |
| Apache Airflow | Orchestration             | Provides dependencies, retries, scheduling, and run history |
| Docker Compose | Local runtime             | Makes the environment reproducible across machines          |
| Pytest         | Automated tests           | Verifies transformation and validation logic                |

## Local Setup

Prerequisites: Docker Desktop with Docker Compose.

1. Copy `.env.example` to `.env` and set local credentials.
2. Start PostgreSQL and wait for its health check:

   ```shell
   docker compose up -d --wait postgres
   ```

3. Verify the initialized schemas and tables:

   ```shell
   docker compose exec -T postgres sh -c 'psql \
     -v ON_ERROR_STOP=1 \
     -U "$POSTGRES_USER" \
     -d "$POSTGRES_DB" \
     -f /workspace/sql/tests/postgres_smoke_test.sql'
   ```

The SQL files in `sql/ddl` run automatically only when the PostgreSQL data
volume is first initialized. Apply later schema changes through explicit,
versioned migrations rather than recreating a populated volume.

After applying all migrations, display the last seven days of aggregate
pipeline-health metrics without reconnecting to the source database:

```shell
shopee-quality show-health --days 7
```

Use `--source-name` only when querying a source name other than
`xomdb.vietnam_ecommerce.shopee_orders`.

### Local Airflow orchestration

The optional Airflow profile uses a custom Linux image with Microsoft ODBC
Driver 18 and the pipeline package installed. Build and start it with:

```shell
docker compose --profile airflow build airflow
docker compose --profile airflow up -d --wait airflow
```

Confirm that Airflow imports the DAG and its two tasks without triggering a
new batch:

```shell
docker compose --profile airflow exec -T airflow airflow dags list
docker compose --profile airflow exec -T airflow \
  airflow dags list-import-errors
docker compose --profile airflow exec -T airflow \
  airflow tasks list shopee_marketplace_quality
```

The UI is served on `http://localhost:8080` by default. The standalone runtime
is intended only for local development; its metadata is stored separately from
the pipeline's PostgreSQL analytical data.

## Continuous Integration

GitHub Actions runs two secret-free checks on every push and pull request:

- Unit tests, DAG configuration tests, whitespace checks, and Docker Compose
  validation on Python 3.13.
- PostgreSQL initialization, the SQL smoke test, and the isolated PostgreSQL
  ingestion integration test.

The CI workflow uses synthetic PostgreSQL credentials and never connects to
the real SQL Server source. The full SQL Server-to-PostgreSQL run remains an
explicit local acceptance test because it requires private source credentials.

## Challenges and Solutions

| Challenge | Implemented solution |
| --------- | -------------------- |
| Masked identifiers collide and cannot prove order-level uniqueness | Model immutable source observations with generated ingestion IDs; use deterministic record hashes only for content identity |
| Incremental reads can miss late-arriving records | Extract by `synced_at` with an overlap window, deduplicate exact content in staging, and advance the watermark only after reconciliation succeeds |
| The 84-column upstream schema can drift | Validate ordered column metadata against a versioned contract and persist a schema snapshot for every run |
| Individual source rows can be malformed | Isolate rejected records with privacy-safe diagnostics while allowing valid observations in the same batch to continue |
| Operational monitoring must not expose source PII | Emit structured batch-level logs and publish only aggregate health metrics |
| Local orchestration and automated tests need different credentials | Run the full source acceptance test locally, while CI uses synthetic credentials and an isolated PostgreSQL instance |

## Verified Outcomes

- Profiled a changing 84-column source and documented why masked identifiers
  cannot support trustworthy order, customer, or GMV metrics.
- Implemented an end-to-end incremental path from schema validation and
  extraction through immutable raw storage, staging, quality checks, and a
  daily source-health mart.
- Verified idempotency, reconciliation, rejected-record handling, schema
  drift detection, freshness checks, collision monitoring, and watermark
  safety through automated tests.
- Ran the Airflow DAG successfully against the real source and local
  PostgreSQL destination without publishing credentials or source records.
- Validated every push and pull request through GitHub Actions using unit,
  SQL smoke, Docker Compose, and PostgreSQL integration checks.

## Repository Guide

| Path | Responsibility |
| ---- | -------------- |
| `src/shopee_quality/` | Extraction, contracts, ingestion, quality rules, observability, reporting, and CLI code |
| `sql/ddl/` | Initial PostgreSQL objects and ordered schema migrations |
| `sql/profiling/` | Read-only source profiling queries |
| `sql/tests/` | PostgreSQL smoke tests |
| `dags/` | Airflow orchestration entry point |
| `tests/unit/` | Fast deterministic behavior tests |
| `tests/integration/` | PostgreSQL and private source acceptance tests |
| `docs/` | Architecture, source evidence, and record-hash contract |

## Privacy

The source schema includes personally identifiable information such as recipient
names, phone numbers, usernames, and delivery addresses. Raw data and
credentials are excluded from version control. Published examples and tests
will use synthetic records only.

## Limitations

- The source is a limited sample rather than a live Shopee API.
- The latest profiled snapshot contains 25,546 records; profiling results remain
  snapshot-specific because the upstream table changes over time.
- Historical data may be replayed to demonstrate incremental and backfill runs.
- The system is intentionally sized for 25,546 records and does not require
  distributed processing technologies such as Spark or Kafka.

## Roadmap

- [x] Define the initial business problem and architecture.
- [x] Profile the source dataset.
- [x] Detect upstream identifier collisions.
- [x] Redefine the project around append-only source observations.
- [x] Define the source record-hash contract.
- [x] Design ingestion and monitoring tables.
- [x] Run PostgreSQL locally with Docker.
- [x] Implement immutable batch ingestion.
- [x] Implement data-quality checks and reconciliation.
- [x] Implement overlapping incremental extraction.
- [x] Add Airflow orchestration.
- [x] Add CI.
- [x] Complete final portfolio documentation.
