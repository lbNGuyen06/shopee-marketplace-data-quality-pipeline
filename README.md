# Shopee Order Analytics Pipeline

A reproducible batch data pipeline that extracts Shopee order records from a
read-only SQL Server source, validates and loads them idempotently into
PostgreSQL, and produces analytics-ready tables for sales, marketplace fees,
cancellations, and fulfillment performance.

> Project status: architecture and environment setup.

## Business Problem

Marketplace order data is stored in a wide operational table containing order
statuses, product information, lifecycle timestamps, shipping details, and
platform fees. Querying this source directly makes reporting difficult to
reproduce and increases the risk of inconsistent business definitions.

This project prepares reliable datasets for three hypothetical users:

- Operations managers monitoring fulfillment performance.
- Finance analysts reconciling marketplace fees and settlement amounts.
- Sales managers tracking order value and product performance.

## Architecture

See [the architecture document](docs/architecture.md).

## Data Model

The preliminary grain of the main fact table is one product or product variant
within a Shopee order. This assumption will be validated during source
profiling before the final schema is implemented.

Planned layers:

- `raw`: source-shaped records with ingestion metadata.
- `staging`: cleaned, typed, deduplicated, and privacy-safe records.
- `core`: reusable facts and dimensions.
- `marts`: aggregated tables for business reporting.

## Reliability and Data Quality

The pipeline will implement:

- Incremental extraction using source update timestamps.
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

Setup instructions will be added after the Docker environment is implemented.

## Challenges and Solutions

This section will document real technical problems discovered during
implementation. It will not contain invented challenges.

## Privacy

The source schema includes personally identifiable information such as recipient
names, phone numbers, usernames, and delivery addresses. Raw data and
credentials are excluded from version control. Published examples and tests
will use synthetic records only.

## Limitations

- The source is a limited sample rather than a live Shopee API.
- Historical data may be replayed to demonstrate incremental and backfill runs.
- The system is intentionally sized for 5,000 records and does not require
  distributed processing technologies such as Spark or Kafka.

## Roadmap

- [x] Define the business problem and initial architecture.
- [ ] Profile the source dataset.
- [ ] Implement raw incremental extraction.
- [ ] Implement PostgreSQL loading.
- [ ] Build staging, core, and mart models.
- [ ] Add automated data-quality tests.
- [ ] Containerize the runtime.
- [ ] Orchestrate the pipeline with Airflow.
- [ ] Add CI and final project documentation.
