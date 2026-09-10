# Shopee Order Analytics Pipeline Architecture

## System context

The pipeline extracts Shopee order records from the read-only Xóm Data SQL
Server source. It preserves a raw copy, validates the records, loads them
idempotently into PostgreSQL, and produces analytics-ready data marts.

## Data flow

```mermaid
flowchart LR
    A[Xóm Data SQL Server] -->|synced_at overlap window| B[Python Extractor]
    B --> C[Immutable Raw Batch]
    C --> D[Schema Validation]
    D --> E[Record Fingerprinting]
    E -->|Valid observations| F[PostgreSQL Raw]
    D -->|Invalid observations| G[Rejected Records]
    F --> H[Standardized Records]
    H --> I[Quality and Collision Checks]
    I --> J[Monitoring Marts]
    K[Apache Airflow] -. Orchestrates .-> B
    K -. Runs checks .-> I
    K -. Commits watermark after success .-> J
```

## Data layers

| Layer          | Grain                                        | Purpose                                                   |
| -------------- | -------------------------------------------- | --------------------------------------------------------- |
| Raw files      | One extracted batch                          | Preserve the source response for replay                   |
| PostgreSQL raw | One observed source record per batch         | Store immutable observations and ingestion metadata       |
| Staging        | One distinct record fingerprint              | Standardize types and remove exact re-ingestion           |
| Monitoring     | One test result or pipeline event            | Record quality, reconciliation, freshness, and collisions |
| Marts          | One metric per date and monitoring dimension | Support operational source-health reporting               |

## Reliability requirements

- Reprocessing the same batch must not create duplicate records.
- Invalid records must be isolated instead of silently discarded.
- Every run must record its batch identifier and ingestion timestamp.
- Source and destination row counts must be reconcilable.
- Logs must not expose names, phone numbers, addresses, or credentials.

## Current constraints

- The source contains a sample of 5,000 Shopee records.
- The dataset may not receive continuous updates.
- Historical dates will be replayed as batches to demonstrate backfills.
- The project is designed for learning and portfolio evaluation, not production use.

## System invariants

- Masked source identifiers are never treated as unique keys.
- Raw observations are immutable.
- Reprocessing a batch does not duplicate standardized records.
- Every loaded or rejected record belongs to a known batch.
- Extracted row count equals loaded, duplicate, and rejected row counts.
- The watermark advances only after successful reconciliation.
- Record fingerprints identify content, not business entities.
- No unique-order or unique-customer metric is published.
