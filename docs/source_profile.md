# Shopee Marketplace Source Profile

## 1. Objective

This profile evaluates whether the Xóm Data Shopee source is suitable for
incremental ingestion and downstream analytics. It examines volume, schema,
identifier cardinality, duplicates, nulls, numeric ranges, lifecycle
timestamps, financial measures, and privacy-sensitive fields before the
destination model is designed.

The results changed the project scope. Because source identifiers are masked
and no longer preserve reliable order-level uniqueness, the project is designed
as a marketplace data-quality and observability pipeline rather than an
order-level sales analytics pipeline.

## 2. Profiling Scope

- Source system: Xóm Data SQL Server
- Source schema: `vietnam_ecommerce`
- Source table: `shopee_orders`
- Access mode: read-only
- Published schema width: 84 columns
- Latest profiling date: 2026-09-10
- Latest observed row count: 25,546
- Profiling SQL: [`../sql/profiling/source_profile.sql`](../sql/profiling/source_profile.sql)

The source is not static. An earlier run contained 5,000 rows, while the latest
run contained 25,546 rows. Counts from different snapshots are kept separate
and must not be interpreted as measurements from the same dataset state.

## 3. Snapshot Evidence

### Initial 5,000-row snapshot

| Measure | Result |
| --- | ---: |
| Total rows | 5,000 |
| Distinct `pkId` values | 4,986 |
| Displayed distinct `order_sn` values | 31 |
| Null `update_time` values | 14 |
| Distinct `update_time` values | 4,595 |
| Null `synced_at` values | 0 |
| Distinct `synced_at` values | 586 |
| Earliest `create_time` | 2025-12-25 21:11:27 |
| Latest `create_time` | 2026-08-10 23:27:46 |
| Earliest `synced_at` | 2026-07-14 18:16:27.580 |
| Latest `synced_at` | 2026-08-14 05:00:27.161 |

Fourteen `pkId` groups occurred twice. Their rows could have different order,
item, model, status, or timestamp attributes. They are therefore identifier
collisions, not evidence that the entire records are exact duplicates.

The low displayed cardinality of `order_sn` is inconsistent with interpreting
the rows as only 31 real marketplace orders. It is evidence that source masking
reduces identifier cardinality, not that the source has exactly 31 orders.

### Latest 25,546-row snapshot

| Measure | Result |
| --- | ---: |
| Total rows | 25,546 |
| Null `update_time` values | 288 |
| Distinct `update_time` values | 19,485 |
| Null `synced_at` values | 0 |
| Distinct `synced_at` values | 1,043 |
| Rows with `synced_at < create_time` | 0 |

The increase in rows confirms that the upstream table can change over time.
Identifier, status, null, and range statistics must therefore be measured per
run and stored as monitoring results rather than treated as constants.

## 4. Row Grain

The business grain cannot be proven from the accessible source. The table
contains order, item, model, financial, shipping, and lifecycle attributes on
the same row, which resembles an order-item extract. However, masked `order_sn`,
`pkId`, `item_id`, and `model_id` values do not preserve the cardinality needed
to identify a unique business order or order item.

The pipeline therefore adopts these technical grains:

| Layer | Grain |
| --- | --- |
| Raw batch file | One immutable extraction batch |
| PostgreSQL raw | One source observation received in one batch |
| Staging | One distinct normalized record fingerprint |
| Monitoring | One quality result for one rule and pipeline run |
| Monitoring mart | One metric per date and monitoring dimension |

No published table will claim a grain of one unique Shopee order or customer.

## 5. Candidate-Key Analysis

### `pkId`

`pkId` was non-null in the initial sample but was not unique: 5,000 rows
produced only 4,986 distinct values. Duplicate groups contained materially
different attributes. It must not be used as a primary key or trusted business
identifier.

### `order_sn`

`order_sn` was non-null, but its displayed cardinality was heavily reduced by
masking. It must not be used to count unique orders, join order-level facts, or
deduplicate observations.

### Composite business fields

Combinations of `order_sn`, `item_id`, `model_id`, `item_sku`, and `model_sku`
remain unsuitable as authoritative keys because several components are masked.
Adding `synced_at` can make a combination technically unique, but that
identifies an observation time rather than a stable business entity.

### Selected technical keys

- `ingestion_id`: generated physical primary key for each raw observation.
- `batch_id`: identifies the extraction run that produced the observation.
- `source_row_hash`: SHA-256 fingerprint of normalized record content, used to
  detect exact re-ingestion.

`source_row_hash` is a content identifier, not an order identifier. Its final
column list and null-serialization rules must be defined in the data contract.

## 6. Duplicate Classification

The pipeline distinguishes three concepts:

1. **Identifier collision:** the same masked identifier is associated with
   materially different records.
2. **Exact content duplicate:** normalized source fields produce the same
   `source_row_hash`.
3. **Re-ingestion duplicate:** the same content is extracted again because an
   incremental overlap window is used.

Identifier collisions are monitored and preserved in raw. Exact re-ingestion
is removed from staging without deleting the raw observation or batch lineage.

## 7. Null and Validity Findings

In the initial snapshot, `pkId`, `order_sn`, `order_status`, `create_time`,
`quantity`, `item_sku`, and `synced_at` had no nulls. `update_time` had 14
nulls. In the latest snapshot, `update_time` had 288 nulls while `synced_at`
remained complete.

Observed numeric findings from the initial snapshot:

| Check | Result |
| --- | ---: |
| Minimum quantity | 1 |
| Maximum quantity | 9 |
| Quantity less than or equal to zero | 0 rows |
| Minimum original price | 1.81 |
| Maximum original price | 9,990,000.00 |
| Negative original price | 0 rows |
| Minimum buyer total amount | 0.00 |
| Maximum buyer total amount | 7,032,784.00 |
| Minimum commission fee | 0.00 |
| Maximum commission fee | 959,016.00 |
| Minimum escrow amount | -6,813.00 |
| Maximum escrow amount | 5,889,943.00 |

Negative `escrow_amount` values are anomalies, not automatic rejects. They may
represent refunds, adjustments, chargebacks, or settlement corrections; the
source does not provide enough semantics to select one interpretation.

## 8. Status and Lifecycle Findings

The initial snapshot contained these statuses:

| Status | Row count |
| --- | ---: |
| `COMPLETED` | 4,094 |
| `CANCELLED` | 867 |
| `TO_CONFIRM_RECEIVE` | 23 |
| `TO_RETURN` | 8 |
| `SHIPPED` | 5 |
| `PROCESSED` | 3 |

This is a row-level source-health distribution, not an order count, because
`order_sn` does not preserve unique-order identity.

Lifecycle checks on the initial snapshot found:

- 0 rows with `pay_time < create_time`.
- 1,926 rows with `shipped_time < pay_time`.
- 0 rows with `completed_time < shipped_time`.
- 0 rows with `update_time < create_time` when `update_time` was present.

The 1,926 `shipped_time < pay_time` rows are monitored as a semantic anomaly,
not rejected. COD behavior or the source definition of `pay_time` may explain
the ordering, and the available data does not prove they are invalid.

## 9. Financial-Measure Findings

Fields such as `total_order_value`, `buyer_total_amount`, and `escrow_amount`
appear to be order-level measures repeated on item-shaped rows. Because the
order identifier is masked, the pipeline cannot safely select one row per real
order or calculate authoritative order-level totals.

Consequently:

- These fields may be checked for nulls, ranges, and distribution changes.
- They must not be summed across source rows as marketplace revenue.
- They must not be deduplicated using masked `order_sn` alone.
- The project will not publish GMV, unique-order, or customer-level metrics.

## 10. Incremental Extraction

`synced_at` is selected as the extraction watermark because it is complete in
both snapshots and no latest-snapshot rows were synchronized before their
creation time. `update_time` is insufficient by itself because it has nulls.

`synced_at` is not unique: the latest snapshot has only 1,043 distinct values
for 25,546 rows. Extraction therefore uses an overlap window:

1. Start before the last committed `synced_at` by a defined overlap duration.
2. Extract all rows in the overlap and new range.
3. Create normalized source-row fingerprints.
4. Reconcile extracted, accepted, duplicate, and rejected counts.
5. Advance the watermark only after the batch succeeds.

This favors completeness over avoiding repeated reads; deterministic
fingerprinting handles exact re-ingestion.

## 11. Privacy Classification

Sensitive or potentially identifying fields include:

- `recipient_name`, `phone`, `full_address`
- `buyer_username`, `buyer_user_id`
- state, city, district, town, and zipcode
- operational identifiers such as `order_sn`, `pkId`, `item_id`, and `model_id`

Controls:

- Raw records and credentials are excluded from Git.
- Source values are not copied into public documentation or fixtures.
- Logs contain batch IDs, counts, rule names, and technical errors, not PII.
- Tests use synthetic records.
- Public monitoring outputs contain aggregated metrics only.
- Masked values are not assumed to be safe for unrestricted publication.

## 12. Data-Quality Rules

### Reject or fail the batch

- Required source columns are missing or have incompatible data types.
- `synced_at` is null or cannot be parsed.
- `create_time`, `quantity`, or another required technical field cannot be
  parsed.
- Batch reconciliation does not satisfy the documented accounting equation.

### Monitor without automatic rejection

- Masked identifier collisions.
- Null `update_time` values.
- New or disappearing status values.
- Negative `escrow_amount` values.
- `shipped_time < pay_time`.
- Sudden changes in volume, null rate, freshness, or numeric ranges.

This distinction prevents uncertain business semantics from silently deleting
otherwise usable observations.

## 13. Design Decisions

- The project is a **Shopee Marketplace Data Quality Pipeline**, not an
  authoritative order analytics system.
- Raw observations are append-only and retain batch lineage.
- No masked source identifier is used as a database primary key.
- A generated `ingestion_id` provides physical identity.
- A deterministic `source_row_hash` detects exact content duplicates.
- `synced_at` with an overlap window drives incremental extraction.
- The watermark advances only after successful reconciliation.
- Identifier collisions and uncertain anomalies are monitored, not discarded.
- No unique-order, unique-customer, GMV, or order-level financial metric is
  published from this source.

## 14. Limitations and Reproducibility

The source changes over time, so numerical findings are snapshot-specific. The
initial 5,000-row figures remain historical evidence and must not be combined
with the latest 25,546-row counts.

Future profiling runs should record `profiled_at`, total rows, maximum
`synced_at`, query version, and all aggregate outputs. No conclusion about the
real identity of an order, item, customer, or identifier may be made unless an
unmasked source contract becomes available.
