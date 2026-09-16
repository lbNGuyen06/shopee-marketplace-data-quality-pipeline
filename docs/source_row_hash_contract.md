# Source Row Hash Contract

## Purpose

`source_row_hash` is a deterministic SHA-256 fingerprint of normalized
source-record content. It detects exact content duplicates and overlapping
re-ingestion. It is not a Shopee order identifier.

## Included fields

All published source fields from
`vietnam_ecommerce.shopee_orders` are included in their documented canonical
order.

## Excluded fields

Pipeline-generated metadata is excluded:

- `ingestion_id`
- `batch_id`
- `source_row_number`
- `observed_at`
- extraction timestamps
- file names and runtime metadata

## Canonicalization Rules

Before hashing, source values are normalized according to their logical data
types:

- Columns are processed in one fixed, explicitly documented order.
- `NULL` is represented as the JSON value `null`.
- Strings are normalized to Unicode NFC and otherwise preserved. Leading and
  trailing whitespace is not removed.
- Empty strings remain empty strings and are not converted to `NULL`.
- Integers use base-10 representation without leading zeroes.
- Decimal values are represented as strings to preserve precision. Scientific
  notation is not used, insignificant trailing zeroes are removed, and
  negative zero is normalized to `0`.
- Boolean values use the JSON values `true` and `false`.
- Datetimes use `YYYY-MM-DDTHH:MM:SS.fffffff`. Naive SQL Server timestamps are
  not assigned a timezone during hashing.
- Floating-point `NaN` and infinity values are rejected rather than hashed.
- Pipeline-generated metadata is never included.

## Source Type Normalization Map

| SQL Server type | Canonical type | Canonical representation                                                 |
| --------------- | -------------- | ------------------------------------------------------------------------ |
| `nvarchar`      | string         | Unicode NFC JSON string; whitespace preserved                            |
| `int`           | integer        | Base-10 JSON number                                                      |
| `decimal`       | decimal        | JSON string without scientific notation or insignificant trailing zeroes |
| `datetime2(7)`  | datetime       | JSON string formatted as `YYYY-MM-DDTHH:MM:SS.fffffff`                   |

Columns are normalized according to their declared SQL Server type, not their
name. For example, `estimated_shipping_date` is `nvarchar` and is therefore
normalized as a string, while `create_time` is `datetime2(7)` and is normalized
as a datetime.

Flag-like `nvarchar` columns such as `cod`, `wholesale`, and `is_primary_row`
remain strings. They are not converted to booleans unless the source contract
later defines an authoritative value mapping.

The `raw_payload` column remains a string. Its contents are not parsed or
re-serialized as JSON before hashing.

## Serialization and Hashing

1. Normalize every included field according to its logical data type.
2. Create a JSON array whose first element is the contract domain marker,
   followed by the normalized source values in canonical column order.
3. Serialize the array as UTF-8 JSON without indentation or insignificant
   whitespace.
4. Compute SHA-256 over the serialized UTF-8 bytes.
5. Store the digest as 64 lowercase hexadecimal characters.

A JSON array is used instead of delimiter-joined text so that `NULL`, empty
strings, embedded delimiters, and field boundaries remain unambiguous.

## Canonical Column Order

Contract version: `1`

Columns are serialized in ascending SQL Server `ORDINAL_POSITION`. They must
not be reordered alphabetically or according to Python dictionary iteration.

| Position | Column                                      | Source type     | Nullable |
| -------: | ------------------------------------------- | --------------- | :------: |
|        1 | `pkId`                                      | `nvarchar(50)`  |    No    |
|        2 | `user_id`                                   | `nvarchar(30)`  |    No    |
|        3 | `shop_id`                                   | `nvarchar(10)`  |    No    |
|        4 | `order_sn`                                  | `nvarchar(20)`  |    No    |
|        5 | `order_status`                              | `nvarchar(20)`  |    No    |
|        6 | `create_time`                               | `datetime2(7)`  |    No    |
|        7 | `pay_time`                                  | `datetime2(7)`  |   Yes    |
|        8 | `shipped_time`                              | `datetime2(7)`  |   Yes    |
|        9 | `completed_time`                            | `datetime2(7)`  |   Yes    |
|       10 | `cancel_reason`                             | `nvarchar(100)` |   Yes    |
|       11 | `buyer_remark`                              | `nvarchar(50)`  |   Yes    |
|       12 | `shipping_carrier`                          | `nvarchar(100)` |   Yes    |
|       13 | `payment_method`                            | `nvarchar(30)`  |    No    |
|       14 | `estimated_shipping_date`                   | `nvarchar(50)`  |   Yes    |
|       15 | `buyer_username`                            | `nvarchar(30)`  |   Yes    |
|       16 | `recipient_name`                            | `nvarchar(10)`  |    No    |
|       17 | `phone`                                     | `nvarchar(10)`  |    No    |
|       18 | `full_address`                              | `nvarchar(10)`  |    No    |
|       19 | `state`                                     | `nvarchar(10)`  |    No    |
|       20 | `city`                                      | `nvarchar(10)`  |    No    |
|       21 | `district`                                  | `nvarchar(10)`  |    No    |
|       22 | `country`                                   | `nvarchar(10)`  |    No    |
|       23 | `item_name`                                 | `nvarchar(150)` |    No    |
|       24 | `item_sku`                                  | `nvarchar(20)`  |    No    |
|       25 | `model_name`                                | `nvarchar(30)`  |   Yes    |
|       26 | `model_sku`                                 | `nvarchar(50)`  |    No    |
|       27 | `quantity`                                  | `int`           |    No    |
|       28 | `original_price`                            | `decimal(18,2)` |    No    |
|       29 | `model_discounted_price`                    | `decimal(18,2)` |   Yes    |
|       30 | `item_weight`                               | `decimal(18,3)` |    No    |
|       31 | `total_order_value`                         | `decimal(18,2)` |   Yes    |
|       32 | `buyer_total_amount`                        | `decimal(18,2)` |    No    |
|       33 | `seller_discount`                           | `decimal(18,2)` |    No    |
|       34 | `shopee_discount`                           | `decimal(18,2)` |    No    |
|       35 | `voucher_from_seller`                       | `decimal(18,2)` |    No    |
|       36 | `voucher_from_shopee`                       | `decimal(18,2)` |    No    |
|       37 | `commission_fee`                            | `decimal(18,2)` |    No    |
|       38 | `service_fee`                               | `decimal(18,2)` |    No    |
|       39 | `transaction_fee`                           | `decimal(18,2)` |    No    |
|       40 | `escrow_amount`                             | `decimal(18,2)` |    No    |
|       41 | `actual_shipping_fee`                       | `decimal(18,2)` |    No    |
|       42 | `shop_name`                                 | `nvarchar(30)`  |    No    |
|       43 | `connection_id`                             | `nvarchar(30)`  |    No    |
|       44 | `region`                                    | `nvarchar(10)`  |   Yes    |
|       45 | `synced_at`                                 | `datetime2(7)`  |    No    |
|       46 | `raw_payload`                               | `nvarchar(50)`  |   Yes    |
|       47 | `currency`                                  | `nvarchar(10)`  |    No    |
|       48 | `order_status_raw`                          | `nvarchar(20)`  |    No    |
|       49 | `cancel_time`                               | `datetime2(7)`  |   Yes    |
|       50 | `fulfillment_flag`                          | `nvarchar(30)`  |    No    |
|       51 | `cod`                                       | `nvarchar(5)`   |   Yes    |
|       52 | `buyer_user_id`                             | `nvarchar(20)`  |    No    |
|       53 | `ship_by_date`                              | `datetime2(7)`  |   Yes    |
|       54 | `note`                                      | `nvarchar(50)`  |   Yes    |
|       55 | `shipping_method`                           | `nvarchar(50)`  |   Yes    |
|       56 | `package_number`                            | `nvarchar(20)`  |   Yes    |
|       57 | `recipient_town`                            | `nvarchar(10)`  |   Yes    |
|       58 | `returned_quantity`                         | `int`           |   Yes    |
|       59 | `total_weight`                              | `decimal(18,3)` |   Yes    |
|       60 | `estimated_shipping_fee`                    | `decimal(18,2)` |   Yes    |
|       61 | `return_shipping_fee`                       | `decimal(18,2)` |   Yes    |
|       62 | `item_id`                                   | `nvarchar(20)`  |   Yes    |
|       63 | `model_id`                                  | `nvarchar(20)`  |   Yes    |
|       64 | `zipcode`                                   | `nvarchar(10)`  |   Yes    |
|       65 | `update_time`                               | `datetime2(7)`  |   Yes    |
|       66 | `buyer_cancel_reason`                       | `nvarchar(100)` |   Yes    |
|       67 | `cancel_by`                                 | `nvarchar(10)`  |   Yes    |
|       68 | `product_location_id`                       | `nvarchar(10)`  |   Yes    |
|       69 | `promotion_type`                            | `nvarchar(30)`  |   Yes    |
|       70 | `promotion_id`                              | `nvarchar(20)`  |   Yes    |
|       71 | `active_qty`                                | `int`           |   Yes    |
|       72 | `cancel_requested_qty`                      | `int`           |   Yes    |
|       73 | `cancelled_qty`                             | `int`           |   Yes    |
|       74 | `return_requested_qty`                      | `int`           |   Yes    |
|       75 | `wholesale`                                 | `nvarchar(5)`   |   Yes    |
|       76 | `add_on_deal`                               | `nvarchar(5)`   |   Yes    |
|       77 | `main_item`                                 | `nvarchar(5)`   |   Yes    |
|       78 | `add_on_deal_id`                            | `nvarchar(20)`  |   Yes    |
|       79 | `can_full_cancel_order`                     | `nvarchar(5)`   |   Yes    |
|       80 | `can_partial_cancel_order`                  | `nvarchar(50)`  |   Yes    |
|       81 | `buyer_preference_for_partial_cancellation` | `int`           |   Yes    |
|       82 | `booking_sn`                                | `nvarchar(50)`  |   Yes    |
|       83 | `advance_package`                           | `nvarchar(50)`  |   Yes    |
|       84 | `is_primary_row`                            | `nvarchar(5)`   |    No    |

## Schema Evolution

Before extracting source rows, the pipeline compares the live SQL Server
schema with the canonical column contract.

Extraction may proceed only when all of the following match contract version
`1`:

- column count;
- column names;
- ordinal positions;
- SQL Server data types;
- type length, precision, and scale;
- nullability.

A new, removed, renamed, reordered, or type-changed column is treated as schema
drift. The pipeline must:

1. capture the observed schema snapshot;
2. record a failed schema quality result;
3. mark the pipeline run as failed;
4. stop before hashing or loading source rows;
5. leave the committed watermark unchanged.

The pipeline must not automatically add a new source column to the hash
contract. A contract change requires review, a new contract version, updated
test vectors, and an explicit migration.

## Contract Versioning

Hash contract version `1` uses the domain marker:

`shopee_orders:v1`

The domain marker is the first element of the canonical JSON array, followed by
the 84 normalized source values in canonical column order. The serialized array
therefore contains 85 elements.

The marker is hashing metadata, not a source field. It prevents hashes produced
by different tables or contract versions from sharing the same hash domain.

A new contract version must use a new marker, for example
`shopee_orders:v2`. Existing hashes are never recalculated or overwritten.

## Test Vector Representation

Test-vector `source_record` objects use JSON as a portable transport format.

- SQL Server `nvarchar` values are represented as JSON strings.
- SQL Server `int` values are represented as JSON numbers.
- SQL Server `decimal` values are represented as JSON strings so precision and
  source scale are not lost through binary floating point.
- SQL Server `datetime2(7)` values are represented as fixed-width JSON strings
  using `YYYY-MM-DDTHH:MM:SS.fffffff`.
- SQL `NULL` values are represented as JSON `null`.

The normalizer interprets each fixture value according to the declared source
type in the canonical column contract, rather than inferring its type from JSON
alone.
A `datetime2(7)` value must retain all seven fractional-second digits during
extraction. Converting it only through a Python `datetime` is insufficient
because Python `datetime` stores microseconds with six fractional digits.
The extractor must preserve the seventh digit through an exact textual or
driver-supported representation before hashing.
