/*
Aggregate-only source profiling for the read-only SQL Server source.

The result sets intentionally contain counts, ranges, and grouped monitoring
metrics only. Do not add row-level selects: the source contains PII and masked
operational identifiers.
*/

USE [xomdata_dataset];
GO

SET NOCOUNT ON;

-- Snapshot volume, timestamp coverage, and watermark suitability.
SELECT
    COUNT_BIG(*) AS total_rows,
    COUNT_BIG(DISTINCT [pkId]) AS distinct_pk_id_count,
    COUNT_BIG(DISTINCT [order_sn]) AS distinct_order_sn_count,
    SUM(CASE WHEN [update_time] IS NULL THEN 1 ELSE 0 END) AS null_update_time_count,
    COUNT_BIG(DISTINCT [update_time]) AS distinct_update_time_count,
    SUM(CASE WHEN [synced_at] IS NULL THEN 1 ELSE 0 END) AS null_synced_at_count,
    COUNT_BIG(DISTINCT [synced_at]) AS distinct_synced_at_count,
    MIN([create_time]) AS min_create_time,
    MAX([create_time]) AS max_create_time,
    MIN([synced_at]) AS min_synced_at,
    MAX([synced_at]) AS max_synced_at,
    SUM(CASE WHEN [synced_at] < [create_time] THEN 1 ELSE 0 END)
        AS synced_before_create_count
FROM [vietnam_ecommerce].[shopee_orders];

-- Required-field completeness. Results are counts, never source values.
SELECT
    COUNT_BIG(*) AS total_rows,
    SUM(CASE WHEN [pkId] IS NULL THEN 1 ELSE 0 END) AS null_pk_id_count,
    SUM(CASE WHEN [order_sn] IS NULL THEN 1 ELSE 0 END) AS null_order_sn_count,
    SUM(CASE WHEN [order_status] IS NULL THEN 1 ELSE 0 END) AS null_order_status_count,
    SUM(CASE WHEN [create_time] IS NULL THEN 1 ELSE 0 END) AS null_create_time_count,
    SUM(CASE WHEN [quantity] IS NULL THEN 1 ELSE 0 END) AS null_quantity_count,
    SUM(CASE WHEN [item_sku] IS NULL THEN 1 ELSE 0 END) AS null_item_sku_count,
    SUM(CASE WHEN [synced_at] IS NULL THEN 1 ELSE 0 END) AS null_synced_at_count
FROM [vietnam_ecommerce].[shopee_orders];

-- Numeric ranges and anomaly counts. Financial fields are not summed because
-- masked order identity cannot prevent repeated order-level amounts.
SELECT
    MIN([quantity]) AS min_quantity,
    MAX([quantity]) AS max_quantity,
    SUM(CASE WHEN [quantity] <= 0 THEN 1 ELSE 0 END) AS nonpositive_quantity_count,
    MIN([original_price]) AS min_original_price,
    MAX([original_price]) AS max_original_price,
    SUM(CASE WHEN [original_price] < 0 THEN 1 ELSE 0 END) AS negative_original_price_count,
    MIN([buyer_total_amount]) AS min_buyer_total_amount,
    MAX([buyer_total_amount]) AS max_buyer_total_amount,
    SUM(CASE WHEN [buyer_total_amount] < 0 THEN 1 ELSE 0 END) AS negative_buyer_total_count,
    MIN([commission_fee]) AS min_commission_fee,
    MAX([commission_fee]) AS max_commission_fee,
    SUM(CASE WHEN [commission_fee] < 0 THEN 1 ELSE 0 END) AS negative_commission_fee_count,
    MIN([escrow_amount]) AS min_escrow_amount,
    MAX([escrow_amount]) AS max_escrow_amount,
    SUM(CASE WHEN [escrow_amount] < 0 THEN 1 ELSE 0 END) AS negative_escrow_amount_count
FROM [vietnam_ecommerce].[shopee_orders];

-- Row-level status distribution for monitoring (not an order count).
SELECT
    COALESCE([order_status], '<NULL>') AS order_status,
    COUNT_BIG(*) AS row_count
FROM [vietnam_ecommerce].[shopee_orders]
GROUP BY [order_status]
ORDER BY row_count DESC, order_status;

-- Lifecycle ordering checks. Nullable timestamps are evaluated only when both
-- sides of a comparison are present.
SELECT
    SUM(CASE WHEN [pay_time] IS NOT NULL
                  AND [create_time] IS NOT NULL
                  AND [pay_time] < [create_time] THEN 1 ELSE 0 END)
        AS pay_before_create_count,
    SUM(CASE WHEN [shipped_time] IS NOT NULL
                  AND [pay_time] IS NOT NULL
                  AND [shipped_time] < [pay_time] THEN 1 ELSE 0 END)
        AS shipped_before_pay_count,
    SUM(CASE WHEN [completed_time] IS NOT NULL
                  AND [shipped_time] IS NOT NULL
                  AND [completed_time] < [shipped_time] THEN 1 ELSE 0 END)
        AS completed_before_shipped_count,
    SUM(CASE WHEN [update_time] IS NOT NULL
                  AND [create_time] IS NOT NULL
                  AND [update_time] < [create_time] THEN 1 ELSE 0 END)
        AS update_before_create_count
FROM [vietnam_ecommerce].[shopee_orders];

-- Collision summaries expose no identifier values. A duplicated masked ID is
-- a collision candidate, not proof of an exact duplicate or business entity.
WITH pk_id_groups AS (
    SELECT [pkId], COUNT_BIG(*) AS rows_per_value
    FROM [vietnam_ecommerce].[shopee_orders]
    WHERE [pkId] IS NOT NULL
    GROUP BY [pkId]
),
order_sn_groups AS (
    SELECT [order_sn], COUNT_BIG(*) AS rows_per_value
    FROM [vietnam_ecommerce].[shopee_orders]
    WHERE [order_sn] IS NOT NULL
    GROUP BY [order_sn]
)
SELECT
    'pkId' AS identifier_name,
    SUM(CASE WHEN rows_per_value > 1 THEN 1 ELSE 0 END) AS duplicated_value_count,
    COALESCE(SUM(CASE WHEN rows_per_value > 1 THEN rows_per_value ELSE 0 END), 0)
        AS rows_in_duplicated_groups,
    COALESCE(MAX(rows_per_value), 0) AS largest_group_size
FROM pk_id_groups
UNION ALL
SELECT
    'order_sn' AS identifier_name,
    SUM(CASE WHEN rows_per_value > 1 THEN 1 ELSE 0 END) AS duplicated_value_count,
    COALESCE(SUM(CASE WHEN rows_per_value > 1 THEN rows_per_value ELSE 0 END), 0)
        AS rows_in_duplicated_groups,
    COALESCE(MAX(rows_per_value), 0) AS largest_group_size
FROM order_sn_groups;

-- Distribution of observations per watermark value, used to size and test the
-- synced_at overlap strategy without exposing individual timestamps.
WITH synced_at_groups AS (
    SELECT [synced_at], COUNT_BIG(*) AS rows_per_value
    FROM [vietnam_ecommerce].[shopee_orders]
    WHERE [synced_at] IS NOT NULL
    GROUP BY [synced_at]
)
SELECT
    COUNT_BIG(*) AS distinct_synced_at_count,
    MIN(rows_per_value) AS min_rows_per_synced_at,
    MAX(rows_per_value) AS max_rows_per_synced_at,
    AVG(CAST(rows_per_value AS decimal(19, 4))) AS avg_rows_per_synced_at
FROM synced_at_groups;
