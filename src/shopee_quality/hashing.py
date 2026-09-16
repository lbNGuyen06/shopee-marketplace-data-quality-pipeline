import hashlib
import json
import re
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation

HASH_CONTRACT_VERSION = 1
HASH_DOMAIN_MARKER = "shopee_orders:v1"
EXPECTED_SOURCE_FIELD_COUNT = 84

CANONICAL_FIELDS = (
    'pkId',
    'user_id',
    'shop_id',
    'order_sn',
    'order_status',
    'create_time',
    'pay_time',
    'shipped_time',
    'completed_time',
    'cancel_reason',
    'buyer_remark',
    'shipping_carrier',
    'payment_method',
    'estimated_shipping_date',
    'buyer_username',
    'recipient_name',
    'phone',
    'full_address',
    'state',
    'city',
    'district',
    'country',
    'item_name',
    'item_sku',
    'model_name',
    'model_sku',
    'quantity',
    'original_price',
    'model_discounted_price',
    'item_weight',
    'total_order_value',
    'buyer_total_amount',
    'seller_discount',
    'shopee_discount',
    'voucher_from_seller',
    'voucher_from_shopee',
    'commission_fee',
    'service_fee',
    'transaction_fee',
    'escrow_amount',
    'actual_shipping_fee',
    'shop_name',
    'connection_id',
    'region',
    'synced_at',
    'raw_payload',
    'currency',
    'order_status_raw',
    'cancel_time',
    'fulfillment_flag',
    'cod',
    'buyer_user_id',
    'ship_by_date',
    'note',
    'shipping_method',
    'package_number',
    'recipient_town',
    'returned_quantity',
    'total_weight',
    'estimated_shipping_fee',
    'return_shipping_fee',
    'item_id',
    'model_id',
    'zipcode',
    'update_time',
    'buyer_cancel_reason',
    'cancel_by',
    'product_location_id',
    'promotion_type',
    'promotion_id',
    'active_qty',
    'cancel_requested_qty',
    'cancelled_qty',
    'return_requested_qty',
    'wholesale',
    'add_on_deal',
    'main_item',
    'add_on_deal_id',
    'can_full_cancel_order',
    'can_partial_cancel_order',
    'buyer_preference_for_partial_cancellation',
    'booking_sn',
    'advance_package',
    'is_primary_row',
)
INTEGER_FIELDS = frozenset(
    {
        "quantity",
        "returned_quantity",
        "active_qty",
        "cancel_requested_qty",
        "cancelled_qty",
        "return_requested_qty",
        "buyer_preference_for_partial_cancellation",
    }
)

DECIMAL_FIELDS = frozenset(
    {
        "original_price",
        "model_discounted_price",
        "item_weight",
        "total_order_value",
        "buyer_total_amount",
        "seller_discount",
        "shopee_discount",
        "voucher_from_seller",
        "voucher_from_shopee",
        "commission_fee",
        "service_fee",
        "transaction_fee",
        "escrow_amount",
        "actual_shipping_fee",
        "total_weight",
        "estimated_shipping_fee",
        "return_shipping_fee",
    }
)

DATETIME_FIELDS = frozenset(
    {
        "create_time",
        "pay_time",
        "shipped_time",
        "completed_time",
        "synced_at",
        "cancel_time",
        "ship_by_date",
        "update_time",
    }
)

STRING_FIELDS = (
    frozenset(CANONICAL_FIELDS)
    - INTEGER_FIELDS
    - DECIMAL_FIELDS
    - DATETIME_FIELDS
)
DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}$"
)


def normalize_decimal(value: str) -> str:
    try:
        decimal_value = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(
            f"Invalid decimal representation: {value!r}"
        ) from exc

    if not decimal_value.is_finite():
        raise ValueError(
            f"Non-finite decimal is not supported: {value!r}"
        )

    if decimal_value == 0:
        return "0"

    return format(decimal_value.normalize(), "f")


def normalize_value(field_name: str, value):
    if field_name not in CANONICAL_FIELDS:
        raise KeyError(f"Unknown source field: {field_name}")

    if value is None:
        return None

    if field_name in DECIMAL_FIELDS:
        if not isinstance(value, str):
            raise TypeError(
                f"{field_name} must be represented as a decimal string"
            )
        return normalize_decimal(value)

    if field_name in DATETIME_FIELDS:
        if not isinstance(value, str) or not DATETIME_PATTERN.fullmatch(value):
            raise ValueError(
                f"{field_name} must use datetime2(7) canonical format"
            )

        try:
            datetime.fromisoformat(value[:-1])
        except ValueError as exc:
            raise ValueError(
                f"{field_name} contains an invalid datetime: {value!r}"
            ) from exc

        return value

    if field_name in INTEGER_FIELDS:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(
                f"{field_name} must be represented as an integer"
            )
        return value

    if field_name in STRING_FIELDS:
        if not isinstance(value, str):
            raise TypeError(
                f"{field_name} must be represented as a string"
            )
        return unicodedata.normalize("NFC", value)

    raise RuntimeError(f"Field type is not configured: {field_name}")


def canonicalize_record(source_record: dict) -> str:
    missing_fields = [
        field_name
        for field_name in CANONICAL_FIELDS
        if field_name not in source_record
    ]

    if missing_fields:
        raise ValueError(
            f"Source record is missing fields: {missing_fields}"
        )

    unexpected_fields = sorted(
        set(source_record) - set(CANONICAL_FIELDS)
    )

    if unexpected_fields:
        raise ValueError(
            f"Source record contains unexpected fields: {unexpected_fields}"
        )

    canonical_values = [HASH_DOMAIN_MARKER]

    canonical_values.extend(
        normalize_value(field_name, source_record[field_name])
        for field_name in CANONICAL_FIELDS
    )

    return json.dumps(
        canonical_values,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def compute_source_row_hash(source_record: dict) -> str:
    canonical_json = canonicalize_record(source_record)

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()
