import json
from pathlib import Path

import pytest

from shopee_quality import hashing
from shopee_quality.hashing import (
    CANONICAL_FIELDS,
    DATETIME_FIELDS,
    DECIMAL_FIELDS,
    EXPECTED_SOURCE_FIELD_COUNT,
    HASH_CONTRACT_VERSION,
    HASH_DOMAIN_MARKER,
    INTEGER_FIELDS,
    STRING_FIELDS,
)


FIXTURE_PATH = (
    Path(__file__).parents[1]
    / "fixtures"
    / "source_row_hash_v1.json"
)


def test_hash_contract_metadata() -> None:
    assert HASH_CONTRACT_VERSION == 1
    assert HASH_DOMAIN_MARKER == "shopee_orders:v1"
    assert EXPECTED_SOURCE_FIELD_COUNT == 84
    assert len(CANONICAL_FIELDS) == EXPECTED_SOURCE_FIELD_COUNT
    assert CANONICAL_FIELDS[0] == "pkId"
    assert CANONICAL_FIELDS[-1] == "is_primary_row"


def test_canonicalize_record_matches_v1_fixture() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    actual = hashing.canonicalize_record(fixture["source_record"])

    assert actual == fixture["expected_canonical_json"]


def test_every_canonical_field_has_exactly_one_type() -> None:
    typed_fields = (
        INTEGER_FIELDS
        | DECIMAL_FIELDS
        | DATETIME_FIELDS
        | STRING_FIELDS
    )

    assert typed_fields == frozenset(CANONICAL_FIELDS)
    assert len(INTEGER_FIELDS) == 7
    assert len(DECIMAL_FIELDS) == 17
    assert len(DATETIME_FIELDS) == 8
    assert len(STRING_FIELDS) == 52

    assert INTEGER_FIELDS.isdisjoint(DECIMAL_FIELDS)
    assert INTEGER_FIELDS.isdisjoint(DATETIME_FIELDS)
    assert DECIMAL_FIELDS.isdisjoint(DATETIME_FIELDS)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("100.00", "100"),
        ("90.50", "90.5"),
        ("1.230", "1.23"),
        ("-0.00", "0"),
        ("-6.00", "-6"),
        ("0.0012300", "0.00123"),
    ],
)
def test_normalize_decimal(
    raw_value: str,
    expected: str,
) -> None:
    assert hashing.normalize_decimal(raw_value) == expected


@pytest.mark.parametrize(
    "raw_value",
    ["NaN", "Infinity", "-Infinity", "not-a-number"],
)
def test_normalize_decimal_rejects_invalid_values(
    raw_value: str,
) -> None:
    with pytest.raises(ValueError):
        hashing.normalize_decimal(raw_value)


def test_normalize_value_preserves_null() -> None:
    assert hashing.normalize_value("update_time", None) is None


def test_normalize_value_normalizes_unicode_to_nfc() -> None:
    decomposed = "Cafe\u0301"

    actual = hashing.normalize_value("item_name", decomposed)

    assert actual == "Café"


@pytest.mark.parametrize(
    "value",
    ["", "  padded text  "],
)
def test_normalize_value_preserves_string_content(
    value: str,
) -> None:
    assert hashing.normalize_value("buyer_remark", value) == value


@pytest.mark.parametrize(
    "value",
    [0, 2, -1],
)
def test_normalize_value_accepts_integers(value: int) -> None:
    assert hashing.normalize_value("quantity", value) == value


@pytest.mark.parametrize(
    "value",
    [True, False, "2", 2.0],
)
def test_normalize_value_rejects_non_integer_types(value) -> None:
    with pytest.raises(TypeError):
        hashing.normalize_value("quantity", value)


def test_normalize_value_delegates_decimal_normalization() -> None:
    assert hashing.normalize_value("original_price", "100.00") == "100"


def test_normalize_value_preserves_datetime2_precision() -> None:
    value = "2026-01-02T03:04:05.1234567"

    assert hashing.normalize_value("create_time", value) == value


@pytest.mark.parametrize(
    "value",
    [
        "2026-01-02T03:04:05.123456",
        "2026-02-30T03:04:05.1234567",
        "2026-01-02T03:04:05.1234567Z",
    ],
)
def test_normalize_value_rejects_invalid_datetime(
    value: str,
) -> None:
    with pytest.raises(ValueError):
        hashing.normalize_value("create_time", value)


def test_normalize_value_rejects_unknown_field() -> None:
    with pytest.raises(KeyError):
        hashing.normalize_value("unknown_field", "value")


def test_compute_source_row_hash_matches_v1_fixture() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    actual = hashing.compute_source_row_hash(
        fixture["source_record"]
    )

    assert actual == fixture["expected_sha256"]
    assert len(actual) == 64


def load_source_record() -> dict:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return fixture["source_record"]


def test_canonicalize_record_rejects_missing_field() -> None:
    source_record = load_source_record()
    del source_record["pkId"]

    with pytest.raises(ValueError, match="missing fields.*pkId"):
        hashing.canonicalize_record(source_record)


def test_canonicalize_record_rejects_unexpected_field() -> None:
    source_record = load_source_record()
    source_record["new_source_column"] = "unexpected"

    with pytest.raises(
        ValueError,
        match="unexpected fields.*new_source_column",
    ):
        hashing.canonicalize_record(source_record)


def test_hash_is_independent_of_dictionary_order() -> None:
    source_record = load_source_record()
    reversed_record = dict(reversed(list(source_record.items())))

    assert hashing.compute_source_row_hash(
        reversed_record
    ) == hashing.compute_source_row_hash(source_record)
