from datetime import datetime

import pytest

from hms_export.common import (
    container_urn,
    dataset_urn,
    mysql_json_value,
    parse_byte_size,
    safe_identifier,
    validate_table_name,
)


@pytest.mark.parametrize(
    ("platform", "name", "env", "expected"),
    [
        ("hive", "hive.orders", "uat", "urn:li:dataset:(urn:li:dataPlatform:hive,hive.orders,UAT)"),
        ("hive", "中文.订单", "prod", "urn:li:dataset:(urn:li:dataPlatform:hive,%E4%B8%AD%E6%96%87.%E8%AE%A2%E5%8D%95,PROD)"),
    ],
)
def test_dataset_urn(platform: str, name: str, env: str, expected: str) -> None:
    assert dataset_urn(platform, name, env) == expected


def test_urn_quotes_tuple_delimiters() -> None:
    assert safe_identifier("db,a(b)") == "db%2Ca%28b%29"


def test_container_urn_is_deterministic() -> None:
    assert container_urn("hive", "sales", "uat") == "urn:li:container:hive.UAT.sales"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None), (b"abc", "abc"), (b"\xff", "�"),
        (datetime(2026, 1, 2, 3, 4, 5), "2026-01-02T03:04:05"),
        (12, 12), (True, True),
    ],
)
def test_mysql_json_values(value: object, expected: object) -> None:
    assert mysql_json_value(value) == expected


@pytest.mark.parametrize("name", ["TBLS", "COLUMNS_V2", "table123"])
def test_table_name_validation_accepts_identifiers(name: str) -> None:
    assert validate_table_name(name) == name


@pytest.mark.parametrize("name", ["", "A-B", "TBLS; DROP TABLE TBLS", "x` y"])
def test_table_name_validation_rejects_unsafe_names(name: str) -> None:
    with pytest.raises(ValueError):
        validate_table_name(name)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("20k", 20 * 1024), ("20KB", 20 * 1024), ("20M", 20 * 1024 ** 2), ("1G", 1024 ** 3), ("99", 99)],
)
def test_parse_byte_size(value: str, expected: int) -> None:
    assert parse_byte_size(value) == expected


@pytest.mark.parametrize("value", ["", "abc", "20T", "-1M", "0"])
def test_parse_byte_size_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        parse_byte_size(value)
