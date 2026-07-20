from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote


def safe_identifier(value: str) -> str:
    """Quote the variable part of a DataHub tuple URN."""
    return quote(value, safe="._-~/")


def dataset_urn(platform: str, name: str, env: str) -> str:
    return (
        f"urn:li:dataset:(urn:li:dataPlatform:{safe_identifier(platform)},"
        f"{safe_identifier(name)},{safe_identifier(env.upper())})"
    )


def container_urn(platform: str, database: str, env: str) -> str:
    # Deterministic key; the same inputs always generate the same container.
    key = safe_identifier(f"{platform}.{env.upper()}.{database}")
    return f"urn:li:container:{key}"


def mysql_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def validate_table_name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"unsafe MySQL table name: {value!r}")
    return value


def parse_byte_size(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([kKmMgG]?[bB]?)?\s*", value)
    if not match:
        raise ValueError(f"invalid byte size: {value!r}")
    amount = int(match.group(1))
    unit = (match.group(2) or "").lower().removesuffix("b")
    multiplier = {"": 1, "k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}[unit]
    size = amount * multiplier
    if size <= 0:
        raise ValueError("byte size must be positive")
    return size
