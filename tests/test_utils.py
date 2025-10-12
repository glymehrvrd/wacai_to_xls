from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from wacai_reconcile.time_utils import as_datetime
from wacai_reconcile.utils import normalize_text, to_decimal


def test_to_decimal_rounds_and_cleans_input() -> None:
    assert to_decimal("16.278") == Decimal("16.28")
    assert to_decimal("   42  ") == Decimal("42.00")
    assert to_decimal("abc") == Decimal("0")
    assert to_decimal(None) == Decimal("0")


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Ａｌｉｐａｙ ", "Alipay"),
        ("  微信支付", "微信支付"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_text_handles_nfkc_and_whitespace(raw: str | None, expected: str) -> None:
    assert normalize_text(raw) == expected


def test_as_datetime_parses_string_with_timezone() -> None:
    dt = as_datetime("2025-10-11 12:25:03")
    assert dt == datetime(2025, 10, 11, 12, 25, 3, tzinfo=ZoneInfo("Asia/Shanghai"))


def test_as_datetime_returns_none_for_empty_input() -> None:
    assert as_datetime("") is None
    assert as_datetime(None) is None
