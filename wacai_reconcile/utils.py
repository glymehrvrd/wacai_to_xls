from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import unicodedata


def to_decimal(value: object) -> Decimal:
    """Normalize numeric inputs to 2 decimal places, 示例："16.278" -> Decimal("16.28")."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return Decimal("0")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        cleaned = "".join(ch for ch in str(value) if ch.isdigit() or ch in {".", "-"})
        if not cleaned:
            return Decimal("0")
        return Decimal(cleaned).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def normalize_text(value: str | None) -> str:
    """Strip and NFKC-normalize text, 示例："Ａｌｉｐａｙ " -> "Alipay"."""
    if not value:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    return text.strip()
