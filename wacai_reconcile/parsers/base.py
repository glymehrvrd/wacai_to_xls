from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, Optional, Tuple

from ..models import ExpenseRecord, IncomeRecord, Sheet, StandardRecord, TransferRecord
from ..time_utils import as_datetime
from ..utils import normalize_text, to_decimal


CARD_KEYWORDS = ("信用卡", "储蓄卡", "借记卡", "银行卡")
CARD_KEYWORDS_LOWER = ("visa", "mastercard", "amex", "american express", "discover", "jcb", "unionpay")


def is_wallet_funded(payment: str, wallet_keywords: Tuple[str, ...]) -> bool:
    """Return True when the支付方式 looks like wallet/余额，而非直接刷卡."""
    normalized = normalize_text(payment)
    if any(keyword in normalized for keyword in wallet_keywords):
        return True
    lowered = normalized.lower()
    if any(keyword in normalized for keyword in CARD_KEYWORDS):
        return False
    if any(keyword in lowered for keyword in CARD_KEYWORDS_LOWER):
        return False
    return True


@dataclass
class ParseConfig:
    source: str
    account_name: str


def create_expense_record(
    *,
    amount: object,
    timestamp: object,
    account: str,
    remark: str,
    source: str,
    merchant: Optional[str] = None,
    category_main: str = "待分类",
    category_sub: str = "待分类",
) -> Optional[StandardRecord]:
    dt = as_datetime(timestamp)
    if dt is None:
        return None
    dec_amount = to_decimal(amount)
    record = ExpenseRecord(
        timestamp=dt,
        amount=dec_amount,
        direction="expense",
        account=account,
        remark=remark,
        source=source,
        category_main=category_main,
        category_sub=category_sub,
        merchant=merchant,
    )
    record.meta.base_remark = remark
    if merchant:
        record.meta.merchant = normalize_text(merchant)
    record.meta.matching_key = record.meta.matching_key or normalize_text(merchant) or normalize_text(remark)
    return record


def create_income_record(
    *,
    amount: object,
    timestamp: object,
    account: str,
    remark: str,
    source: str,
    payer: Optional[str] = None,
    category: str = "待分类",
) -> Optional[StandardRecord]:
    dt = as_datetime(timestamp)
    if dt is None:
        return None
    dec_amount = to_decimal(amount)
    record = IncomeRecord(
        timestamp=dt,
        amount=dec_amount,
        direction="income",
        account=account,
        remark=remark,
        source=source,
        category=category,
        payer=payer,
    )
    record.meta.base_remark = remark
    if payer:
        record.meta.merchant = normalize_text(payer)
    record.meta.matching_key = record.meta.matching_key or normalize_text(payer) or normalize_text(remark)
    return record


def create_transfer_record(
    *,
    amount: object,
    timestamp: object,
    account: str,
    remark: str,
    source: str,
    from_account: str,
    to_account: str,
    from_currency: str = "人民币",
    to_currency: str = "人民币",
) -> Optional[StandardRecord]:
    dt = as_datetime(timestamp)
    if dt is None:
        return None
    dec_amount = to_decimal(amount)
    record = TransferRecord(
        timestamp=dt,
        amount=dec_amount,
        direction="transfer",
        account=account,
        remark=remark,
        source=source,
        from_account=from_account,
        to_account=to_account,
        from_currency=from_currency,
        to_currency=to_currency,
        out_amount=dec_amount,
        in_amount=dec_amount,
    )
    record.meta.base_remark = remark
    record.meta.matching_key = record.meta.matching_key or normalize_text(remark)
    return record


def annotate_source(record: StandardRecord, extra: Dict[str, str] | None = None) -> None:
    """Append来源信息到备注，示例：extra={'支付方式': '中信银行信用卡(1129)'}."""
    parts = [f"来源: {record.source}"]
    if record.raw_id:
        parts.append(f"ID: {record.raw_id}")
    if extra:
        for key, value in extra.items():
            if not value:
                continue
            record.meta.source_extras[key] = value
            parts.append(f"{key}: {value}")
    suffix = "; ".join(parts)
    remark = record.remark.strip()
    record.remark = f"{remark}; {suffix}" if remark else suffix


def ensure_column_order(records: Iterable[StandardRecord]) -> None:
    """Backwards-compat shim for legacy callers; no-op now that to_row handles ordering."""
    for _ in records:
        continue
