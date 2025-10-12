from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

from ..models import Sheet, StandardRecord
from ..schema import DEFAULT_VALUES, SHEET_COLUMNS
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


def _apply_defaults(sheet: str, row: Dict[str, object]) -> Dict[str, object]:
    defaults = DEFAULT_VALUES.get(sheet, {})
    for column, value in defaults.items():
        row.setdefault(column, value)
    return row


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
    """Build支出记录，示例：amount=16.28 -> row['消费金额']="16.28"."""
    dt = as_datetime(timestamp)
    if dt is None:
        return None
    dec_amount = to_decimal(amount)
    row: Dict[str, object] = {
        "支出大类": category_main,
        "支出小类": category_sub,
        "账户": account,
        "消费日期": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "消费金额": f"{dec_amount:.2f}",
        "备注": remark,
    }
    if merchant:
        row["商家"] = merchant
    record = StandardRecord(
        sheet=Sheet.EXPENSE,
        row=_apply_defaults("支出", row),
        timestamp=dt,
        amount=dec_amount,
        direction="expense",
        account=account,
        remark=remark,
        source=source,
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
    """Build收入记录，示例：payer='公司报销' -> row['付款方']="公司报销"."""
    dt = as_datetime(timestamp)
    if dt is None:
        return None
    dec_amount = to_decimal(amount)
    row: Dict[str, object] = {
        "收入大类": category,
        "账户": account,
        "收入日期": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "收入金额": f"{dec_amount:.2f}",
        "备注": remark,
    }
    if payer:
        row["付款方"] = payer
    record = StandardRecord(
        sheet=Sheet.INCOME,
        row=_apply_defaults("收入", row),
        timestamp=dt,
        amount=dec_amount,
        direction="income",
        account=account,
        remark=remark,
        source=source,
    )
    record.meta.base_remark = remark
    if payer:
        record.meta.merchant = normalize_text(payer)
    record.meta.matching_key = record.meta.matching_key or normalize_text(payer) or normalize_text(remark)
    return record


def annotate_source(record: StandardRecord, extra: Dict[str, str] | None = None) -> None:
    """Append来源信息到备注，示例：extra={'支付方式': '中信银行信用卡(1129)'}."""
    parts = [f"来源: {record.source}"]
    if record.raw_id:
        parts.append(f"ID: {record.raw_id}")
    if extra:
        for k, v in extra.items():
            if not v:
                continue
            record.meta.source_extras[k] = v
            parts.append(f"{k}: {v}")
    remark = normalize_text(record.row.get("备注"))
    if remark:
        record.row["备注"] = f"{remark}; " + "; ".join(parts)
    else:
        record.row["备注"] = "; ".join(parts)
    record.remark = normalize_text(record.row.get("备注"))


def ensure_column_order(records: Iterable[StandardRecord]) -> None:
    for record in records:
        columns = SHEET_COLUMNS[record.sheet]
        # Insert missing columns as empty string to satisfy DataFrame construction later.
        for column in columns:
            record.row.setdefault(column, "")
        record.row = {column: record.row.get(column, "") for column in columns}
