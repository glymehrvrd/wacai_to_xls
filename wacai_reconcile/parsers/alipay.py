from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from .base import annotate_source, create_expense_record, create_income_record, is_wallet_funded
from ..models import StandardRecord
from ..utils import normalize_text


ALIPAY_COLUMNS = [
    "交易时间",
    "交易分类",
    "交易对方",
    "对方账号",
    "商品说明",
    "收/支",
    "金额",
    "收/付款方式",
    "交易状态",
    "交易订单号",
    "商家订单号",
    "备注",
    "附加信息",
]

WALLET_KEYWORDS = ("余额", "余额宝", "花呗", "余利宝")


def parse_alipay(path: Path) -> List[StandardRecord]:
    if not path.exists():
        raise FileNotFoundError(f"Alipay statement not found: {path}")

    df = pd.read_csv(path, skiprows=25, encoding="gbk", names=ALIPAY_COLUMNS)
    df = df.dropna(subset=["交易时间"])

    records: List[StandardRecord] = []
    for _, row in df.iterrows():
        direction = normalize_text(row.get("收/支"))
        status = normalize_text(row.get("交易状态"))
        if direction not in {"支出", "收入"}:
            continue
        remark_items = []
        product = normalize_text(row.get("商品说明"))
        if product:
            remark_items.append(product)
        note_raw = row.get("备注")
        note = "" if pd.isna(note_raw) else normalize_text(note_raw)
        if note:
            remark_items.append(note)
        remark = "; ".join(remark_items)
        merchant = normalize_text(row.get("交易对方"))
        payment = normalize_text(row.get("收/付款方式"))
        order_no = normalize_text(row.get("交易订单号"))
        merchant_order = normalize_text(row.get("商家订单号"))

        wallet_payment = is_wallet_funded(payment, WALLET_KEYWORDS)  # 示例：payment="花呗" -> True

        if direction == "支出":
            record = create_expense_record(
                amount=row.get("金额"),
                timestamp=row.get("交易时间"),
                account="支付宝",
                remark=remark,
                merchant=merchant,
                source="支付宝",
            )
        else:
            record = create_income_record(
                amount=row.get("金额"),
                timestamp=row.get("交易时间"),
                account="支付宝",
                remark=remark,
                payer=merchant,
                source="支付宝",
                category="待分类",
            )
        if record is None:
            continue
        record.raw_id = order_no or merchant_order
        annotate_source(record, {"支付方式": payment, "状态": status})
        if merchant:
            record.meta.matching_key = normalize_text(merchant)
        if not wallet_payment:
            record.skipped_reason = "non-wallet-payment"
            record.meta.supplement_only = True
        records.append(record)

    return records
