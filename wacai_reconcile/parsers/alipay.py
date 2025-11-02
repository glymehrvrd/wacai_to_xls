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
        if direction not in {"支出", "收入", "不计收支"}:
            continue

        # 金额为0的交易不记录
        amount = row.get("金额")
        try:
            amount_float = float(amount) if amount is not None and not pd.isna(amount) else 0.0
            if amount_float == 0.0:
                continue
        except (ValueError, TypeError):
            continue
        # 备注只保留第一个字段（商品说明）
        product = row.get("商品说明")
        if product is None or pd.isna(product):
            remark = ""
        else:
            remark = normalize_text(str(product)) or ""
        merchant = normalize_text(row.get("交易对方"))
        payment = normalize_text(row.get("收/付款方式"))
        order_no = normalize_text(row.get("交易订单号"))
        merchant_order = normalize_text(row.get("商家订单号"))

        wallet_payment = is_wallet_funded(payment, WALLET_KEYWORDS)  # 示例：payment="花呗" -> True

        # 账户名称处理：花呗单独记为"花呗"，其他支付宝内部账户记为"支付宝"
        payment_normalized = payment or ""
        if "花呗" in payment_normalized:
            account_name = "花呗"
        elif is_wallet_funded(payment, WALLET_KEYWORDS):
            account_name = "支付宝"
        else:
            account_name = payment_normalized or "支付宝"

        if direction == "支出":
            record = create_expense_record(
                amount=row.get("金额"),
                timestamp=row.get("交易时间"),
                account=account_name,
                remark=remark,
                merchant=merchant,
            )
        elif direction == "收入":
            record = create_income_record(
                amount=row.get("金额"),
                timestamp=row.get("交易时间"),
                account=account_name,
                remark=remark,
                payer=merchant,
                category="待分类",
            )
        else:  # 不计收支
            is_refund = direction == "不计收支" and (normalize_text(row.get("交易分类")) == "退款" or "退款" in status)
            if not is_refund:
                continue
            record = create_income_record(
                amount=row.get("金额"),
                timestamp=row.get("交易时间"),
                account=account_name,
                remark=remark,
                payer=merchant,
                category="退款返款",
            )
        if record is None:
            continue
        record.raw_id = order_no or merchant_order
        annotate_source(record, {"支付方式": payment, "状态": status})
        if merchant:
            record.meta.matching_key = merchant
        if not wallet_payment:
            record.skipped_reason = "non-wallet-payment"
            record.meta.supplement_only = True
        records.append(record)

    return records
