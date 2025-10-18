from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from .base import annotate_source, create_expense_record, create_income_record, create_transfer_record, is_wallet_funded
from ..models import StandardRecord
from ..utils import normalize_text


WALLET_KEYWORDS = ("零钱", "零钱通", "小金罐", "亲属卡")


def parse_wechat(path: Path) -> List[StandardRecord]:
    if not path.exists():
        raise FileNotFoundError(f"WeChat statement not found: {path}")

    df = pd.read_excel(path, skiprows=16)
    df = df.dropna(subset=["交易时间"])

    records: List[StandardRecord] = []
    for _, row in df.iterrows():
        pay_flag = normalize_text(row.get("收/支"))
        remark_raw = normalize_text(row.get("备注"))
        remark_items = []
        if remark_raw and remark_raw != "/":
            remark_items.append(remark_raw)
        status = normalize_text(row.get("当前状态"))
        if status:
            remark_items.append(f"状态: {status}")
        product = normalize_text(row.get("商品"))
        if product:
            remark_items.append(f"商品: {product}")
        remark = "; ".join(remark_items)
        merchant = normalize_text(row.get("交易对方"))
        trade_id = normalize_text(row.get("交易单号"))
        pay_method = normalize_text(row.get("支付方式"))

        wallet_payment = is_wallet_funded(pay_method, WALLET_KEYWORDS)  # 示例：支付方式"零钱"返回 True

        if pay_flag == "支出":
            record = create_expense_record(
                amount=row.get("金额(元)"),
                timestamp=row.get("交易时间"),
                account="微信",
                remark=remark,
                merchant=merchant,
                source="微信支付",
            )
        elif pay_flag == "收入":
            record = create_income_record(
                amount=row.get("金额(元)"),
                timestamp=row.get("交易时间"),
                account="微信",
                remark=remark,
                payer=merchant,
                source="微信支付",
                category="待分类",
            )
        elif pay_flag == "/":
            transaction_type = normalize_text(row.get("交易类型"))
            from_account = normalize_text(row.get("支付方式")) or "微信"
            to_account = ""
            if "-来自" in transaction_type:
                to_part, from_part = transaction_type.split("-来自", 1)
                to_account = to_part.replace("转入", "").replace("「", "").replace("」", "").strip() or "微信"
            elif "-到" in transaction_type:
                to_part, from_part = transaction_type.split("-到", 1)
                from_account = to_part.replace("转出", "").replace("「", "").replace("」", "").strip() or "微信"
                to_account = from_part.replace("「", "").replace("」", "").strip() or "零钱"
            account_name = to_account or from_account or "微信"
            record = create_transfer_record(
                amount=row.get("金额(元)"),
                timestamp=row.get("交易时间"),
                account=account_name,
                remark=remark,
                source="微信支付",
                from_account=from_account or "",
                to_account=to_account or "",
            )
        else:
            continue

        if record is None:
            continue
        record.raw_id = trade_id
        extras = {"支付方式": pay_method}
        if status:
            extras["状态"] = status
        annotate_source(record, extras)
        if merchant:
            record.meta.matching_key = normalize_text(merchant)
        if pay_flag != "/" and not wallet_payment:
            record.skipped_reason = "non-wallet-payment"
            record.meta.supplement_only = True
        records.append(record)

    return records
