from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from .base import annotate_source, create_expense_record, create_income_record, create_transfer_record, is_wallet_funded
from ..models import StandardRecord
from ..utils import normalize_text


WALLET_KEYWORDS = ("零钱", "零钱通", "小金罐", "亲属卡")

# 无意义的备注值，这些备注应该被忽略
MEANINGLESS_REMARKS = ("none", "\\", "/", "-", "_", "无", "无备注")


def is_meaningless_remark(remark: str) -> bool:
    """检查备注是否无意义，如果是则返回True。

    只有明确的无意义值（如 "none", "\", "/"）才会返回True。
    空字符串不视为无意义（可能是正常的空值）。
    """
    if not remark:
        return False  # 空字符串不视为无意义
    normalized = remark.lower() if remark else ""
    if not normalized:
        return False  # 标准化后为空也不视为无意义
    return normalized in MEANINGLESS_REMARKS


def parse_wechat(path: Path) -> List[StandardRecord]:
    if not path.exists():
        raise FileNotFoundError(f"WeChat statement not found: {path}")

    df = pd.read_excel(path, skiprows=16)
    df = df.dropna(subset=["交易时间"])

    records: List[StandardRecord] = []
    for _, row in df.iterrows():
        pay_flag = normalize_text(row.get("收/支"))
        status = normalize_text(row.get("当前状态"))
        # 备注只保留商品字段
        product = row.get("商品")
        if product is None or pd.isna(product):
            remark = ""
        else:
            remark = normalize_text(str(product)) or ""

        # 如果备注无意义，不记录备注（设为空字符串）
        if is_meaningless_remark(remark):
            remark = ""

        merchant = normalize_text(row.get("交易对方"))
        trade_id = normalize_text(row.get("交易单号"))
        pay_method = normalize_text(row.get("支付方式"))

        wallet_payment = is_wallet_funded(pay_method, WALLET_KEYWORDS)  # 示例：支付方式"零钱"返回 True

        # 使用 wallet_payment 判断是否是微信内部账户，统一映射为"微信"
        account_name = "微信" if wallet_payment else (pay_method or "微信")

        if pay_flag == "支出":
            record = create_expense_record(
                amount=row.get("金额(元)"),
                timestamp=row.get("交易时间"),
                account=account_name,
                remark=remark,
                merchant=merchant,
            )
        elif pay_flag == "收入":
            record = create_income_record(
                amount=row.get("金额(元)"),
                timestamp=row.get("交易时间"),
                account="微信",
                remark=remark,
                payer=merchant,
                category="待分类",
            )
        elif pay_flag == "/":
            transaction_type = normalize_text(row.get("交易类型"))
            from_account = account_name or "微信"
            to_account = ""
            if "-来自" in transaction_type:
                to_part, from_part = transaction_type.split("-来自", 1)
                to_account_raw = to_part.replace("转入", "").replace("「", "").replace("」", "").strip() or "微信"
                from_account_raw = from_part.replace("「", "").replace("」", "").strip() or from_account or "零钱"
                # 使用 is_wallet_funded 判断是否是微信内部账户
                to_account = "微信" if is_wallet_funded(to_account_raw, WALLET_KEYWORDS) else (to_account_raw or "微信")
                from_account = (
                    "微信" if is_wallet_funded(from_account_raw, WALLET_KEYWORDS) else (from_account_raw or "微信")
                )
            elif "零钱充值" == transaction_type:
                to_account = "微信"
            if from_account == to_account:
                continue
            record = create_transfer_record(
                amount=row.get("金额(元)"),
                timestamp=row.get("交易时间"),
                account=from_account or to_account or "微信",
                remark=remark,
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
            record.meta.matching_key = merchant
        if pay_flag != "/" and not wallet_payment:
            record.skipped_reason = "non-wallet-payment"
            record.meta.supplement_only = True
        records.append(record)

    return records
