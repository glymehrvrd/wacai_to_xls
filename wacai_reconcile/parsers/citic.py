from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from .base import annotate_source, create_expense_record, create_income_record
from ..models import StandardRecord
from ..utils import normalize_text, to_decimal


def parse_citic(path: Path) -> List[StandardRecord]:
    if not path.exists():
        raise FileNotFoundError(f"Citic statement not found: {path}")

    df = pd.read_excel(path, header=1)
    df = df.dropna(subset=["交易日期"])

    records: List[StandardRecord] = []
    for _, row in df.iterrows():
        amount = row.get("交易金额")
        description = normalize_text(row.get("交易描述"))
        merchant_name = description.split("－", 1)[-1] if "－" in description else description.split("-", 1)[-1] if "-" in description else description
        merchant_name = normalize_text(merchant_name)
        tail = normalize_text(row.get("卡末四位"))
        account_name = f"中信银行信用卡({tail})" if tail else "中信银行信用卡"
        remark = ""
        if to_decimal(amount) <= 0:
            record = create_income_record(
                amount=abs(to_decimal(amount)),
                timestamp=row.get("交易日期"),
                account=account_name,
                remark=remark,
                payer=description,
                category="退款返款",
            )
            if record:
                record.meta.merchant = merchant_name
        else:
            record = create_expense_record(
                amount=amount,
                timestamp=row.get("交易日期"),
                account=account_name,
                remark=remark,
                merchant=description,
            )
            if record:
                record.meta.merchant = merchant_name
        if record is None:
            continue
        record.meta.matching_key = merchant_name or description
        record.raw_id = f"{row.get('交易日期')}_{description}"
        annotate_source(record, {})
        records.append(record)

    return records
