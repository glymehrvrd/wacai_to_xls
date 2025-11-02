from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple

import pandas as pd

from .schema import (
    AMOUNT_COLUMNS,
    DATE_COLUMNS,
    LOCK_REMARKS,
    SHEET_NAMES,
)
from .time_utils import as_datetime
from .utils import normalize_text, to_decimal


def normalize_account_name(account: str) -> str:
    """标准化账户名称，去掉尾号（括号及其内容）。

    示例：
    - "招商银行信用卡(1129)" -> "招商银行信用卡"
    - "中信银行信用卡(5678)" -> "中信银行信用卡"
    """
    if not account:
        return account
    # 查找第一个左括号的位置
    paren_pos = account.find("(")
    if paren_pos > 0:
        return account[:paren_pos]
    return account


def build_account_locks(frames: Dict[str, pd.DataFrame]) -> Dict[str, datetime]:
    """Capture the latest adjustment timestamp per account to freeze older entries."""
    locks: Dict[str, datetime] = {}
    for sheet_name, frame in frames.items():
        if frame.empty:
            continue
        date_col = DATE_COLUMNS.get(sheet_name)
        if not date_col or date_col not in frame.columns:
            continue
        for _, row in frame.iterrows():
            date_value = row.get(date_col)
            if date_value is None:
                continue
            dt = as_datetime(date_value)
            if dt is None:
                continue
            remark = normalize_text(row.get("备注"))
            lock_trigger = False
            # 满足备注的锁定条件即冻结账户历史交易。
            if remark in LOCK_REMARKS:
                lock_trigger = True
            if not lock_trigger:
                continue
            account = normalize_text(row.get("账户"))
            if not account:
                continue
            # 标准化账户名称，去掉尾号
            account = normalize_account_name(account)
            current = locks.get(account)
            # 同一账户保留最近的锁定时间，确保不倒退。
            if current is None or dt > current:
                locks[account] = dt
    return locks


class BaselineIndex:
    """Index supporting duplicate checks against baseline workbook."""

    def __init__(self, frames: Dict[str, pd.DataFrame], amount_tolerance: float, date_tolerance: timedelta):
        # Store tolerance in Decimal to keep comparisons consistent with downstream money math.
        self.amount_tolerance = Decimal(str(amount_tolerance))
        self.date_tolerance = date_tolerance
        self.entries: Dict[str, Dict[str, List[Tuple[datetime, Decimal, str]]]] = {
            sheet: defaultdict(list) for sheet in SHEET_NAMES
        }
        for sheet, frame in frames.items():
            if frame.empty:
                continue
            date_col = DATE_COLUMNS.get(sheet)
            amount_cols = AMOUNT_COLUMNS.get(sheet, [])
            if not date_col or not amount_cols:
                continue
            for _, row in frame.iterrows():
                date_value = row.get(date_col)
                if date_value is None:
                    continue
                dt = as_datetime(date_value)
                if dt is None:
                    continue
                remarks = normalize_text(row.get("备注"))
                for amount_col in amount_cols:
                    amount = to_decimal(row.get(amount_col))
                    # Prefer an explicit account name; fall back to placeholder to keep buckets separate.
                    account = normalize_text(row.get("账户")) or normalize_text(row.get("转出账户"))
                    key_account = account or "__UNKNOWN__"
                    self.entries[sheet][key_account].append((dt, amount, remarks))

        # Sort entries for potentially faster searches
        for sheet_entries in self.entries.values():
            for values in sheet_entries.values():
                values.sort(key=lambda item: item[0])

    def exists(self, sheet: str, account: str, amount: Decimal, timestamp: datetime, remark: str = "") -> bool:
        account_key = account or "__UNKNOWN__"
        candidates = self.entries.get(sheet, {}).get(account_key, [])
        if not candidates:
            return False
        for dt, base_amount, base_remark in candidates:
            # Time window check first to quickly discard irrelevant history.
            if abs((dt - timestamp).total_seconds()) > self.date_tolerance.total_seconds():
                continue
            if abs(base_amount - amount) > self.amount_tolerance:
                continue
            if remark and base_remark and remark != base_remark:
                continue
            return True
        return False
