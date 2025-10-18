from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Dict, List, Optional

import pandas as pd

from .schema import DEFAULT_VALUES, SHEET_COLUMNS, SHEET_NAMES


class Sheet(StrEnum):
    EXPENSE = "支出"
    INCOME = "收入"
    TRANSFER = "转账"
    BORROW = "借入借出"
    REPAY = "收款还款"


@dataclass
class RecordMeta:
    """Structured metadata attached to a StandardRecord."""

    base_remark: Optional[str] = None  # 解析阶段保留的原始备注文本
    merchant: Optional[str] = None  # 标准化后的商户/付款方名称
    matching_key: Optional[str] = None  # 供去重/匹配使用的主键，包含商户或备注
    source_extras: Dict[str, str] = field(default_factory=dict)  # 渠道提供的额外字段，如支付方式、状态
    channel: Optional[str] = None  # 渠道标识，例如 "wechat"、"alipay"
    channel_label: Optional[str] = None  # 渠道人类可读名称，例如 "微信支付"
    supplement_only: bool = False  # 标记为仅用于补充信息，不进入最终输出
    duplicate_with: Optional[str] = None  # 指向去重时匹配到的记录 ID 或渠道
    supplemented_from: Optional[str] = None  # 备注补充来源渠道
    accepted: bool = False  # 最终是否被确认导入


@dataclass
class StandardRecord(ABC):
    """Single normalized transaction ready for reconciliation."""

    sheet: Sheet
    timestamp: datetime
    amount: Decimal
    direction: str
    account: str
    remark: str
    source: str
    raw_id: Optional[str] = None
    meta: RecordMeta = field(default_factory=RecordMeta)
    canceled: bool = False
    skipped_reason: Optional[str] = None

    def to_row(self) -> Dict[str, Any]:
        base = self._build_row()
        defaults = DEFAULT_VALUES.get(self.sheet.value, {})
        for column, value in defaults.items():
            base.setdefault(column, value)
        ordered: Dict[str, Any] = {}
        for column in SHEET_COLUMNS[self.sheet.value]:
            ordered[column] = base.get(column, "")
        return ordered

    @abstractmethod
    def _build_row(self) -> Dict[str, Any]:
        """Return a dictionary representing the row contents before defaults/ordering."""
        raise NotImplementedError


@dataclass
class ExpenseRecord(StandardRecord):
    category_main: str = "待分类"
    category_sub: str = "待分类"
    currency: str = "人民币"
    project: str = "日常"
    merchant: Optional[str] = None
    reimburse: str = "非报销"
    member_amount: str = ""
    ledger: str = "日常账本"

    def _build_row(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "支出大类": self.category_main,
            "支出小类": self.category_sub,
            "账户": self.account,
            "消费日期": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "消费金额": f"{self.amount:.2f}",
            "备注": self.remark,
            "币种": self.currency,
            "项目": self.project,
            "报销": self.reimburse,
            "成员金额": self.member_amount,
            "账本": self.ledger,
        }
        if self.merchant:
            row["商家"] = self.merchant
        return row


@dataclass
class IncomeRecord(StandardRecord):
    category: str = "待分类"
    currency: str = "人民币"
    project: str = "日常"
    payer: Optional[str] = None
    member_amount: str = ""
    ledger: str = "日常账本"

    def _build_row(self) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "收入大类": self.category,
            "账户": self.account,
            "收入日期": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "收入金额": f"{self.amount:.2f}",
            "备注": self.remark,
            "币种": self.currency,
            "项目": self.project,
            "成员金额": self.member_amount,
            "账本": self.ledger,
        }
        if self.payer:
            row["付款方"] = self.payer
        return row


@dataclass
class TransferRecord(StandardRecord):
    from_account: str = ""
    to_account: str = ""
    from_currency: str = "人民币"
    to_currency: str = "人民币"
    out_amount: Decimal = Decimal("0")
    in_amount: Decimal = Decimal("0")
    ledger: str = "日常账本"

    def _build_row(self) -> Dict[str, Any]:
        return {
            "转出账户": self.from_account,
            "币种": self.from_currency,
            "转出金额": f"{self.out_amount:.2f}",
            "转入账户": self.to_account,
            "币种.1": self.to_currency,
            "转入金额": f"{self.in_amount:.2f}",
            "转账时间": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "备注": self.remark,
            "账本": self.ledger,
        }


@dataclass
class BorrowRecord(StandardRecord):
    borrow_type: str = "借出"
    loan_account: str = ""
    counterparty_account: str = ""
    ledger: str = "日常账本"

    def _build_row(self) -> Dict[str, Any]:
        return {
            "借贷类型": self.borrow_type,
            "借贷时间": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "借贷账户": self.loan_account,
            "账户": self.counterparty_account,
            "金额": f"{self.amount:.2f}",
            "备注": self.remark,
            "账本": self.ledger,
        }


@dataclass
class RepayRecord(StandardRecord):
    borrow_type: str = "借出"
    loan_account: str = ""
    counterparty_account: str = ""
    interest: str = "0"
    ledger: str = "日常账本"

    def _build_row(self) -> Dict[str, Any]:
        return {
            "借贷类型": self.borrow_type,
            "借贷时间": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "借贷账户": self.loan_account,
            "账户": self.counterparty_account,
            "金额": f"{self.amount:.2f}",
            "利息": self.interest,
            "备注": self.remark,
            "账本": self.ledger,
        }


class SheetBundle:
    """Container maintaining pandas DataFrames for all sheets."""

    def __init__(self) -> None:
        self.frames: Dict[str, pd.DataFrame] = {
            sheet: pd.DataFrame(columns=SHEET_COLUMNS[sheet]) for sheet in SHEET_NAMES
        }  # 示例：self.frames["支出"] -> 空 DataFrame

    def copy(self) -> "SheetBundle":
        clone = SheetBundle()
        clone.frames = {name: frame.copy() for name, frame in self.frames.items()}
        return clone

    def update_from_records(self, records: List[StandardRecord]) -> None:
        by_sheet: Dict[str, List[Dict[str, Any]]] = {sheet: [] for sheet in SHEET_NAMES}
        for record in records:
            if record.canceled or record.skipped_reason:
                continue  # 示例：skipped_reason="duplicate-baseline" 时不写入
            by_sheet[record.sheet.value].append(record.to_row())
        for sheet, rows in by_sheet.items():
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=SHEET_COLUMNS[sheet])
            self.frames[sheet] = pd.concat([self.frames[sheet], df], ignore_index=True)

    def to_dict(self) -> Dict[str, pd.DataFrame]:
        return self.frames
