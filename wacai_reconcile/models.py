from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Dict, List, Optional

import pandas as pd

from .schema import SHEET_COLUMNS, SHEET_NAMES


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
class StandardRecord:
    """Single normalized transaction ready for reconciliation."""

    sheet: Sheet  # 枚举化 Sheet，确保值限定在模板定义范围内
    row: Dict[str, Any]  # 与 Excel 模板列名一致的一行数据
    timestamp: datetime  # 交易发生时间，使用上海时区 tz-aware datetime
    amount: Decimal  # 交易金额，已保留两位小数
    direction: str  # 资金方向，通常为 "expense" 或 "income"
    account: str  # 记账账户名称，例如 "微信"、"中信银行信用卡(1129)"
    remark: str  # 标准化备注，常包含来源或原始渠道附加信息
    source: str  # 原始渠道名称，例如 "微信支付"
    raw_id: Optional[str] = None  # 渠道侧唯一标识，如交易单号/账单号
    meta: RecordMeta = field(default_factory=RecordMeta)  # 额外上下文信息，以结构化字段维护
    canceled: bool = False  # 是否被退款配对抵消，不再写入最终输出
    skipped_reason: Optional[str] = None  # 若跳过导入，记录原因，例如 "non-wallet-payment"

    def to_row(self) -> Dict[str, Any]:
        """Return a shallow copy so downstream修改不会影响缓存."""
        return dict(self.row)


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
            by_sheet[record.sheet].append(record.to_row())
        for sheet, rows in by_sheet.items():
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=SHEET_COLUMNS[sheet])
            self.frames[sheet] = pd.concat([self.frames[sheet], df], ignore_index=True)

    def to_dict(self) -> Dict[str, pd.DataFrame]:
        return self.frames
