from __future__ import annotations

from email import policy
from email.parser import BytesParser
import re
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup, Tag

from decimal import Decimal

from .base import annotate_source, create_expense_record, create_income_record
from ..models import ExpenseRecord, IncomeRecord, StandardRecord
from ..utils import normalize_text, to_decimal


def _extract_html_content(html_content: str, css_selector: str) -> Tag:
    soup = BeautifulSoup(html_content, "html.parser")
    selected_element = soup.select_one(css_selector)
    if not selected_element:
        raise ValueError(f"未找到选择器 '{css_selector}' 对应的元素")
    return selected_element


def _extract_transactions(tag: Tag) -> List[dict]:
    transactions = []
    for tr in tag.find_all("tr", recursive=False):
        cells = tr.find_all("div")
        if len(cells) != 7:
            continue
        transactions.append(
            {
                "交易日": cells[0].get_text(strip=True),
                "记账日": cells[1].get_text(strip=True),
                "交易摘要": cells[2].get_text(strip=True),
                "人民币金额": cells[3].get_text(strip=True).removeprefix("¥\xa0"),
                "卡号末四位": cells[4].get_text(strip=True),
                "交易地": cells[5].get_text(strip=True),
                "交易地金额": cells[6].get_text(strip=True),
            }
        )
    return transactions


def _extract_cycle(html: str) -> Optional[Tuple[int, int, int, int]]:
    """Grab the billing cycle so mmdd-style日期能补全年份。"""
    match = re.search(r"(\d{4})/(\d{2})/(\d{2})-(\d{4})/(\d{2})/(\d{2})", html)
    if not match:
        return None
    y1, m1, _ = match.group(1, 2, 3)
    y2, m2, _ = match.group(4, 5, 6)
    return int(y1), int(m1), int(y2), int(m2)


def _resolve_date(value: str, cycle: Optional[Tuple[int, int, int, int]]) -> Optional[str]:
    # 报文里日期可能只有 MMDD，需要结合账单周期推断年份。
    value = normalize_text(value)
    if not value:
        return None
    value = value.replace("/", "")
    if len(value) == 8:  # already yyyyMMdd
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    if len(value) == 4 and cycle:
        start_year, start_month, end_year, end_month = cycle
        month = int(value[:2])
        day = int(value[2:])
        year = start_year if month >= start_month else end_year
        try:
            return date(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return None
    if len(value) == 5 and cycle:  # format like M/DD
        parts = value.split("/")
        if len(parts) == 2:
            month = int(parts[0])
            day = int(parts[1])
            start_year, start_month, end_year, _ = cycle
            year = start_year if month >= start_month else end_year
            try:
                return date(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                return None
    return None


def _merge_cmb_silver_rebate_records(records: List[StandardRecord]) -> List[StandardRecord]:
    """合并所有"银联Pay境内返现"和"银联Pay境内返现调回"的记录，只输出一条记录。

    合并策略：
    - 将所有"银联Pay境内返现"和"银联Pay境内返现调回"的记录合并成一条
    - 金额为所有相关记录的代数和（返现相加，调回相减）
    - 如果合并后金额为0，则取消所有相关记录
    - 如果合并后金额为正数，创建一个支出记录
    - 如果合并后金额为负数，创建一个收入记录（金额取绝对值）
    - 新记录使用时间戳最早的记录的时间戳和账户
    """
    # 查找需要合并的记录
    rebate_records: List[StandardRecord] = []
    adjust_records: List[StandardRecord] = []

    for record in records:
        # 获取描述信息：支出记录使用merchant，收入记录使用payer
        if isinstance(record, ExpenseRecord):
            description = record.merchant or ""
        elif isinstance(record, IncomeRecord):
            description = record.payer or ""
        else:
            description = ""

        # 实际描述可能是 "银联Pay境内返现-xxx" 或 "银联Pay境内返现调回-xxx" 格式
        if description.startswith("银联Pay境内返现") and not description.startswith("银联Pay境内返现调回"):
            rebate_records.append(record)
        elif description.startswith("银联Pay境内返现调回"):
            adjust_records.append(record)

    # 如果没有相关记录，不需要合并
    if not rebate_records and not adjust_records:
        return records

    # 合并所有相关记录：计算总金额
    total_rebate = Decimal("0")
    total_adjust = Decimal("0")
    all_records = rebate_records + adjust_records
    earliest_timestamp = None
    first_account = None

    # 计算返现总额
    for rebate in rebate_records:
        total_rebate += rebate.amount
        if earliest_timestamp is None or rebate.timestamp < earliest_timestamp:
            earliest_timestamp = rebate.timestamp
            first_account = rebate.account

    # 计算调回总额（调回记录如果是收入记录，说明原始金额是负数）
    for adjust in adjust_records:
        if adjust.direction == "income":
            # 调回是收入记录，说明原始金额是负数，合并时减去
            total_adjust += adjust.amount
        else:
            # 如果调回也是支出记录，直接用加法（但这种情况不应该出现）
            total_adjust += adjust.amount
        if earliest_timestamp is None or adjust.timestamp < earliest_timestamp:
            earliest_timestamp = adjust.timestamp
            first_account = adjust.account

    # 计算合并后的金额
    merged_amount = total_rebate - total_adjust

    # 取消所有原始记录
    for record in all_records:
        record.canceled = True
        record.skipped_reason = "merged-with-rebate"

    # 如果合并后金额为0，不创建新记录
    if abs(merged_amount) < Decimal("0.01"):
        result: List[StandardRecord] = []
        for record in records:
            if record.skipped_reason != "merged-with-rebate":
                result.append(record)
        return result

    # 根据合并后的金额创建新记录
    if earliest_timestamp is None or first_account is None:
        # 如果没有找到时间戳或账户，不应该发生，但为了安全起见返回原记录
        return records

    if merged_amount > 0:
        # 正数创建支出记录
        merged_record = create_expense_record(
            amount=merged_amount,
            timestamp=earliest_timestamp,
            account=first_account,
            remark="",
            merchant="银联Pay境内返现",
        )
    else:
        # 负数创建收入记录（金额取绝对值）
        merged_record = create_income_record(
            amount=abs(merged_amount),
            timestamp=earliest_timestamp,
            account=first_account,
            remark="",
            payer="银联Pay境内返现调回",
            category="退款返款",
        )

    if merged_record is None:
        # 如果创建失败，返回原记录
        return records

    # 构建结果列表：包含新创建的记录和其他未合并的记录
    result: List[StandardRecord] = []
    for record in records:
        if record.skipped_reason != "merged-with-rebate":
            result.append(record)
    result.append(merged_record)

    return result


def parse_cmb(path: Path) -> List[StandardRecord]:
    if not path.exists():
        raise FileNotFoundError(f"CMB statement not found: {path}")

    with path.open("rb") as fp:
        msg = BytesParser(policy=policy.default).parse(fp)

    html_body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue
                html_body = payload.decode(part.get_content_charset() or "utf-8")
                break
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            html_body = payload.decode(msg.get_content_charset() or "utf-8")

    if not html_body:
        raise ValueError("未找到招商银行邮件的HTML正文")

    cycle = _extract_cycle(html_body)
    table = _extract_html_content(html_body, "#loopBand2 > table > tbody")
    transactions = _extract_transactions(table)

    records: List[StandardRecord] = []
    for tx in transactions:
        description = normalize_text(tx.get("交易摘要"))
        merchant_name = (
            description.split("－", 1)[-1]
            if "－" in description
            else description.split("-", 1)[-1] if "-" in description else description
        )
        merchant_name = normalize_text(merchant_name)
        tail = normalize_text(tx.get("卡号末四位"))
        account = f"招商银行信用卡({tail})" if tail else "招商银行信用卡"
        amount = to_decimal(tx.get("人民币金额"))
        timestamp = _resolve_date(tx.get("交易日") or tx.get("记账日"), cycle)
        remark = ""
        if amount <= 0:
            # 招行账单里负值代表退款/还款，转成收入侧方便后续抵消。
            record = create_income_record(
                amount=abs(amount),
                timestamp=timestamp,
                account=account,
                remark=remark,
                payer=description,
                category="退款返款",
            )
            if record is None:
                continue
            record.meta.merchant = merchant_name
            record.raw_id = f"{tx.get('交易日')}_{description}"
            annotate_source(record, {"卡末四位": tail})
            records.append(record)
            continue
        record = create_expense_record(
            amount=amount,
            timestamp=timestamp,
            account=account,
            remark=remark,
            merchant=description,
        )
        if record is None:
            continue
        record.meta.merchant = merchant_name
        foreign = normalize_text(tx.get("交易地金额"))
        record.raw_id = f"{tx.get('交易日')}_{description}_{foreign}"
        annotate_source(record, {"卡末四位": tail})
        records.append(record)

    # 合并"银联Pay境内返现"和"银联Pay境内返现调回"的记录
    return _merge_cmb_silver_rebate_records(records)
