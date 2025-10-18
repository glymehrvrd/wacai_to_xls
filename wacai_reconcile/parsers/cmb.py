from __future__ import annotations

from email import policy
from email.parser import BytesParser
import re
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup, Tag

from .base import annotate_source, create_expense_record, create_income_record
from ..models import StandardRecord
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
        merchant_name = description.split("－", 1)[-1] if "－" in description else description.split("-", 1)[-1] if "-" in description else description
        merchant_name = normalize_text(merchant_name)
        tail = normalize_text(tx.get("卡号末四位"))
        account = f"招商银行信用卡({tail})" if tail else "招商银行信用卡"
        amount = to_decimal(tx.get("人民币金额"))
        timestamp = _resolve_date(tx.get("交易日") or tx.get("记账日"), cycle)
        if amount <= 0:
            # 招行账单里负值代表退款/还款，转成收入侧方便后续抵消。
            record = create_income_record(
                amount=abs(amount),
                timestamp=timestamp,
                account=account,
                remark="退款/还款",
                payer=description,
                source="招商银行信用卡",
                category="退款返款",
            )
            if record is None:
                continue
            record.meta.merchant = merchant_name
            record.raw_id = f"{tx.get('交易日')}_{description}"
            annotate_source(record, {"卡末四位": tail})
            records.append(record)
            continue
        remark_parts = []
        posting = _resolve_date(tx.get("记账日"), cycle)
        if posting:
            remark_parts.append(f"记账: {posting}")
        location = normalize_text(tx.get("交易地"))
        if location:
            remark_parts.append(f"地点: {location}")
        foreign = normalize_text(tx.get("交易地金额"))
        if foreign:
            remark_parts.append(f"原币金额: {foreign}")
        remark = "; ".join(remark_parts)
        record = create_expense_record(
            amount=amount,
            timestamp=timestamp,
            account=account,
            remark=remark,
            merchant=description,
            source="招商银行信用卡",
        )
        if record is None:
            continue
        record.meta.merchant = merchant_name
        record.raw_id = f"{tx.get('交易日')}_{description}_{foreign}"
        annotate_source(record, {"卡末四位": tail})
        records.append(record)

    return records
