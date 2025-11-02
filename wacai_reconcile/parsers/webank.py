from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import re
import pdfplumber

from .base import annotate_source, create_expense_record, create_income_record
from ..models import StandardRecord
from ..utils import normalize_text, to_decimal


# Column boundaries measured from sample statement layout (points from left margin).
_Token = Tuple[str, float]
_COLUMNS: Sequence[tuple[str, float, float]] = (
    ("date", 0, 80),
    ("counterparty_name", 80, 140),
    ("counterparty_account", 140, 228),
    ("counterparty_bank", 228, 288),
    ("description", 288, 348),
    ("remark", 348, 388),
    ("transaction_card", 388, 468),
    ("amount", 468, 528),
    ("balance", 528, 620),
)

_ACCOUNT_NUMBER = re.compile(r"(?:账号/卡号：|Account/Card No.：)\s*(\d{8,})")
_FOOTER_TOP_THRESHOLD = 760.0
_FOOTER_PATTERNS = [
    re.compile(r"^第\d+页$"),
    re.compile(r"^共\d+页$"),
    re.compile(r"^打印时间[:：]"),
    re.compile(r"^\d{2}:\d{2}:\d{2}$"),
]


@dataclass
class _WebankRow:
    date: str
    counterparty_name: str
    counterparty_account: str
    counterparty_bank: str
    description: str
    remark: str
    transaction_card: str
    amount: str
    balance: str


def parse_webank(path: Path) -> List[StandardRecord]:
    if not path.exists():
        raise FileNotFoundError(f"WeBank statement not found: {path}")

    rows = _extract_rows(path)
    if not rows:
        return []

    account_number = _extract_account_number(path)
    account_tail = account_number[-4:] if account_number else ""
    account_name = "微众银行"
    if account_tail:
        account_name = f"{account_name}({account_tail})"

    records: List[StandardRecord] = []
    for row in rows:
        amount_text = row.amount.replace(" ", "")
        if not amount_text:
            continue
        amount_decimal = to_decimal(amount_text)
        if amount_decimal == 0:
            continue

        date_text = _format_date(row.date)
        counterparty = row.counterparty_name or "—"
        remark = row.remark or row.description or ""
        counterparty_account = row.counterparty_account
        if account_number and counterparty_account:
            filtered = [token for token in counterparty_account.split() if token != account_number]
            counterparty_account = " ".join(filtered)

        extras: Dict[str, str] = {}
        if counterparty_account:
            extras["对方账号"] = counterparty_account
        if row.counterparty_bank:
            extras["对方行名"] = row.counterparty_bank
        if row.transaction_card:
            extras["交易卡号"] = row.transaction_card
        if row.balance:
            extras["交易后余额"] = row.balance
        if row.description:
            extras["摘要"] = row.description
        if row.remark:
            extras["备注"] = row.remark

        if amount_decimal < 0:
            record = create_expense_record(
                amount=abs(amount_decimal),
                timestamp=date_text,
                account=account_name,
                remark=remark,
                merchant=counterparty,
            )
        else:
            record = create_income_record(
                amount=amount_decimal,
                timestamp=date_text,
                account=account_name,
                remark=remark,
                payer=counterparty,
                category="待分类",
            )

        if record is None:
            continue

        record.raw_id = f"{row.date}-{row.description}-{row.amount}-{row.transaction_card}"
        annotate_source(record, extras)
        record.meta.matching_key = record.meta.matching_key or counterparty or remark
        records.append(record)

    return records


def _extract_account_number(path: Path) -> str:
    with pdfplumber.open(path) as pdf:
        first_page = pdf.pages[0]
        text = first_page.extract_text() or ""
    match = _ACCOUNT_NUMBER.search(text)
    return match.group(1) if match else ""


def _extract_rows(path: Path) -> List[_WebankRow]:
    rows: List[_WebankRow] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(use_text_flow=True)
            rows.extend(_parse_words(words))
    return rows


def _parse_words(words: Sequence[dict]) -> List[_WebankRow]:
    records: List[_WebankRow] = []
    current: Dict[str, List[_Token]] | None = None

    for word in words:
        if float(word["top"]) >= _FOOTER_TOP_THRESHOLD:
            continue
        text = normalize_text(word.get("text"))
        if not text:
            continue
        if any(pattern.search(text) for pattern in _FOOTER_PATTERNS):
            continue
        column = _resolve_column(word)
        if column == "date" and _is_date_token(text):
            if current:
                records.append(_build_row(current))
                print(records[-1])
            current = {name: [] for name, *_ in _COLUMNS}
            current["date"].append((text, float(word["x0"])))
            continue
        if current is None:
            continue
        if column is None:
            continue
        current[column].append((text, float(word["x0"])))

    if current:
        records.append(_build_row(current))
        print(records[-1])
    return records


def _resolve_column(word: dict) -> str | None:
    center = (float(word["x0"]) + float(word["x1"])) / 2
    for name, left, right in _COLUMNS:
        if left <= center < right:
            return name
    return None


def _is_date_token(text: str) -> bool:
    return text.isdigit() and len(text) == 8


def _build_row(columns: Dict[str, List[_Token]]) -> _WebankRow:
    def join_tokens(tokens: Iterable[_Token], *, separator: str = " ") -> str:
        parts: List[str] = []
        buffer = ""
        prev_x0: float | None = None
        for text_raw, x0 in tokens:
            text = normalize_text(text_raw)
            if not text:
                continue
            if prev_x0 is not None and abs(x0 - prev_x0) <= 0.5:
                buffer += text
            else:
                if buffer:
                    parts.append(buffer)
                buffer = text
            prev_x0 = x0
        if buffer:
            parts.append(buffer)
        if not parts:
            return ""
        if separator == "":
            return "".join(parts)
        return separator.join(parts)

    return _WebankRow(
        date=join_tokens(columns["date"], separator=""),
        counterparty_name=join_tokens(columns["counterparty_name"]),
        counterparty_account=join_tokens(columns["counterparty_account"]),
        counterparty_bank=join_tokens(columns["counterparty_bank"]),
        description=join_tokens(columns["description"]),
        remark=join_tokens(columns["remark"]),
        transaction_card=join_tokens(columns["transaction_card"]),
        amount=join_tokens(columns["amount"], separator=""),
        balance=join_tokens(columns["balance"], separator=""),
    )


def _format_date(raw: str) -> str:
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw
