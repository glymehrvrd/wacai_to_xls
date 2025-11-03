from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import re
import pdfplumber

from .base import annotate_source, create_expense_record, create_income_record
from ..models import StandardRecord
from ..utils import normalize_text, to_decimal

_Token = Tuple[str, float]
_COLUMNS: Sequence[tuple[str, float, float]] = (
    ("date", 0, 90),
    ("currency", 90, 140),
    ("amount", 150, 220),
    ("balance", 220, 300),
    ("description", 300, 410),
    ("counterparty", 410, 600),
)

_FOOTER_TOP_THRESHOLD = 740.0

_ACCOUNT_PATTERN = re.compile(r"账号：(\d+)")


@dataclass
class _CMBRow:
    date: str
    currency: str
    amount: str
    balance: str
    description: str
    counterparty: str


def parse_cmb_debit(path: Path) -> List[StandardRecord]:
    if not path.exists():
        raise FileNotFoundError(f"CMB debit statement not found: {path}")

    rows = _extract_rows(path)
    if not rows:
        return []

    account_tail = _extract_account_tail(path)
    account_name = "招商银行储蓄卡"
    if account_tail:
        account_name = f"{account_name}({account_tail})"

    records: List[StandardRecord] = []
    for row in rows:
        amount_decimal = to_decimal(row.amount)
        if amount_decimal == 0:
            continue
        extras: Dict[str, str] = {}
        if row.currency:
            extras["币种"] = row.currency
        if row.balance:
            extras["联机余额"] = row.balance
        if row.description:
            extras["交易摘要"] = row.description
        if row.counterparty:
            extras["对手信息"] = row.counterparty

        remark = row.description or ""
        counterparty = row.counterparty or ""
        if amount_decimal > 0:
            record = create_income_record(
                amount=amount_decimal,
                timestamp=row.date,
                account=account_name,
                remark=remark,
                payer=counterparty or None,
                category="待分类",
            )
        else:
            record = create_expense_record(
                amount=abs(amount_decimal),
                timestamp=row.date,
                account=account_name,
                remark=remark,
                merchant=counterparty or None,
                category_main="待分类",
                category_sub="待分类",
            )
        if record is None:
            continue
        record.raw_id = f"{row.date}-{row.amount}-{row.description}-{row.counterparty}"
        annotate_source(record, extras)
        records.append(record)
    return records


def _extract_account_tail(path: Path) -> str:
    with pdfplumber.open(path) as pdf:
        first_page = pdf.pages[0]
        text = first_page.extract_text() or ""
    match = _ACCOUNT_PATTERN.search(text)
    if match:
        number = match.group(1)
        return number[-4:]
    return ""


def _extract_rows(path: Path) -> List[_CMBRow]:
    rows: List[_CMBRow] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            words = page.extract_words(use_text_flow=True)
            rows.extend(_parse_words(words))
    return rows


def _parse_words(words: Sequence[dict]) -> List[_CMBRow]:
    rows: List[_CMBRow] = []
    current: Dict[str, List[_Token]] | None = None

    for word in words:
        top = float(word["top"])
        if top >= _FOOTER_TOP_THRESHOLD:
            continue
        text = normalize_text(word.get("text"))
        if not text:
            continue

        column = _resolve_column(word)
        if column == "date" and _is_date_token(text):
            if current:
                rows.append(_build_row(current))
                print(rows[-1])
            current = {name: [] for name, *_ in _COLUMNS}
            current["date"].append((text, float(word["x0"])))
            continue

        if current is None or column is None:
            continue
        current[column].append((text, float(word["x0"])))

    if current:
        rows.append(_build_row(current))
        print(rows[-1])
    return rows


def _resolve_column(word: dict) -> str | None:
    center = (float(word["x0"]) + float(word["x1"])) / 2
    for name, left, right in _COLUMNS:
        if left <= center < right:
            return name
    return None


def _is_date_token(text: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", text))


def _join_tokens(tokens: List[_Token], separator: str = " ") -> str:
    if not tokens:
        return ""
    parts: List[str] = []
    buffer = ""
    prev_x: float | None = None
    for value, x0 in tokens:
        cleaned = normalize_text(value)
        if not cleaned:
            continue
        if prev_x is not None and abs(x0 - prev_x) <= 0.5:
            buffer += cleaned
        else:
            if buffer:
                parts.append(buffer)
            buffer = cleaned
        prev_x = x0
    if buffer:
        parts.append(buffer)
    if not parts:
        return ""
    return separator.join(parts)


def _build_row(columns: Dict[str, List[_Token]]) -> _CMBRow:
    return _CMBRow(
        date=_join_tokens(columns["date"], separator=""),
        currency=_join_tokens(columns["currency"], separator=""),
        amount=_join_tokens(columns["amount"], separator=""),
        balance=_join_tokens(columns["balance"], separator=""),
        description=_join_tokens(columns["description"]),
        counterparty=_join_tokens(columns["counterparty"]),
    )
