from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from wacai_reconcile.parsers.cmb_debit import parse_cmb_debit


def test_cmb_debit_parser_extracts_income_and_expense() -> None:
    statement = Path("data/招商银行交易流水(申请时间2025年11月02日16时11分28秒).pdf")
    records = parse_cmb_debit(statement)

    assert len(records) > 40

    income = next(record for record in records if record.timestamp.strftime("%Y-%m-%d") == "2025-07-14")
    assert income.direction == "income"
    assert income.amount == Decimal("39677.00")
    assert income.meta.source_extras.get("交易摘要") == "行内转账转入"

    expense_amounts = {
        record.amount
        for record in records
        if record.direction == "expense" and record.timestamp.strftime("%Y-%m-%d") == "2025-09-04"
    }
    assert Decimal("1000.00") in expense_amounts
    assert Decimal("31862.31") in expense_amounts


def test_cmb_debit_parser_strips_headers_and_footers() -> None:
    statement = Path("data/招商银行交易流水(申请时间2025年11月02日16时11分28秒).pdf")
    records = parse_cmb_debit(statement)

    row = next(record for record in records if record.timestamp.strftime("%Y-%m-%d") == "2025-05-13")
    extras = row.meta.source_extras
    assert "记账日期" not in extras.values()
    assert all("第" not in value for value in extras.values())
    assert all("/" not in value for value in extras.values())
