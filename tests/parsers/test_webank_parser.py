from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from wacai_reconcile.parsers.webank import parse_webank


def test_webank_parser_extracts_income_and_expense_records() -> None:
    statement_path = Path("data/微众银行-交易流水No.Z202511020062-1.pdf")
    records = parse_webank(statement_path)

    assert len(records) == 110
    assert any(record.direction == "expense" for record in records)
    assert any(record.direction == "income" for record in records)
    assert {record.account for record in records} == {"微众银行(3793)"}


def test_webank_parser_populates_extras_for_known_transactions() -> None:
    statement_path = Path("data/微众银行-交易流水No.Z202511020062-1.pdf")
    records = parse_webank(statement_path)

    expense = next(
        record
        for record in records
        if record.direction == "expense"
        and record.amount == Decimal("900.00")
        and record.meta.source_extras.get("摘要") == "账户扣划"
    )
    assert expense.meta.source_extras.get("对方行名") == "招商银行"
    assert expense.remark == "账户扣划"

    income = next(
        record
        for record in records
        if record.direction == "income"
        and record.amount == Decimal("900.00")
        and record.meta.source_extras.get("摘要") == "理财子转出"
    )
    assert income.payer == "快速赎回待清算款项-理财子-活期加plus"
    assert income.meta.source_extras.get("对方账号") == "10600601104401060000169"
    assert income.remark == "理财子转出"


def test_webank_parser_ignores_page_footer_tokens() -> None:
    statement_path = Path("data/微众银行-交易流水No.Z202511020062-1.pdf")
    records = parse_webank(statement_path)

    footer_record = next(
        record
        for record in records
        if record.direction == "expense"
        and record.timestamp.strftime("%Y%m%d") == "20250506"
        and record.meta.source_extras.get("摘要") == "基金申购"
    )

    extras = footer_record.meta.source_extras
    assert footer_record.account == "微众银行(3793)"
    assert footer_record.amount == Decimal("34121.14")
    assert extras.get("对方账号") == "10600601304006010001060"
    assert extras.get("对方行名") == "深圳前海微众银行股份有限公司"
    assert all("第" not in value and "打印时间" not in value for value in extras.values())
