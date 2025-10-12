from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pandas as pd

from wacai_reconcile.baseline import BaselineIndex, build_account_locks
from wacai_reconcile.pipeline import apply_account_locks, apply_baseline_dedupe, supplement_card_remarks
from wacai_reconcile.refund import apply_refund_pairs
from wacai_reconcile.models import Sheet, StandardRecord


def _make_record(
    *,
    sheet: Sheet = Sheet.EXPENSE,
    amount: Decimal = Decimal("10.00"),
    timestamp: datetime,
    account: str,
    remark: str = "测试",
    source: str = "测试渠道",
    channel: str,
    extras: dict | None = None,
) -> StandardRecord:
    record = StandardRecord(
        sheet=sheet,
        row={"备注": remark},
        timestamp=timestamp,
        amount=amount,
        direction="income" if sheet == Sheet.INCOME else "expense",
        account=account,
        remark=remark,
        source=source,
    )
    record.meta.base_remark = remark
    record.meta.channel = channel
    if extras:
        for key, value in extras.items():
            if key == "source_extras" and isinstance(value, dict):
                record.meta.source_extras.update(value)
            else:
                setattr(record.meta, key, value)
    return record


def test_build_account_locks_picks_latest_timestamp() -> None:
    lock_time_1 = "2025-09-01 08:00:00"
    lock_time_2 = "2025-10-01 09:00:00"
    frames = {
        "支出": pd.DataFrame(
            [
                {"账户": "微信", "消费日期": lock_time_1, "备注": "余额调整产生的烂账"},
                {"账户": "微信", "消费日期": lock_time_2, "备注": "余额调整产生的烂账"},
                {"账户": "支付宝", "消费日期": lock_time_1, "备注": "普通记录"},
            ]
        ),
        "收入": pd.DataFrame(),
        "转账": pd.DataFrame(),
        "借入借出": pd.DataFrame(),
        "收款还款": pd.DataFrame(),
    }

    locks = build_account_locks(frames)
    assert "微信" in locks
    assert locks["微信"] == datetime(2025, 10, 1, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert "支付宝" not in locks


def test_apply_refund_pairs_marks_expense_and_income() -> None:
    expense = _make_record(
        sheet=Sheet.EXPENSE,
        timestamp=datetime(2025, 10, 11, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        amount=Decimal("50.00"),
        account="微信",
        remark="订单A",
        source="微信支付",
        channel="wechat",
    )
    income = _make_record(
        sheet=Sheet.INCOME,
        timestamp=datetime(2025, 10, 11, 13, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        amount=Decimal("50.00"),
        account="微信",
        remark="订单A",
        source="微信支付",
        channel="wechat",
        extras={"matching_key": "订单A"},
    )

    apply_refund_pairs([expense, income], window=timedelta(hours=2))

    assert expense.canceled is True
    assert income.canceled is True
    assert expense.skipped_reason == "refund-matched"
    assert income.skipped_reason == "refund-matched"


def test_apply_account_locks_marks_records() -> None:
    dt_lock = datetime(2025, 10, 1, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    locks = {"微信": dt_lock}
    record_before = _make_record(
        timestamp=datetime(2025, 9, 30, 23, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        account="微信",
        channel="wechat",
    )
    record_after = _make_record(
        timestamp=datetime(2025, 10, 2, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        account="微信",
        channel="wechat",
    )

    apply_account_locks([record_before, record_after], locks)

    assert record_before.skipped_reason == "account-locked"
    assert record_after.skipped_reason is None


def test_apply_baseline_dedupe_detects_duplicates() -> None:
    baseline_frame = pd.DataFrame(
        [
            {
                "账户": "微信",
                "消费日期": "2025-10-11 12:25:03",
                "消费金额": "10.00",
                "备注": "测试",
            }
        ]
    )
    frames = {sheet: pd.DataFrame() for sheet in ["支出", "收入", "转账", "借入借出", "收款还款"]}
    frames["支出"] = baseline_frame
    baseline_index = BaselineIndex(frames, amount_tolerance=0.01, date_tolerance=timedelta(hours=48))

    record = _make_record(
        timestamp=datetime(2025, 10, 11, 12, 25, 3, tzinfo=ZoneInfo("Asia/Shanghai")),
        account="微信",
        channel="wechat",
    )

    apply_baseline_dedupe([record], baseline_index)
    assert record.skipped_reason == "duplicate-baseline"


def test_supplement_card_remarks_matches_by_remark() -> None:
    timestamp = datetime(2025, 10, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    wallet_record = _make_record(
        timestamp=timestamp,
        account="微信",
        remark="订单B",
        source="微信支付",
        channel="wechat",
        extras={"source_extras": {"支付方式": "零钱", "状态": "支付成功"}},
    )
    card_record = _make_record(
        timestamp=timestamp + timedelta(minutes=5),
        account="中信银行信用卡(1129)",
        remark="订单B",
        source="中信银行信用卡",
        channel="citic",
    )

    supplement_card_remarks(
        [wallet_record, card_record],
        amount_tolerance=Decimal("0.01"),
        date_tolerance=timedelta(hours=2),
    )

    assert "来源补充(" in card_record.row["备注"]
    assert "支付成功" in card_record.row["备注"]
    assert card_record.meta.supplemented_from == "wechat"
