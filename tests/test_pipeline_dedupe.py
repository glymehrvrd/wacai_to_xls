from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pandas as pd

from pathlib import Path

from wacai_reconcile.baseline import BaselineIndex, build_account_locks
from wacai_reconcile.io_utils import build_increment_frames
from wacai_reconcile.pipeline import (
    apply_account_locks,
    apply_baseline_dedupe,
    supplement_card_remarks,
    write_intermediate_csv,
)
from wacai_reconcile.refund import apply_refund_pairs
from wacai_reconcile.models import (
    ExpenseRecord,
    IncomeRecord,
    Sheet,
    SheetBundle,
    StandardRecord,
)


def _make_record(
    *,
    sheet: Sheet = Sheet.EXPENSE,
    amount: Decimal = Decimal("10.00"),
    timestamp: datetime,
    account: str,
    remark: str = "测试",
    channel: str,
    extras: dict | None = None,
) -> StandardRecord:
    if sheet == Sheet.EXPENSE:
        record: StandardRecord = ExpenseRecord(
            timestamp=timestamp,
            amount=amount,
            direction="expense",
            account=account,
            remark=remark,
        )
    elif sheet == Sheet.INCOME:
        record = IncomeRecord(
            timestamp=timestamp,
            amount=amount,
            direction="income",
            account=account,
            remark=remark,
        )
    else:
        raise ValueError(f"Unsupported sheet in test factory: {sheet}")
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
        channel="wechat",
    )
    income = _make_record(
        sheet=Sheet.INCOME,
        timestamp=datetime(2025, 10, 11, 13, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        amount=Decimal("50.00"),
        account="微信",
        remark="订单A",
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
        channel="wechat",
        extras={"source_extras": {"支付方式": "中信银行信用卡(1129)", "状态": "支付成功"}},
    )
    card_record = _make_record(
        timestamp=timestamp + timedelta(minutes=5),
        account="中信银行信用卡(1129)",
        remark="订单B",
        channel="citic",
    )

    supplement_card_remarks(
        [wallet_record, card_record],
        amount_tolerance=Decimal("0.01"),
        date_tolerance=timedelta(hours=2),
    )

    assert "来源补充(" in card_record.remark
    assert "支付成功" in card_record.remark
    assert card_record.meta.supplemented_from == "wechat"


def test_supplement_card_remarks_skips_when_pay_method_mismatch() -> None:
    timestamp = datetime(2025, 10, 13, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    wallet_record = _make_record(
        timestamp=timestamp,
        account="微信",
        remark="订单C",
        channel="wechat",
        extras={"source_extras": {"支付方式": "零钱"}},
    )
    card_record = _make_record(
        timestamp=timestamp + timedelta(minutes=3),
        account="中信银行信用卡(1129)",
        remark="订单C",
        channel="citic",
    )

    supplement_card_remarks(
        [wallet_record, card_record],
        amount_tolerance=Decimal("0.01"),
        date_tolerance=timedelta(hours=2),
    )

    assert "来源补充" not in card_record.remark
    assert card_record.meta.supplemented_from is None


def test_supplement_only_filtered_from_intermediate_csv(tmp_path: Path) -> None:
    """测试 supplement_only 的记录不会写入中间 CSV."""
    timestamp = datetime(2025, 10, 14, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    normal_record = _make_record(
        timestamp=timestamp,
        account="微信",
        remark="正常记录",
        channel="wechat",
    )
    supplement_only_record = _make_record(
        timestamp=timestamp + timedelta(minutes=1),
        account="微信",
        remark="补充记录",
        channel="wechat",
        extras={"supplement_only": True},
    )

    intermediate_dir = tmp_path / "intermediate"
    write_intermediate_csv(intermediate_dir, "wechat", [normal_record, supplement_only_record])

    # 检查中间 CSV 文件
    wechat_dir = intermediate_dir / "wechat"
    expense_csv = wechat_dir / "支出.csv"
    assert expense_csv.exists()

    df = pd.read_csv(expense_csv, encoding="utf-8-sig")
    # 应该只有正常记录，supplement_only 记录应该被过滤
    assert len(df) == 1
    assert df.iloc[0]["备注"] == "正常记录"


def test_supplement_only_filtered_from_sheet_bundle() -> None:
    """测试 supplement_only 的记录不会进入 SheetBundle."""
    timestamp = datetime(2025, 10, 15, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    normal_record = _make_record(
        timestamp=timestamp,
        account="微信",
        remark="正常记录",
        channel="wechat",
    )
    supplement_only_record = _make_record(
        timestamp=timestamp + timedelta(minutes=1),
        account="微信",
        remark="补充记录",
        channel="wechat",
        extras={"supplement_only": True},
    )
    canceled_record = _make_record(
        timestamp=timestamp + timedelta(minutes=2),
        account="微信",
        remark="已取消记录",
        channel="wechat",
    )
    canceled_record.canceled = True

    bundle = SheetBundle()
    bundle.update_from_records([normal_record, supplement_only_record, canceled_record])

    # 应该只有正常记录
    assert len(bundle.frames["支出"]) == 1
    assert bundle.frames["支出"].iloc[0]["备注"] == "正常记录"


def test_supplement_only_filtered_from_increment_frames() -> None:
    """测试 supplement_only 的记录不会进入增量帧."""
    timestamp = datetime(2025, 10, 16, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    normal_record = _make_record(
        timestamp=timestamp,
        account="微信",
        remark="正常记录",
        channel="wechat",
    )
    supplement_only_record = _make_record(
        timestamp=timestamp + timedelta(minutes=1),
        account="微信",
        remark="补充记录",
        channel="wechat",
        extras={"supplement_only": True},
    )
    skipped_record = _make_record(
        timestamp=timestamp + timedelta(minutes=2),
        account="微信",
        remark="已跳过记录",
        channel="wechat",
    )
    skipped_record.skipped_reason = "duplicate-baseline"

    frames = build_increment_frames([normal_record, supplement_only_record, skipped_record])

    # 应该只有正常记录
    assert len(frames["支出"]) == 1
    assert frames["支出"].iloc[0]["备注"] == "正常记录"
