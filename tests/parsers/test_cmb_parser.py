from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from wacai_reconcile.models import ExpenseRecord, IncomeRecord
from wacai_reconcile.parsers.cmb import _merge_cmb_silver_rebate_records


def test_merge_cmb_silver_rebate_records() -> None:
    """测试合并"银联Pay境内返现"和"银联Pay境内返现调回"的记录."""
    timestamp1 = datetime(2025, 10, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    timestamp2 = datetime(2025, 10, 12, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 创建"银联Pay境内返现"记录（正数），实际格式是 "银联Pay境内返现-xxx"
    rebate_record = ExpenseRecord(
        timestamp=timestamp1,
        amount=Decimal("100.00"),
        direction="expense",
        account="招商银行信用卡(1129)",
        remark="",
        merchant="银联Pay境内返现-美团(商城)(云闪付)-A",
    )
    rebate_record.meta.base_remark = ""

    # 创建"银联Pay境内返现调回"记录（负数，但转换为收入记录），实际格式是 "银联Pay境内返现调回-xxx"
    adjust_record = IncomeRecord(
        timestamp=timestamp2,
        amount=Decimal("50.00"),  # 负数转为正数后的金额
        direction="income",
        account="招商银行信用卡(1129)",
        remark="",
        payer="银联Pay境内返现调回-携程旅行网-Apple Pay",
        category="退款返款",
    )
    adjust_record.meta.base_remark = ""

    # 注意：在CMB解析器中，负值会转换为收入记录，所以调回记录的amount是正数
    # 但在实际数据中，调回可能是负数或正数。为了测试，我们假设调回记录的金额是-50
    # 合并后的金额应该是 100 + (-50) = 50
    # 但我们需要模拟实际情况，调回记录可能是收入记录，金额为正数
    # 所以我们需要在合并函数中处理这种情况

    records = [rebate_record, adjust_record]
    result = _merge_cmb_silver_rebate_records(records)

    # 应该只有1条记录（合并后的新记录）
    assert len(result) == 1
    merged = result[0]
    # 应该是一个新的支出记录（因为合并后金额为正）
    assert isinstance(merged, ExpenseRecord)
    # 金额应该是合并后的金额（100 - 50 = 50）
    assert merged.amount == Decimal("50.00")
    # 时间戳应该是最早的
    assert merged.timestamp == timestamp1
    # merchant应该是"银联Pay境内返现"
    assert merged.merchant == "银联Pay境内返现"
    # 原始记录应该被取消
    assert rebate_record.canceled is True
    assert rebate_record.skipped_reason == "merged-with-rebate"
    assert adjust_record.canceled is True
    assert adjust_record.skipped_reason == "merged-with-rebate"


def test_merge_cmb_silver_rebate_records_zero_amount() -> None:
    """测试合并后金额为0的情况，两条记录都应该被取消."""
    timestamp1 = datetime(2025, 10, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    timestamp2 = datetime(2025, 10, 12, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))

    rebate_record = ExpenseRecord(
        timestamp=timestamp1,
        amount=Decimal("100.00"),
        direction="expense",
        account="招商银行信用卡(1129)",
        remark="",
        merchant="银联Pay境内返现-美团(商城)(云闪付)-A",
    )
    rebate_record.meta.base_remark = ""

    adjust_record = IncomeRecord(
        timestamp=timestamp2,
        amount=Decimal("100.00"),  # 金额相等，合并后为0
        direction="income",
        account="招商银行信用卡(1129)",
        remark="",
        payer="银联Pay境内返现调回-携程旅行网-Apple Pay",
        category="退款返款",
    )
    adjust_record.meta.base_remark = ""

    records = [rebate_record, adjust_record]
    result = _merge_cmb_silver_rebate_records(records)

    # 合并后金额为0，两条记录都应该被取消
    assert len(result) == 0


def test_merge_cmb_silver_rebate_records_all_merged() -> None:
    """测试所有相关记录都合并成一条，不管账户是否相同."""
    timestamp1 = datetime(2025, 10, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    timestamp2 = datetime(2025, 10, 12, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    timestamp3 = datetime(2025, 10, 12, 11, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 创建多条返现和调回记录（包括不同账户）
    rebate_record1 = ExpenseRecord(
        timestamp=timestamp1,
        amount=Decimal("100.00"),
        direction="expense",
        account="招商银行信用卡(1129)",
        remark="",
        merchant="银联Pay境内返现-美团(商城)(云闪付)-A",
    )
    rebate_record1.meta.base_remark = ""

    rebate_record2 = ExpenseRecord(
        timestamp=timestamp2,
        amount=Decimal("50.00"),
        direction="expense",
        account="招商银行信用卡(5678)",  # 不同账户
        remark="",
        merchant="银联Pay境内返现-携程旅行网-Apple Pay",
    )
    rebate_record2.meta.base_remark = ""

    adjust_record = IncomeRecord(
        timestamp=timestamp3,
        amount=Decimal("30.00"),
        direction="income",
        account="招商银行信用卡(1129)",
        remark="",
        payer="银联Pay境内返现调回-滴滴出行(出行)(云闪付",
        category="退款返款",
    )
    adjust_record.meta.base_remark = ""

    records = [rebate_record1, rebate_record2, adjust_record]
    result = _merge_cmb_silver_rebate_records(records)

    # 所有记录合并成一条，金额 = 100 + 50 - 30 = 120
    assert len(result) == 1
    merged = result[0]
    # 应该是一个新的支出记录（因为合并后金额为正）
    assert isinstance(merged, ExpenseRecord)
    assert merged.amount == Decimal("120.00")
    # 时间戳应该是最早的
    assert merged.timestamp == timestamp1
    # merchant应该是"银联Pay境内返现"
    assert merged.merchant == "银联Pay境内返现"
    # 所有原始记录应该被取消
    assert rebate_record1.skipped_reason == "merged-with-rebate"
    assert rebate_record2.skipped_reason == "merged-with-rebate"
    assert adjust_record.skipped_reason == "merged-with-rebate"


def test_merge_cmb_silver_rebate_records_negative_amount() -> None:
    """测试合并后金额为负数时，应该创建收入记录."""
    timestamp1 = datetime(2025, 10, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    timestamp2 = datetime(2025, 10, 12, 10, 5, tzinfo=ZoneInfo("Asia/Shanghai"))

    # 调回金额大于返现金额，合并后为负数
    rebate_record = ExpenseRecord(
        timestamp=timestamp1,
        amount=Decimal("50.00"),
        direction="expense",
        account="招商银行信用卡(1129)",
        remark="",
        merchant="银联Pay境内返现-美团(商城)(云闪付)-A",
    )
    rebate_record.meta.base_remark = ""

    adjust_record = IncomeRecord(
        timestamp=timestamp2,
        amount=Decimal("100.00"),  # 调回金额大于返现
        direction="income",
        account="招商银行信用卡(1129)",
        remark="",
        payer="银联Pay境内返现调回-携程旅行网-Apple Pay",
        category="退款返款",
    )
    adjust_record.meta.base_remark = ""

    records = [rebate_record, adjust_record]
    result = _merge_cmb_silver_rebate_records(records)

    # 应该只有1条记录（合并后的新记录）
    assert len(result) == 1
    merged = result[0]
    # 应该是一个新的收入记录（因为合并后金额为负）
    assert isinstance(merged, IncomeRecord)
    # 金额应该是合并后的金额取绝对值（50 - 100 = -50，取绝对值为50）
    assert merged.amount == Decimal("50.00")
    # 时间戳应该是最早的
    assert merged.timestamp == timestamp1
    # payer应该是"银联Pay境内返现调回"
    assert merged.payer == "银联Pay境内返现调回"
    # 原始记录应该被取消
    assert rebate_record.skipped_reason == "merged-with-rebate"
    assert adjust_record.skipped_reason == "merged-with-rebate"

