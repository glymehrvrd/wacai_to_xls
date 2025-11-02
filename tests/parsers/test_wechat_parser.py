from __future__ import annotations

from pathlib import Path

import pandas as pd

from wacai_reconcile.models import TransferRecord
from wacai_reconcile.parsers.wechat import parse_wechat


def _write_wechat_file(path: Path, rows: list[dict[str, object]]) -> None:
    df = pd.DataFrame(rows)
    with pd.ExcelWriter(path) as writer:
        # 将表头及数据写到第 17 行，模拟真实账单前置说明行
        df.to_excel(writer, index=False, startrow=16)


def test_wechat_parser_marks_wallet_vs_card(tmp_path: Path) -> None:
    file_path = tmp_path / "wechat.xlsx"
    rows = [
        {
            "收/支": "支出",
            "金额(元)": 10.0,
            "交易时间": "2025-10-11 12:25:03",
            "备注": "测试钱包支付",
            "当前状态": "支付成功",
            "商品": "商品A",
            "交易对方": "商户甲",
            "交易单号": "ID_WALLET",
            "支付方式": "零钱",
        },
        {
            "收/支": "支出",
            "金额(元)": 12.0,
            "交易时间": "2025-10-11 13:25:03",
            "备注": "测试信用卡支付",
            "当前状态": "支付成功",
            "商品": "商品B",
            "交易对方": "商户乙",
            "交易单号": "ID_CARD",
            "支付方式": "中信银行信用卡(1129)",
        },
        {
            "收/支": "/",
            "金额(元)": 100.0,
            "交易时间": "2025-10-11 14:00:00",
            "备注": "/",
            "当前状态": "支付成功",
            "商品": "/",
            "交易类型": "转入「日常消费」小金罐-来自零钱",
            "交易对方": "/",
            "交易单号": "ID_TRANSFER",
            "支付方式": "零钱",
        },
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)
    # 转账记录（零钱到小金罐）都映射为"微信"，所以会被跳过
    assert len(records) == 2

    wallet_record = next(record for record in records if record.raw_id == "ID_WALLET")
    card_record = next(record for record in records if record.raw_id == "ID_CARD")

    assert wallet_record.skipped_reason is None
    assert wallet_record.meta.source_extras.get("支付方式") == "零钱"

    assert card_record.skipped_reason == "non-wallet-payment"
    assert card_record.meta.supplement_only is True


def test_wechat_income_account_is_wechat(tmp_path: Path) -> None:
    file_path = tmp_path / "wechat_income.xlsx"
    rows = [
        {
            "收/支": "收入",
            "金额(元)": 100.0,
            "交易时间": "2025-10-11 15:00:00",
            "备注": "收款测试",
            "当前状态": "已入账",
            "商品": "商品收款",
            "交易对方": "付款人A",
            "交易单号": "ID_INCOME",
            "支付方式": "/",
        },
        {
            "收/支": "收入",
            "金额(元)": 200.0,
            "交易时间": "2025-10-11 16:00:00",
            "备注": "收款测试2",
            "当前状态": "已入账",
            "商品": "商品收款2",
            "交易对方": "付款人B",
            "交易单号": "ID_INCOME2",
            "支付方式": "零钱",
        },
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)
    assert len(records) == 2

    income_record1 = next(record for record in records if record.raw_id == "ID_INCOME")
    income_record2 = next(record for record in records if record.raw_id == "ID_INCOME2")

    # 无论支付方式是什么，收入记录的账户都应该是"微信"
    assert income_record1.sheet.value == "收入"
    assert income_record1.direction == "income"
    assert income_record1.account == "微信"
    assert income_record1.amount == 100.0

    assert income_record2.sheet.value == "收入"
    assert income_record2.direction == "income"
    assert income_record2.account == "微信"
    assert income_record2.amount == 200.0


def test_wechat_internal_accounts_normalized(tmp_path: Path) -> None:
    """测试微信内部账户（小金罐、亲属卡等）统一映射为"微信"."""
    file_path = tmp_path / "wechat_internal.xlsx"
    rows = [
        {
            "收/支": "支出",
            "金额(元)": 50.0,
            "交易时间": "2025-10-11 10:00:00",
            "备注": "测试小金罐",
            "当前状态": "支付成功",
            "商品": "商品X",
            "交易对方": "商户X",
            "交易单号": "ID_XIAOJINGUAN",
            "支付方式": "小金罐",
        },
        {
            "收/支": "支出",
            "金额(元)": 60.0,
            "交易时间": "2025-10-11 11:00:00",
            "备注": "测试亲属卡",
            "当前状态": "支付成功",
            "商品": "商品Y",
            "交易对方": "商户Y",
            "交易单号": "ID_QINSHU",
            "支付方式": "亲属卡",
        },
        {
            "收/支": "/",
            "金额(元)": 200.0,
            "交易时间": "2025-10-11 12:00:00",
            "备注": "/",
            "当前状态": "支付成功",
            "商品": "/",
            "交易类型": "转入「日常消费」小金罐-来自零钱",
            "交易对方": "/",
            "交易单号": "ID_XIAOJINGUAN_TRANSFER",
            "支付方式": "零钱",
        },
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)
    # 转账记录（零钱到小金罐）都映射为"微信"，所以会被跳过
    assert len(records) == 2

    xiaojinguan_record = next(record for record in records if record.raw_id == "ID_XIAOJINGUAN")
    qinshu_record = next(record for record in records if record.raw_id == "ID_QINSHU")

    # 小金罐账户应映射为"微信"
    assert xiaojinguan_record.account == "微信"
    assert xiaojinguan_record.meta.source_extras.get("支付方式") == "小金罐"

    # 亲属卡账户应映射为"微信"
    assert qinshu_record.account == "微信"
    assert qinshu_record.meta.source_extras.get("支付方式") == "亲属卡"


def test_wechat_transfer_same_account_skipped(tmp_path: Path) -> None:
    """测试转入和转出账户相同时，跳过该转账记录."""
    file_path = tmp_path / "wechat_same_account.xlsx"
    rows = [
        {
            "收/支": "/",
            "金额(元)": 50.0,
            "交易时间": "2025-10-11 10:00:00",
            "备注": "/",
            "当前状态": "支付成功",
            "商品": "/",
            "交易类型": "转入零钱-来自零钱",
            "交易对方": "/",
            "交易单号": "ID_SAME_ACCOUNT",
            "支付方式": "零钱",
        },
        {
            "收/支": "/",
            "金额(元)": 100.0,
            "交易时间": "2025-10-11 11:00:00",
            "备注": "/",
            "当前状态": "支付成功",
            "商品": "/",
            "交易类型": "转入「日常消费」小金罐-来自零钱",
            "交易对方": "/",
            "交易单号": "ID_DIFF_ACCOUNT",
            "支付方式": "零钱",
        },
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)
    # 前两个记录都被跳过（零钱到零钱，零钱到小金罐都映射为"微信"）
    assert len(records) == 0


def test_wechat_recharge(tmp_path: Path) -> None:
    """测试零钱充值的处理."""
    file_path = tmp_path / "wechat_recharge.xlsx"
    rows = [
        {
            "收/支": "/",
            "金额(元)": 500.0,
            "交易时间": "2025-10-11 10:00:00",
            "备注": "零钱充值",
            "当前状态": "支付成功",
            "商品": "/",
            "交易类型": "零钱充值",
            "交易对方": "/",
            "交易单号": "ID_RECHARGE_CARD",
            "支付方式": "中信银行信用卡(1129)",
        },
        {
            "收/支": "/",
            "金额(元)": 300.0,
            "交易时间": "2025-10-11 11:00:00",
            "备注": "零钱充值",
            "当前状态": "支付成功",
            "商品": "/",
            "交易类型": "零钱充值",
            "交易对方": "/",
            "交易单号": "ID_RECHARGE_WALLET",
            "支付方式": "零钱",
        },
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)

    # 第一个记录：从银行卡充值到零钱，from_account != to_account，应该保留
    # 第二个记录：从零钱充值到零钱（都映射为"微信"），from_account == to_account，应该被跳过
    assert len(records) == 1

    recharge_record = next(record for record in records if record.raw_id == "ID_RECHARGE_CARD")
    assert recharge_record.sheet.value == "转账"
    assert recharge_record.direction == "transfer"
    assert isinstance(recharge_record, TransferRecord)
    # 从银行卡充值到零钱
    assert recharge_record.from_account == "中信银行信用卡(1129)"
    assert recharge_record.to_account == "微信"


def test_wechat_remark_only_keeps_product_field(tmp_path: Path) -> None:
    """测试备注只保留商品字段，不包含备注字段和状态字段."""
    file_path = tmp_path / "wechat_remark.xlsx"
    rows = [
        {
            "收/支": "支出",
            "金额(元)": 10.0,
            "交易时间": "2025-10-11 12:00:00",
            "备注": "备注内容A",
            "当前状态": "支付成功",
            "商品": "商品A",
            "交易对方": "商户A",
            "交易单号": "ID_WITH_PRODUCT",
            "支付方式": "零钱",
        },
        {
            "收/支": "支出",
            "金额(元)": 20.0,
            "交易时间": "2025-10-11 12:05:00",
            "备注": "备注内容B",
            "当前状态": "支付成功",
            "商品": "",
            "交易对方": "商户B",
            "交易单号": "ID_WITHOUT_PRODUCT",
            "支付方式": "零钱",
        },
        {
            "收/支": "收入",
            "金额(元)": 30.0,
            "交易时间": "2025-10-11 12:10:00",
            "备注": "备注内容C",
            "当前状态": "收款成功",
            "商品": "商品C",
            "交易对方": "商户C",
            "交易单号": "ID_INCOME",
            "支付方式": "零钱",
        },
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)
    assert len(records) == 3

    # 验证第一个记录：有商品时，备注以商品开头，不包含备注字段和状态字段的内容
    record1 = next(record for record in records if record.raw_id == "ID_WITH_PRODUCT")
    assert record1.remark.startswith("商品A")
    assert "备注内容A" not in record1.remark
    # 验证不会包含原状态字段作为备注内容（但annotate_source添加的除外）
    # 验证 annotate_source 添加的信息仍然存在
    assert record1.meta.source_extras.get("支付方式") == "零钱"
    assert record1.meta.source_extras.get("状态") == "支付成功"

    # 验证第二个记录：没有商品时，备注为空（annotate_source 不再添加到备注）
    record2 = next(record for record in records if record.raw_id == "ID_WITHOUT_PRODUCT")
    assert record2.remark == ""
    assert "备注内容B" not in record2.remark
    # 验证 extra 信息保存在 source_extras 中，但不添加到备注
    assert record2.meta.source_extras.get("支付方式") == "零钱"
    assert record2.meta.source_extras.get("状态") == "支付成功"

    # 验证第三个记录：收入记录同样只保留商品字段
    record3 = next(record for record in records if record.raw_id == "ID_INCOME")
    assert record3.remark.startswith("商品C")
    assert "备注内容C" not in record3.remark


def test_wechat_meaningless_remark_cleared(tmp_path: Path) -> None:
    """测试无意义备注的记录保留，但备注被清空."""
    file_path = tmp_path / "wechat_meaningless.xlsx"
    rows = [
        {
            "收/支": "支出",
            "金额(元)": 10.0,
            "交易时间": "2025-10-11 12:00:00",
            "备注": "备注内容A",
            "当前状态": "支付成功",
            "商品": "商品A",
            "交易对方": "商户A",
            "交易单号": "ID_VALID",
            "支付方式": "零钱",
        },
        {
            "收/支": "支出",
            "金额(元)": 20.0,
            "交易时间": "2025-10-11 12:05:00",
            "备注": "备注内容B",
            "当前状态": "支付成功",
            "商品": "none",  # 无意义备注
            "交易对方": "商户B",
            "交易单号": "ID_NONE",
            "支付方式": "零钱",
        },
        {
            "收/支": "收入",
            "金额(元)": 30.0,
            "交易时间": "2025-10-11 12:10:00",
            "备注": "备注内容C",
            "当前状态": "收款成功",
            "商品": "\\",  # 无意义备注
            "交易对方": "商户C",
            "交易单号": "ID_BACKSLASH",
            "支付方式": "零钱",
        },
        {
            "收/支": "/",
            "金额(元)": 100.0,
            "交易时间": "2025-10-11 12:15:00",
            "备注": "转账备注",
            "当前状态": "支付成功",
            "商品": "/",  # 无意义备注
            "交易类型": "零钱充值",
            "交易对方": "商户D",
            "交易单号": "ID_TRANSFER",
            "支付方式": "中信银行信用卡(1129)",
        },
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)

    # 应该保留所有4条记录
    assert len(records) == 4

    # 验证有效记录
    valid_record = next(record for record in records if record.raw_id == "ID_VALID")
    assert valid_record.remark == "商品A"

    # 验证无意义备注的记录保留，但备注被清空
    none_record = next(record for record in records if record.raw_id == "ID_NONE")
    assert none_record.remark == ""

    backslash_record = next(record for record in records if record.raw_id == "ID_BACKSLASH")
    assert backslash_record.remark == ""

    # 验证转账记录保留，但备注被清空
    transfer_record = next(record for record in records if record.raw_id == "ID_TRANSFER")
    assert transfer_record.sheet.value == "转账"
    assert transfer_record.remark == ""
