from __future__ import annotations

from pathlib import Path

from wacai_reconcile.models import IncomeRecord
from wacai_reconcile.parsers.alipay import parse_alipay


def _write_alipay_file(path: Path, rows: list[list[str]]) -> None:
    header_lines = ["备注说明\n"] * 25
    data_lines = [",".join(values) + "\n" for values in rows]
    content = "".join(header_lines + data_lines).encode("gbk")
    path.write_bytes(content)


def test_alipay_parser_filters_card_payments(tmp_path: Path) -> None:
    file_path = tmp_path / "alipay.csv"
    rows = [
        [
            "2025-10-11 12:00:00",
            "线上购物",
            "商户甲",
            "",
            "商品A",
            "支出",
            "20.00",
            "余额",
            "交易成功",
            "ORDER_WALLET",
            "",
            "",
            "",
        ],
        [
            "2025-10-11 12:05:00",
            "线上购物",
            "商户乙",
            "",
            "商品B",
            "支出",
            "30.00",
            "中信银行信用卡(1129)",
            "交易成功",
            "ORDER_CARD",
            "",
            "",
            "",
        ],
    ]
    _write_alipay_file(file_path, rows)

    records = parse_alipay(file_path)
    assert len(records) == 2

    wallet_record = next(record for record in records if record.raw_id == "ORDER_WALLET")
    card_record = next(record for record in records if record.raw_id == "ORDER_CARD")

    assert wallet_record.skipped_reason is None
    assert wallet_record.account == "支付宝"  # 支付宝内部账户统一记为"支付宝"
    assert wallet_record.meta.source_extras.get("支付方式") == "余额"

    assert card_record.skipped_reason == "non-wallet-payment"
    assert card_record.account == "中信银行信用卡(1129)"  # 外部账户保持原值
    assert card_record.meta.supplement_only is True


def test_alipay_parser_treats_refund_in_not_counting_as_income(tmp_path: Path) -> None:
    file_path = tmp_path / "alipay_refund.csv"
    rows = [
        [
            "2025-09-13 11:12:45",
            "退款",
            "天津迎客松科技有限公司",
            "",
            "商品R",
            "不计收支",
            "420.00",
            "中信银行信用卡(1129)",
            "退款成功",
            "ORDER_REFUND",
            "",
            "",
            "",
        ],
        [
            "2025-09-13 11:12:50",
            "投资理财",
            "天弘基金管理有限公司",
            "",
            "理财",
            "不计收支",
            "0.01",
            "余额",
            "交易成功",
            "ORDER_INVEST",
            "",
            "",
            "",
        ],
    ]
    _write_alipay_file(file_path, rows)

    records = parse_alipay(file_path)
    assert len(records) == 1
    refund_record = records[0]
    assert refund_record.sheet.value == "收入"
    assert refund_record.amount == 420
    assert refund_record.account == "中信银行信用卡(1129)"  # 外部账户保持原值
    assert refund_record.meta.source_extras.get("状态") == "退款成功"


def test_alipay_internal_accounts_normalized(tmp_path: Path) -> None:
    """测试支付宝内部账户：余额、余额宝记为"支付宝"，花呗单独记为"花呗"."""
    file_path = tmp_path / "alipay_internal.csv"
    rows = [
        [
            "2025-10-11 10:00:00",
            "线上购物",
            "商户A",
            "",
            "商品A",
            "支出",
            "10.00",
            "余额",
            "交易成功",
            "ORDER_BALANCE",
            "",
            "",
            "",
        ],
        [
            "2025-10-11 10:05:00",
            "线上购物",
            "商户B",
            "",
            "商品B",
            "支出",
            "20.00",
            "余额宝",
            "交易成功",
            "ORDER_YUEBAO",
            "",
            "",
            "",
        ],
        [
            "2025-10-11 10:10:00",
            "线上购物",
            "商户C",
            "",
            "商品C",
            "支出",
            "30.00",
            "花呗",
            "交易成功",
            "ORDER_HUABEI",
            "",
            "",
            "",
        ],
        [
            "2025-10-11 10:15:00",
            "线上购物",
            "商户D",
            "",
            "商品D",
            "收入",
            "40.00",
            "余额",
            "交易成功",
            "ORDER_INCOME",
            "",
            "",
            "",
        ],
    ]
    _write_alipay_file(file_path, rows)

    records = parse_alipay(file_path)
    assert len(records) == 4

    # 验证余额、余额宝记为"支付宝"
    balance_record = next(record for record in records if record.raw_id == "ORDER_BALANCE")
    assert balance_record.account == "支付宝"
    assert balance_record.meta.source_extras.get("支付方式") == "余额"

    yuebao_record = next(record for record in records if record.raw_id == "ORDER_YUEBAO")
    assert yuebao_record.account == "支付宝"
    assert yuebao_record.meta.source_extras.get("支付方式") == "余额宝"

    # 验证花呗单独记为"花呗"
    huabei_record = next(record for record in records if record.raw_id == "ORDER_HUABEI")
    assert huabei_record.account == "花呗"
    assert huabei_record.meta.source_extras.get("支付方式") == "花呗"

    # 验证收入记录
    income_record = next(record for record in records if record.raw_id == "ORDER_INCOME")
    assert income_record.account == "支付宝"
    assert income_record.meta.source_extras.get("支付方式") == "余额"


def test_alipay_only_refund_status_treated_as_refund(tmp_path: Path) -> None:
    """测试只有明确是"退款"的才是退款，"交易关闭"不是退款."""
    file_path = tmp_path / "alipay_refund_status.csv"
    rows = [
        [
            "2025-09-13 11:12:45",
            "退款",
            "商户A",
            "",
            "商品A",
            "不计收支",
            "100.00",
            "余额",
            "退款成功",
            "ORDER_REFUND_SUCCESS",
            "",
            "",
            "",
        ],
        [
            "2025-09-13 11:13:00",
            "退款",
            "商户B",
            "",
            "商品B",
            "不计收支",
            "200.00",
            "余额",
            "退款",
            "ORDER_REFUND",
            "",
            "",
            "",
        ],
        [
            "2025-09-13 11:13:15",
            "线上购物",
            "商户C",
            "",
            "商品C",
            "不计收支",
            "300.00",
            "余额",
            "交易关闭",
            "ORDER_CLOSED",
            "",
            "",
            "",
        ],
        [
            "2025-09-13 11:13:30",
            "投资理财",
            "商户D",
            "",
            "理财",
            "不计收支",
            "400.00",
            "余额",
            "交易成功",
            "ORDER_SUCCESS",
            "",
            "",
            "",
        ],
    ]
    _write_alipay_file(file_path, rows)

    records = parse_alipay(file_path)
    # 应该只有2条记录（明确是"退款"的记录）
    assert len(records) == 2

    # 验证退款成功被识别为退款
    refund_success = next(record for record in records if record.raw_id == "ORDER_REFUND_SUCCESS")
    assert refund_success.sheet.value == "收入"
    assert isinstance(refund_success, IncomeRecord)
    assert refund_success.category == "退款返款"
    assert refund_success.meta.source_extras.get("状态") == "退款成功"

    # 验证状态为"退款"的也被识别为退款
    refund_status = next(record for record in records if record.raw_id == "ORDER_REFUND")
    assert refund_status.sheet.value == "收入"
    assert isinstance(refund_status, IncomeRecord)
    assert refund_status.category == "退款返款"
    assert refund_status.meta.source_extras.get("状态") == "退款"

    # 验证"交易关闭"和"交易成功"没有被识别为退款（不会被记录）


def test_alipay_skips_zero_amount_transactions(tmp_path: Path) -> None:
    """测试金额为0的交易被跳过."""
    file_path = tmp_path / "alipay_zero.csv"
    rows = [
        [
            "2025-10-11 10:00:00",
            "线上购物",
            "商户A",
            "",
            "商品A",
            "支出",
            "0.00",
            "余额",
            "交易成功",
            "ORDER_ZERO_EXPENSE",
            "",
            "",
            "",
        ],
        [
            "2025-10-11 10:05:00",
            "线上购物",
            "商户B",
            "",
            "商品B",
            "收入",
            "0.00",
            "余额",
            "交易成功",
            "ORDER_ZERO_INCOME",
            "",
            "",
            "",
        ],
        [
            "2025-10-11 10:10:00",
            "退款",
            "商户C",
            "",
            "商品C",
            "不计收支",
            "0.00",
            "余额",
            "退款成功",
            "ORDER_ZERO_REFUND",
            "",
            "",
            "",
        ],
        [
            "2025-10-11 10:15:00",
            "线上购物",
            "商户D",
            "",
            "商品D",
            "支出",
            "10.00",
            "余额",
            "交易成功",
            "ORDER_NON_ZERO",
            "",
            "",
            "",
        ],
    ]
    _write_alipay_file(file_path, rows)

    records = parse_alipay(file_path)
    # 应该只有1条记录（金额非0的记录）
    assert len(records) == 1

    # 验证只有金额非0的记录被保留
    non_zero_record = records[0]
    assert non_zero_record.raw_id == "ORDER_NON_ZERO"
    assert non_zero_record.amount == 10.00


def test_alipay_remark_only_keeps_first_field(tmp_path: Path) -> None:
    """测试备注只保留第一个字段（商品说明），不包含备注字段."""
    file_path = tmp_path / "alipay_remark.csv"
    rows = [
        [
            "2025-10-11 10:00:00",
            "线上购物",
            "商户A",
            "",
            "商品说明A",
            "支出",
            "10.00",
            "余额",
            "交易成功",
            "ORDER_WITH_PRODUCT",
            "",
            "备注内容A",
            "",
        ],
        [
            "2025-10-11 10:05:00",
            "线上购物",
            "商户B",
            "",
            "",
            "支出",
            "20.00",
            "余额",
            "交易成功",
            "ORDER_WITHOUT_PRODUCT",
            "",
            "备注内容B",
            "",
        ],
    ]
    _write_alipay_file(file_path, rows)

    records = parse_alipay(file_path)
    assert len(records) == 2

    # 验证第一个记录：有商品说明时，备注以商品说明开头，不包含备注字段的内容
    record1 = next(record for record in records if record.raw_id == "ORDER_WITH_PRODUCT")
    assert record1.remark.startswith("商品说明A")
    assert "备注内容A" not in record1.remark
    # 验证备注字段（第12列）的内容没有被包含
    assert record1.meta.source_extras.get("支付方式") == "余额"

    # 验证第二个记录：没有商品说明时，备注为空（annotate_source 不再添加到备注）
    record2 = next(record for record in records if record.raw_id == "ORDER_WITHOUT_PRODUCT")
    # 备注应该为空，不包含备注字段的内容
    assert record2.remark == ""
    assert "备注内容B" not in record2.remark
    # 验证 extra 信息保存在 source_extras 中，但不添加到备注
    assert record2.meta.source_extras.get("支付方式") == "余额"
    assert record2.meta.source_extras.get("状态") == "交易成功"
