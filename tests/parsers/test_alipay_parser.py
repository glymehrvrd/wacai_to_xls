from __future__ import annotations

from pathlib import Path

from wacai_reconcile.parsers.alipay import parse_alipay


def _write_alipay_file(path: Path, rows: list[list[str]]) -> None:
    header_lines = ["备注说明\n"] * 25
    data_lines = [
        ",".join(values) + "\n"
        for values in rows
    ]
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
    assert wallet_record.meta.source_extras.get("支付方式") == "余额"

    assert card_record.skipped_reason == "non-wallet-payment"
    assert card_record.meta.supplement_only is True
