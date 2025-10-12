from __future__ import annotations

from pathlib import Path

import pandas as pd

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
    ]
    _write_wechat_file(file_path, rows)

    records = parse_wechat(file_path)
    assert len(records) == 2

    wallet_record = next(record for record in records if record.raw_id == "ID_WALLET")
    card_record = next(record for record in records if record.raw_id == "ID_CARD")

    assert wallet_record.skipped_reason is None
    assert wallet_record.meta.source_extras.get("支付方式") == "零钱"

    assert card_record.skipped_reason == "non-wallet-payment"
    assert card_record.meta.supplement_only is True
