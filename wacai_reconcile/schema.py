from __future__ import annotations

from datetime import timedelta

# Sheet definitions mirror wacai.xlsx template structure.
SHEET_COLUMNS: dict[str, list[str]] = {
    "支出": [
        "消费日期",
        "支出大类",
        "支出小类",
        "消费金额",
        "币种",
        "账户",
        "标签",
        "商家",
        "报销",
        "成员金额",
        "备注",
    ],
    "收入": [
        "收入日期",
        "收入大类",
        "收入金额",
        "币种",
        "账户",
        "标签",
        "付款方",
        "成员金额",
        "备注",
        "报销",
    ],
    "转账": [
        "转账时间",
        "转账类别",
        "转出账户",
        "转出金额",
        "币种",
        "转入账户",
        "转入金额",
        "币种",
        "备注",
        "标签",
    ],
    "借入借出": [
        "借贷时间",
        "借贷类型",
        "金额",
        "币种",
        "借贷账户",
        "账户",
        "备注",
        "标签",
    ],
    "收款还款": [
        "借贷时间",
        "借贷类型",
        "借贷账户",
        "账户",
        "金额",
        "利息",
        "币种",
        "备注",
        "标签",
    ],
}

SHEET_NAMES = list(SHEET_COLUMNS.keys())

# 用于定位日期列
DATE_COLUMNS: dict[str, str] = {
    "支出": "消费日期",
    "收入": "收入日期",
    "转账": "转账时间",
    "借入借出": "借贷时间",
    "收款还款": "借贷时间",
}

# 用于定位金额列
AMOUNT_COLUMNS: dict[str, list[str]] = {
    "支出": ["消费金额"],
    "收入": ["收入金额"],
    "转账": ["转出金额", "转入金额"],
    "借入借出": ["金额"],
    "收款还款": ["金额"],
}

# Defaults filled when template columns absent in source.
DEFAULT_VALUES: dict[str, dict[str, str]] = {
    "支出": {
        "标签": "导入",
        "报销": "非报销",
        "币种": "人民币",
    },
    "收入": {
        "标签": "导入",
        "币种": "人民币",
    },
    "转账": {
        "转账类别": "",
        "币种": "人民币",
        "标签": "导入",
    },
    "借入借出": {
        "币种": "人民币",
        "标签": "导入",
    },
    "收款还款": {
        "币种": "人民币",
        "标签": "导入",
        "利息": "0",
    },
}

LOCK_REMARKS = ["余额调整产生的烂账", "余额调整产生的差额"]  # 账户锁定时写入的备注文字

DEFAULT_AMOUNT_TOLERANCE = 0.01
DEFAULT_DATE_TOLERANCE = timedelta(hours=48)
DEFAULT_REFUND_WINDOW = timedelta(days=30)
