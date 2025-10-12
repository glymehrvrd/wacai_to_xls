from __future__ import annotations

from datetime import timedelta

# Sheet definitions mirror wacai.xlsx template structure.
SHEET_COLUMNS: dict[str, list[str]] = {
    "支出": [
        "支出大类",
        "支出小类",
        "账户",
        "币种",
        "项目",
        "商家",
        "报销",
        "消费日期",
        "消费金额",
        "成员金额",
        "备注",
        "账本",
    ],
    "收入": [
        "收入大类",
        "账户",
        "币种",
        "项目",
        "付款方",
        "收入日期",
        "收入金额",
        "成员金额",
        "备注",
        "账本",
    ],
    "转账": [
        "转出账户",
        "币种",
        "转出金额",
        "转入账户",
        "币种",
        "转入金额",
        "转账时间",
        "备注",
        "账本",
    ],
    "借入借出": [
        "借贷类型",
        "借贷时间",
        "借贷账户",
        "账户",
        "金额",
        "备注",
        "账本",
    ],
    "收款还款": [
        "借贷类型",
        "借贷时间",
        "借贷账户",
        "账户",
        "金额",
        "利息",
        "备注",
        "账本",
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
        "项目": "日常",
        "报销": "非报销",
        "币种": "人民币",
        "账本": "日常账本",
    },
    "收入": {
        "项目": "日常",
        "币种": "人民币",
        "账本": "日常账本",
    },
    "转账": {
        "币种": "人民币",
        "币种.1": "人民币",
        "账本": "日常账本",
    },
    "借入借出": {
        "账本": "日常账本",
    },
    "收款还款": {
        "账本": "日常账本",
        "利息": "0",
    },
}

LOCK_REMARK = "余额调整产生的烂账"  # 账户锁定时写入的备注文字

DEFAULT_AMOUNT_TOLERANCE = 0.01
DEFAULT_DATE_TOLERANCE = timedelta(hours=48)
DEFAULT_REFUND_WINDOW = timedelta(days=30)
