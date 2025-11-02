# -*- coding: utf-8 -*-
import pandas as pd
import sqlite3
from datetime import datetime
import sys
import os


def parse_account(account_uuid, accounts):
    account = accounts[account_uuid]
    pos = account.find("-")
    fee_type = "人民币"
    if pos != -1:
        fee_type = account[pos + 1 :]
        account = account[:pos]
    return account, fee_type


if len(sys.argv) != 2:
    print("Usage: %s wacai365.so" % sys.argv[0])
    sys.exit(1)

# if file not exists, exit
if not os.path.isfile(sys.argv[1]):
    print("File not found: %s" % sys.argv[1])
    sys.exit(1)

conn = sqlite3.connect(sys.argv[1])

df = pd.read_sql_query("select uuid,name from TBL_ACCOUNTINFO", conn)
accounts = {}
for _, row in df.iterrows():
    accounts[row["uuid"]] = row["name"]

df = pd.read_sql_query("select uuid,name from TBL_OUTGOCATEGORYINFO", conn)
outgomaintype = {}
for _, row in df.iterrows():
    outgomaintype[row["uuid"]] = row["name"]

df = pd.read_sql_query("select uuid,name,parentUuid from TBL_OUTGOCATEGORYINFO", conn)
outgosubtype = {}
outgosubtomain = {}
for _, row in df.iterrows():
    outgosubtype[row["uuid"]] = row["name"]
    outgosubtomain[row["uuid"]] = row["parentUuid"] or row["uuid"]

df = pd.read_sql_query("select uuid,name from TBL_INCOMEMAINTYPEINFO", conn)
incomemaintype = {}
for _, row in df.iterrows():
    incomemaintype[row["uuid"]] = row["name"]

df = pd.read_sql_query("select uuid,name from TBL_BOOK", conn)
books = {}
for _, row in df.iterrows():
    books[row["uuid"]] = row["name"]

df = pd.read_sql_query("select * from TBL_TRADEINFO where date>0 order by date", conn)

# 支出
dd_outgo = []
# 收入
dd_income = []
# 转账
dd_transfer = []
# 借入借出
dd_borrow = []
# 收款还款
dd_refund = []

for _, row in df.iterrows():
    try:
        if row["isdelete"] == 1:
            continue

        book = books[row["bookUuid"]]
        account, fee_type = parse_account(row["accountUuid"], accounts)
        dd = datetime.fromtimestamp(row["date"]).strftime("%Y-%m-%d %H:%M:%S")

        tradetype = row["tradetype"]
        if tradetype == 1:
            # outcome
            maintyp = outgomaintype[outgosubtomain[row["typeUuid"]]]
            subtyp = outgosubtype[row["typeUuid"]]
            dd_outgo.append(
                (
                    dd,
                    maintyp,
                    subtyp,
                    "%.2f" % (float(row["money"]) / 100),
                    fee_type,
                    account,
                    "日常",
                    "",
                    "非报销",
                    "",
                    row["comment"] or "",
                )
            )
        elif tradetype == 2:
            # income
            typ = incomemaintype[row["typeUuid"]]
            dd_income.append(
                (
                    dd,
                    typ,
                    "%.2f" % (float(row["money"]) / 100),
                    fee_type,
                    account,
                    "日常",
                    "",
                    "",
                    row["comment"] or "",
                    "非报销",
                )
            )
        elif tradetype == 3:
            # transfer
            account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
            dd_transfer.append(
                (
                    dd,
                    "",
                    account,
                    "%.2f" % (float(row["money"]) / 100),
                    fee_type,
                    account2,
                    "%.2f" % (float(row["money2"]) / 100),
                    fee_type2,
                    row["comment"] or "",
                    "日常",
                )
            )
        elif tradetype == 4:
            # borrow
            if row["typeUuid"] == "0":
                # borrow in
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_borrow.append(
                    (
                        dd,
                        "借入",
                        "%.2f" % (float(row["money"]) / 100),
                        fee_type,
                        account2,
                        account,
                        row["comment"] or "",
                        "日常",
                    )
                )
            else:
                # borrow out
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_borrow.append(
                    (
                        dd,
                        "借出",
                        "%.2f" % (float(row["money"]) / 100),
                        fee_type,
                        account2,
                        account,
                        row["comment"] or "",
                        "日常",
                    )
                )
        elif tradetype == 5:
            # refund
            if row["typeUuid"] == "0":
                # refund in
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_refund.append(
                    (
                        dd,
                        "收款",
                        account2,
                        account,
                        "%.2f" % (float(row["money"]) / 100),
                        "%.2f" % (float(row["money2"]) / 100),
                        fee_type,
                        row["comment"] or "",
                        "日常",
                    )
                )
            else:
                # refund out
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_refund.append(
                    (
                        dd,
                        "还款",
                        account2,
                        account,
                        "%.2f" % (float(row["money"]) / 100),
                        "%.2f" % (float(row["money2"]) / 100),
                        fee_type,
                        row["comment"] or "",
                        "日常",
                    )
                )
        else:
            raise Exception("Unknown tradetype: %s" % tradetype)
    except Exception as e:
        raise e


df_outgo = pd.DataFrame(
    dd_outgo,
    columns=[
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
)
df_income = pd.DataFrame(
    dd_income,
    columns=["收入日期", "收入大类", "收入金额", "币种", "账户", "标签", "付款方", "成员金额", "备注", "报销"],
)
df_transfer = pd.DataFrame(
    dd_transfer,
    columns=["转账时间", "转账类别", "转出账户", "转出金额", "币种", "转入账户", "转入金额", "币种", "备注", "标签"],
)
df_borrow = pd.DataFrame(
    dd_borrow, columns=["借贷时间", "借贷类型", "金额", "币种", "借贷账户", "账户", "备注", "标签"]
)
df_refund = pd.DataFrame(
    dd_refund, columns=["借贷时间", "借贷类型", "借贷账户", "账户", "金额", "利息", "币种", "备注", "标签"]
)

# 输出到输入文件所在目录
input_path = sys.argv[1]
output_path = os.path.join(os.path.dirname(input_path), "wacai.xlsx")
writer = pd.ExcelWriter(output_path)
df_outgo.to_excel(writer, sheet_name="支出", index=False)
df_income.to_excel(writer, sheet_name="收入", index=False)
df_transfer.to_excel(writer, sheet_name="转账", index=False)
df_borrow.to_excel(writer, sheet_name="借入借出", index=False)
df_refund.to_excel(writer, sheet_name="收款还款", index=False)
writer.close()
