# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 19:32:52 2019

@author: jasonjsyuan
"""

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
    outgosubtomain[row["uuid"]] = row["parentUuid"]

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
        # print(book,account)

        tradetype = row["tradetype"]
        if tradetype == 1:
            # outcome
            maintyp = outgomaintype[outgosubtomain[row["typeUuid"]]]
            subtyp = outgosubtype[row["typeUuid"]]
            dd_outgo.append(
                (
                    maintyp,
                    subtyp,
                    account,
                    fee_type,
                    "日常",
                    "",
                    "非报销",
                    dd,
                    "%.2f" % (float(row["money"]) / 100),
                    "",
                    row["comment"] or "",
                    book,
                )
            )
        elif tradetype == 2:
            # income
            typ = incomemaintype[row["typeUuid"]]
            dd_income.append(
                (
                    typ,
                    account,
                    fee_type,
                    "日常",
                    "",
                    dd,
                    "%.2f" % (float(row["money"]) / 100),
                    "",
                    row["comment"] or "",
                    book,
                )
            )
        elif tradetype == 3:
            # transfer
            account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
            dd_transfer.append(
                (
                    account,
                    fee_type,
                    "%.2f" % (float(row["money"]) / 100),
                    account2,
                    fee_type2,
                    "%.2f" % (float(row["money2"]) / 100),
                    dd,
                    row["comment"] or "",
                    book,
                )
            )
        elif tradetype == 4:
            # borrow
            if row["typeUuid"] == "0":
                # borrow in
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_borrow.append(
                    ("借入", dd, account2, account, "%.2f" % (float(row["money"]) / 100), row["comment"] or "", book)
                )
            else:
                # borrow out
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_borrow.append(
                    ("借出", dd, account2, account, "%.2f" % (float(row["money"]) / 100), row["comment"] or "", book)
                )
        elif tradetype == 5:
            # refund
            if row["typeUuid"] == "0":
                # refund in
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_refund.append(
                    (
                        "收款",
                        dd,
                        account2,
                        account,
                        "%.2f" % (float(row["money"]) / 100),
                        "%.2f" % (float(row["money2"]) / 100),
                        row["comment"] or "",
                        book,
                    )
                )
            else:
                # refund out
                account2, fee_type2 = parse_account(row["accountUuid2"], accounts)
                dd_refund.append(
                    (
                        "还款",
                        dd,
                        account2,
                        account,
                        "%.2f" % (float(row["money"]) / 100),
                        "%.2f" % (float(row["money2"]) / 100),
                        row["comment"] or "",
                        book,
                    )
                )
        else:
            print(row)
            typ = incomemaintype[row["typeUuid"]]
            print(typ)
    except Exception as e:
        print("exception", e)
        print(row)
        continue


df_outgo = pd.DataFrame(
    dd_outgo,
    columns=[
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
)
df_income = pd.DataFrame(
    dd_income,
    columns=["收入大类", "账户", "币种", "项目", "付款方", "收入日期", "收入金额", "成员金额", "备注", "账本"],
)
df_transfer = pd.DataFrame(
    dd_transfer, columns=["转出账户", "币种", "转出金额", "转入账户", "币种", "转入金额", "转账时间", "备注", "账本"]
)
df_borrow = pd.DataFrame(dd_borrow, columns=["借贷类型", "借贷时间", "借贷账户", "账户", "金额", "备注", "账本"])
df_refund = pd.DataFrame(
    dd_refund, columns=["借贷类型", "借贷时间", "借贷账户", "账户", "金额", "利息", "备注", "账本"]
)

writer = pd.ExcelWriter("wacai.xlsx")
df_outgo.to_excel(writer, sheet_name="支出", index=False)
df_income.to_excel(writer, sheet_name="收入", index=False)
df_transfer.to_excel(writer, sheet_name="转账", index=False)
df_borrow.to_excel(writer, sheet_name="借入借出", index=False)
df_refund.to_excel(writer, sheet_name="收款还款", index=False)
writer.close()

print(accounts)
