# -*- coding: utf-8 -*-
"""
Created on Fri Mar  8 19:32:52 2019

@author: jasonjsyuan
"""

import pandas as pd
import sqlite3
from datetime import datetime

conn=sqlite3.connect('wacai365.so')

df=pd.read_sql_query('select uuid,name from TBL_ACCOUNTINFO',conn)
accounts={}
for _, row in df.iterrows():
    accounts[row['uuid']]=row['name']
    
df=pd.read_sql_query('select uuid,name from TBL_OUTGOMAINTYPEINFO',conn)
outgomaintype={}
for _,row in df.iterrows():
    outgomaintype[row['uuid']]=row['name']
    
df=pd.read_sql_query('select uuid,name,parentUuid from TBL_OUTGOSUBTYPEINFO',conn)
outgosubtype={}
outgosubtomain={}
for _,row in df.iterrows():
    outgosubtype[row['uuid']]=row['name']
    outgosubtomain[row['uuid']]=row['parentUuid']
    
df=pd.read_sql_query('select uuid,name from TBL_INCOMEMAINTYPEINFO',conn)
incomemaintype={}
for _,row in df.iterrows():
    incomemaintype[row['uuid']]=row['name']
    
df=pd.read_sql_query('select uuid,name from TBL_BOOK',conn)
books={}
for _,row in df.iterrows():
    books[row['uuid']]=row['name']
    
df=pd.read_sql_query('select * from TBL_TRADEINFO where date>0 order by date',conn)

dd_1=[]
dd_2=[]
dd_3=[]

for _,row in df.iterrows():
    try:
        if row['isdelete']==1:
            continue
        
        book=books[row['bookUuid']]
        account=accounts[row['accountUuid']]
        pos=account.find('-')
        fee_type='人民币'
        if pos!=-1:
            fee_type=account[pos+1:]
            account=account[:pos]
        dd=datetime.fromtimestamp(row['date']).strftime('%Y-%m-%d %H:%M:%S')
        #print(book,account)

        tradetype=row['tradetype']
        if tradetype==1:
            # outcome
            maintyp=outgomaintype[outgosubtomain[row['typeUuid']]]
            subtyp=outgosubtype[row['typeUuid']]
            dd_1.append((maintyp, subtyp, account, fee_type, '日常', '', 
                  '非报销', dd, '%.2f'%(float(row['money'])/100),
                  '', row['comment'] or '', book))
        elif tradetype==2:
            # income
            typ=incomemaintype[row['typeUuid']]
            dd_2.append((typ, account, fee_type, '日常', '', 
                  dd, '%.2f'%(float(row['money'])/100),
                  '', row['comment'] or '', book))
        elif tradetype==3:
            account2=accounts[row['accountUuid2']]
            pos=account2.find('-')
            fee_type2='人民币'
            if pos!=-1:
                fee_type2=account2[pos+1:]
                account2=account2[:pos]
            dd_3.append((account, fee_type, '%.2f'%(float(row['money'])/100),
                   account2, fee_type2, '%.2f'%(float(row['money2'])/100), dd, row['comment'] or '', book))
        else:
            print(row)
    except Exception as e:
        print("exception: " + str(e))
        continue
    


df_1=pd.DataFrame(dd_1,columns=['支出大类','支出小类','账户','币种','项目','商家','报销','消费日期','消费金额','成员金额','备注','账本'])
df_2=pd.DataFrame(dd_2,columns=['收入大类','账户','币种','项目','付款方','收入日期','收入金额','成员金额','备注','账本'])
df_3=pd.DataFrame(dd_3,columns=['转出账户','币种','转出金额','转入账户','币种','转入金额','转账时间','备注','账本'])

writer = pd.ExcelWriter('out.xls')
df_1.to_excel(writer,sheet_name='支出',index=False)
df_2.to_excel(writer,sheet_name='收入',index=False)
df_3.to_excel(writer,sheet_name='转账',index=False)
writer.save()