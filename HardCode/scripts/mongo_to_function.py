# -*- coding: utf-8 -*-
"""mongodb_data_fetcher

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ednjCaAMAxaSfm9nD1-c2KGbuyvrDBAg
"""

import json
import pandas as pd
import pprint
import pymongo
import regex as re
from pymongo import MongoClient

client = MongoClient(
    "mongodb://superadmin:rock0004@13.76.177.87:27017/?authSource=admin&readPreference=primary&ssl=false")
transaction = client.messagecluster1.transaction
extra = client.messagecluster1.extra


def customer_salary(id1):
    file1 = transaction.find_one({"_id": id1})
    x = pd.DataFrame(file1)
    df1 = pd.DataFrame()
    full = pd.DataFrame()
    for i in range(x.shape[0]):
        # print(x['sms'][i])
        p = pd.DataFrame(x['sms'][i], index=[0])
        df1 = pd.concat([df1, p], axis=0)
    df1 = df1.reset_index(drop=True)
    full = pd.concat([x, df1], axis=1)
    full = full.drop(["sms"], 1)

    file2 = extra.find_one({"_id": id1})
    y = pd.DataFrame(file2)
    df2 = pd.DataFrame()
    full2 = pd.DataFrame()
    for i in range(y.shape[0]):
        # print(x['sms'][i])
        p = pd.DataFrame(y['sms'][i], index=[0])
        df2 = pd.concat([df2, p], axis=0)
    df2 = df2.reset_index(drop=True)
    full2 = pd.concat([y, df2], axis=1)
    full2 = full2.drop(["sms"], 1)

    epf = []
    for i in range(full2.shape[0]):
        if re.search("EPFOHO", full2["sender"][i]):
            epf.append(full2.values[i])
    epf = pd.DataFrame(epf, columns=['_id', 'sender', 'body', 'timestamp', 'read'])
    total = pd.concat([full, epf], 0)
    total = total.reset_index(drop=True)

    salary = salary_check(total)
    return salary
