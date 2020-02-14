# import json
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm


def conn():
    connection = MongoClient(
        "mongodb://superadmin:rock0004@13.76.177.87:27017/?authSource=admin&readPreference=primary&ssl=false")
    return connection


def read_json(sms_json, user_id):
    try:
        df = pd.DataFrame.from_dict(sms_json, orient='index')
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0'}
    df['timestamp'] = [0] * df.shape[0]
    df['temp'] = df.index
    df.reset_index(drop=True, inplace=True)
    try:
        for i in range(df.shape[0]):
            df['timestamp'][i] = datetime.utcfromtimestamp(int(df['temp'][i]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0'}
    df.reset_index(inplace=True, drop=True)
    list_idx = []
    for i in range(df.shape[0]):
        if df['body'][i] == 'null':
            list_idx.append(i)
    df.drop(list_idx, inplace=True)
    df = df.sort_values(by=['timestamp'])
    df.reset_index(inplace=True, drop=True)
    columns_titles = ['body', 'timestamp', 'sender', 'read']
    df = df.reindex(columns=columns_titles)
    return {'status': True, 'message': 'success', 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0',
            'df': df}


def convert_json(data, name):
    obj = {"_id": int(name), "sms": []}
    for i in tqdm(range(data.shape[0])):
        sms = {"sender": data['sender'][i], "body": data['body'][i], "timestamp": data['timestamp'][i],
               "read": data['read'][i]}
        obj['sms'].append(sms)
    return obj
