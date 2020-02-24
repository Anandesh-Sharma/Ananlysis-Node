import logging
import pandas as pd
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pymongo import MongoClient
from tqdm import tqdm


def conn():
    connection = MongoClient(
        "mongodb://god:rock0004@13.67.79.22:27017/?authSource=admin&readPreference=primary&ssl=false",socketTimeoutMS=900000)
    return connection


def logger_1(name, user_id):
    logger = logging.getLogger('analysis_node ' + str(user_id) + "  " + name)
    logger.setLevel(logging.INFO)
    logHandler = TimedRotatingFileHandler(filename="analysis_node.log", when="midnight")
    logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logHandler.setFormatter(logFormatter)

    if not logger.handlers:
        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        streamhandler.setFormatter(formatter)
        logger.addHandler(streamhandler)
        logger.addHandler(logHandler)
    return logger


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
    max_timestamp = max(df['timestamp'])
    result = update_sms(df, user_id, max_timestamp)
    if not result['status']:
        return result
    if result['new']:
        return result
    df = result['df']
    df.reset_index(inplace=True, drop=True)
    return {'status': True, 'message': 'success', 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0',
            'df': df, "timestamp": max_timestamp, 'new': False}


def convert_json(data, name, max_timestamp):
    obj = {"_id": int(name), "timestamp": max_timestamp, "sms": []}
    for i in tqdm(range(data.shape[0])):
        sms = {"sender": data['sender'][i], "body": data['body'][i], "timestamp": data['timestamp'][i],
               "read": data['read'][i]}
        obj['sms'].append(sms)
    return obj


def update_sms(df, user_id, max_timestamp):
    try:
        client = conn()
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0',
                'df': df, "timestamp": max_timestamp}

    extra = client.messagecluster.extra
    msgs = extra.find_one({"_id": int(user_id)})
    client.close()
    if msgs is None:
        return {'status': True, 'message': 'success', 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0',
                'new': True, 'df': df, "timestamp": max_timestamp}
    old_timestamp = msgs["timestamp"]
    for i in range(df.shape[0]):
        if df['timestamp'][i] == old_timestamp:
            index = i + 1
    df = df.loc[index:]
    return {'status': True, 'message': 'success', 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0',
            'new': False, 'df': df, "timestamp": max_timestamp}