from HardCode.scripts.Util import conn, logger_1
from HardCode.scripts.balance_sheet_analysis.transaction_analysis import process_data
import warnings
import re
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")

def epf(df, result, user_id):
    logger = logger_1("epf_classifier", user_id)
    logger.info("epf started")

    selected = []
    mask = []

    logger.info("loop started")
    for i,row in df.iterrows():
        sender = row['sender'].lower()
        if "epfoho" in sender:
            mask.append(True)
            selected.append(i)
        else:
            mask.append(False)

    if user_id in result.keys():
        a = result[user_id]
        a.extend(list(selected))
        result[user_id] = a
    else:
        result[user_id] = list(selected)

    logger.info("loop ended")
    return df[mask].reset_index(drop=True),df.drop(selected).reset_index(drop=True)

def convert_json(epf,salary,cust_id,timestamp):
    obj = {"cust_id": int(cust_id), "timestamp": timestamp, "epf": [],"deposited":[]}

    for i in range(epf.shape[0]):
        sms = {"sender": epf['sender'][i], "body": epf['body'][i], "timestamp": epf['timestamp'][i],
               "read": epf['read'][i]}
        obj['epf'].append(sms)
    data = salary
    for i in range(data.shape[0]):
        sms = {"sender": data['sender'][i], "body": data['body'][i], "timestamp": str(data['timestamp'][i]),
            "read": data['read'][i], "time_message": str(data['time,message'][i]), "acc_no": int(data['acc_no'][i]),
            "VPA": str(data['VPA'][i]), "IMPS Ref no": str(data["IMPS Ref no"][i]),
            'UPI Ref no': int(data['UPI Ref no'][i]),
            'neft': int(data['neft'][i]), 'Neft no': str(data['neft no'][i]),
            'Credit Amount': float(data['credit_amount'][i]),
            'Debit Amount': float(data['debit_amount'][i]), 'UPI': int(data['upi'][i]),
            'Date Time': str(data['date_time'][i]),
            'Date Message': str(data['date,message'][i]), 'IMPS': int(data['imps'][i]),
            'Available Balance': float(data['available balance'][i])}
        obj['deposited'].append(sms)

    return obj

def deposited_keyword(df, result, user_id):
    logger = logger_1("deposited keyword function", user_id)
    logger.info("deposited keyword function started")

    selected = []
    mask = []

    logger.info("loop started")
    for i,row in df.iterrows():
        body = row['body'].lower()
        if "deposited" in body:
            if "salary" in body:
                mask.append(True)
                selected.append(i)
                continue
        mask.append(False)

    if user_id in result.keys():
        a = result[user_id]
        a.extend(list(selected))
        result[user_id] = a
    else:
        result[user_id] = list(selected)

    logger.info("loop ended")
    return df[mask].reset_index(drop=True),df.drop(selected).reset_index(drop=True)

def salary(df, result, user_id, max_timestamp, new):
    logger = logger_1("Salary function", user_id)
    logger.info("Salary function started")

    epf_messages,df = epf(df, result, user_id)

    deposited_messages,df = deposited_keyword(df, result, user_id)

    deposited_messages = process_data(deposited_messages, user_id)

    data = convert_json(epf_messages,deposited_messages['df'],user_id,max_timestamp)

    try:
        logger.info('making connection with db')
        client = conn()
        db = client.messagecluster
    except Exception as e:
        logger.critical('error in connection')
        return {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    logger.info('connection success')

    if new:
        logger.info("New user checked")
        db.salary.update({"cust_id": int(user_id)},{"cust_id": int(user_id), 'timestamp': data['timestamp'],
                                                    'modified_at': str(
                                                    datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                    "epf": data['epf'],"deposited":data['deposited']} , upsert=True)
        logger.info("All salary messages of new user inserted successfully")
    else:
        logger.info("Old User checked")
        for i in range(len(data['deposited'])):
            db.salary.update({"cust_id": int(user_id)}, {"$push": {"deposited": data['deposited'][i],"epf": data['epf'][i]}})
            logger.info("loan approval sms of old user updated successfully")
        db.salary.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                   upsert=True)
        logger.info("Timestamp of User updated")
    client.close()
