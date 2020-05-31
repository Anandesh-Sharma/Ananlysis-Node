import re
import pandas as pd
from HardCode.scripts.Util import conn,logger_1,convert_json
from datetime import datetime
import pytz


def chq_bounce(df,user_id,result):
    logger = logger_1("Cheque_bounce_classifier", user_id)
    logger.info("Cheque bounce started")

    chq_bounce_list = []
    mask = []
    selected = []
    pattern_1 = r'auto(?:\-|\s)debit\sattempt\sfailed.*cheque\s(?:bounce[d]?|dishono[u]?r)\scharge[s]?'
    pattern_2 = r'cheque.*(?:dishono[u]?red|bounce[d]?).*insufficient\s(?:balance|fund[s]?|bal)'


    for i in range(df.shape[0]):
        message = str(df['body'][i].encode('utf-8')).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)

        if matcher_1 or matcher_2:
            chq_bounce_list.append(i)
            mask.append(True)
        else:
            mask.append(False)

    if user_id in result.keys():
        a = result[user_id]
        a.extend(list(selected))
        result[user_id] = a
    else:
        result[user_id] = list(selected)

    return df.copy()[mask].reset_index(drop = True)

def Cheque_Classifier(df, result, user_id, max_timestamp, new):
    logger = logger_1("Cheque bounce function", user_id)
    logger.info("Cheque bounce function started")

    chq_messages = chq_bounce(df = df, result = result , user_id = user_id)

    if chq_messages.empty:
        chq_messages = pd.DataFrame(columns = ['body', 'timestamp', 'sender', 'read'])


    data = convert_json(chq_messages,user_id,max_timestamp)

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
        db.cheque_bounce_msgs.update({"cust_id": int(user_id)},{"cust_id": int(user_id), 'timestamp': data['timestamp'],
                                                    'modified_at': str(
                                                    datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                    "sms": data['sms']} , upsert=True)
        logger.info("All cheque bounce messages of new user inserted successfully")
    else:
        logger.info("Old User checked")
        for i in range(len(data['sms'])):
            db.cheque_bounce_msgs.update({"cust_id": int(user_id)}, {"$push": {"sms": data['sms'][i]}})
            logger.info("cheque bounce sms of old user updated successfully")
        db.cheque_bounce_msgs.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                   upsert=True)
        logger.info("Timestamp of User updated")
    client.close()