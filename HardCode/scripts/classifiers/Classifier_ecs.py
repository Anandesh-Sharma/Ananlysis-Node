import re
import pandas as pd
from HardCode.scripts.Util import conn,logger_1,convert_json
from datetime import datetime
import pytz


def ecs_bounce(df,user_id,result):

    ecs_bounce_list = []
    mask = []
    selected = []
    patterns = [
        r'ecs\sbounce\sho\schuka\shai'
        r'ecs\s(?:transaction|request).*(rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*returned.*insufficient\s(?:balance|fund[s]?)'
        r'unable\sto\sprocess.*ecs\srequest.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*insufficient\s(?:balance|fund[s]?)'
        r'(?:emi|payment|paymt|paymnt|ecs).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:has|is)\s(?:bounce[d]?|dishono[u]?red)'
        r'(?:emi|payment|paymt|paymnt|ecs).*(?:is|has)\s(?:dishono[u]?red|bounced)'
        r'ecs.*dishono[u]?red.*(?:due\sto|because\sof)\sinsufficient\s(?:balance|fund[s]?|bal)'
        r'nach\s(?:payment|paymt|paymnt).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\s(?:has|is)\sbeen?\s(?:bounced|dishono[u]?red)'
        r'(?:emi|payment|paymnt|paymt|ecs)\s.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*has\sbeen\sdishono[u]?red.*is\soverdue',
        r'your\s(?:nach|ecs)\s?(payment)?\swas\sunsuccessful',
        r'repayment.*not\ssuccessful\sthrough.*auto\s?\-?debit\sfacility'
    ]


    for i in range(df.shape[0]):
        message = str(df['body'][i].encode('utf-8')).lower()
        for pattern in patterns:
            matcher = re.search(pattern, message)
            if matcher:
                ecs_bounce_list.append(i)
                mask.append(True)
                break
        else:
            mask.append(False)

    # if user_id in result.keys():
    #     a = result[user_id]
    #     a.extend(list(selected))
    #     result[user_id] = a
    # else:
    #     result[user_id] = list(selected)

    return df.copy()[mask].reset_index(drop = True),df.drop(selected).reset_index(drop=True)

def Ecs_Classifier(df, result, user_id, max_timestamp, new):
    logger = logger_1("Ecs function", user_id)
    logger.info("Ecs function started")

    ecs_messages,df = ecs_bounce(df, result, user_id)

    if ecs_messages.empty:
        ecs_messages = pd.DataFrame(columns = ['body', 'timestamp', 'sender', 'read'])


    data = convert_json(ecs_messages,user_id,max_timestamp)

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
        db.ecs_msgs.update({"cust_id": int(user_id)},{"cust_id": int(user_id), 'timestamp': data['timestamp'],
                                                    'modified_at': str(
                                                    datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                    "sms": data['sms']} , upsert=True)
        logger.info("All ecs messages of new user inserted successfully")
    else:
        logger.info("Old User checked")
        for i in range(len(data['sms'])):
            db.legal_msgs.update({"cust_id": int(user_id)}, {"$push": {"sms": data['sms'][i]}})
            logger.info("ecs sms of old user updated successfully")
        db.legal_msgs.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                   upsert=True)
        logger.info("Timestamp of User updated")
    client.close()