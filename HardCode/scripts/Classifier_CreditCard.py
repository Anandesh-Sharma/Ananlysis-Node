import re
from tqdm import tqdm

from .Util import conn, read_json, convert_json


def get_cc_messages(data, data_not_needed, result, name):
    index_of_messages = []
    pattern = '(.*)?credit card(.*)?'
    for i in tqdm(range(data.shape[0])):
        if i in data_not_needed:
            continue
        message = str(data['body'][i]).lower()
        matcher = re.search(pattern, message)

        if matcher is not None:
            index_of_messages.append(i)

    if name in result.keys():
        a = result[name]
        a.extend(list(index_of_messages))
        result[name] = a
    else:
        result[name] = list(index_of_messages)

    mask = []
    for i in range(data.shape[0]):
        if i in index_of_messages:
            mask.append(True)
        else:
            mask.append(False)

    return data.copy()[mask].reset_index(drop=True)


def get_creditcard_promotion(data):
    credit_messages_filtered = []

    pattern_1 = 'congratulations'
    pattern_2 = 'sale'
    pattern_3 = 'voucher'
    pattern_4 = 'reward(.*)points'
    pattern_5 = 'discount'
    pattern_6 = 'rewarding'
    pattern_7 = 'off'
    pattern_8 = 'flat'
    pattern_9 = 'cashback'
    pattern_10 = 'offer'
    pattern_11 = 'offers'
    pattern_12 = 'won'
    pattern_13 = 'features'
    for i in tqdm(range(len(data['body']))):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13, message)

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_4 is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None or matcher_9 is not None or matcher_10 is not None or matcher_11 is not None or matcher_12 is not None or matcher_13 != None:
            credit_messages_filtered.append(i)
    return credit_messages_filtered


def credit(df, result, user_id, max_timestamp, new):
    data_not_needed = get_creditcard_promotion(df)
    data = get_cc_messages(df, data_not_needed, result, user_id)
    data_credit = convert_json(data, user_id, max_timestamp)

    try:
        client = conn()
        db = client.messagecluster
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    if new:
        db.creditcard.insert_one(data_credit)
    else:
        for i in range(len(data_credit['sms'])):
            db.creditcard.update({"_id": int(user_id)}, {"$push": {"sms": data_credit['sms'][i]}})
        db.creditcard.update_one({"_id": int(user_id)}, {"$set": {"timestamp": max_timestamp}}, upsert=True)
    client.close()
