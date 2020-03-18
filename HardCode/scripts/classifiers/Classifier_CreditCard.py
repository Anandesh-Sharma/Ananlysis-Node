import re
from HardCode.scripts.Util import conn, convert_json, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def get_cc_messages(data, data_not_needed, result, name):
    logger = logger_1("cc messages", name)
    index_of_messages = []
    pattern = '(.*)?credit card(.*)?'
    for i in range(data.shape[0]):
        if i in data_not_needed:
            continue
        message = str(data['body'][i]).lower()
        matcher = re.search(pattern, message)

        if matcher is not None:
            index_of_messages.append(i)
    logger.info("Credit card sms extracted successfully")

    logger.info("appending name in result credit card dictionary")
    if name in result.keys():
        a = result[name]
        a.extend(list(index_of_messages))
        result[name] = a
    else:
        result[name] = list(index_of_messages)
    logger.info("Appended name in result credit card dictionary successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in index_of_messages:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than credit card")
    return data.copy()[mask].reset_index(drop=True)


def get_creditcard_promotion(data):
    credit_messages_filtered = []

    pattern_1 = r'congratulations'
    pattern_2 = r'sale'
    pattern_3 = r'voucher'
    pattern_4 = r'reward(.*)points'
    pattern_5 = r'discount'
    pattern_6 = r'rewarding'
    pattern_7 = r'off'
    pattern_8 = r'flat'
    pattern_9 = r'cashback'
    pattern_10 = r'offer'
    pattern_11 = r'offers'
    pattern_12 = r'w[o]?[i]?n'
    pattern_13 = r'features'
    pattern_14 = r'paperless(.*)?approval'
    pattern_15 = r'apply(.*)?now'
    pattern_16 = r'apply(.*)?for'
    pattern_17 = r'credit card approval'
    pattern_18 = r'apply(.*)?karein'
    pattern_19 = r'eligible(.*)?for membership'
    pattern_20 = r'to(.*)?apply'
    pattern_21 = r'instant(.*)?approval'
    pattern_22 = r'get your credit card'
    pattern_23 = r'free(.*)credit(.*)card'
    pattern_24 = r'congrats'
    pattern_25 = r'save.*up\s?to'
    pattern_26 = r'can\sbe\sapproved'
    pattern_27 = r'prevent\sfraud'
    pattern_28 = r'now\sget'
    pattern_29 = r'otp'

    for i in range(data.shape[0]):
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
        matcher_14 = re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)
        matcher_16 = re.search(pattern_16, message)
        matcher_17 = re.search(pattern_17, message)
        matcher_18 = re.search(pattern_18, message)
        matcher_19 = re.search(pattern_19, message)
        matcher_20 = re.search(pattern_20, message)
        matcher_21 = re.search(pattern_21, message)
        matcher_22 = re.search(pattern_22, message)
        matcher_23 = re.search(pattern_23, message)
        matcher_24 = re.search(pattern_24, message)
        matcher_25 = re.search(pattern_25, message)
        matcher_26 = re.search(pattern_26, message)
        matcher_27 = re.search(pattern_27, message)
        matcher_28 = re.search(pattern_28, message)
        matcher_29 = re.search(pattern_29, message)

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_4 \
                is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 \
                is not None or matcher_9 is not None or matcher_10 is not None or matcher_11 is not None or matcher_12 \
                is not None or matcher_13 is not None or matcher_14 is not None or matcher_15 is not None or matcher_16 \
                is not None or matcher_17 is not None or matcher_18 is not None or matcher_19 is not None or \
                matcher_20 is not None or matcher_21 is not None or matcher_22 is not None or matcher_23 is not None or \
                matcher_24 is not None or matcher_25 is not None or matcher_26 is not None or matcher_27 is not None or \
                matcher_28 is not None or matcher_29 is not None:
            pass
        else:
            credit_messages_filtered.append(i)
    return credit_messages_filtered


def credit(df, result, user_id, max_timestamp, new):
    logger = logger_1("credit card", user_id)
    logger.info("Removing credit card promotional sms")
    data_not_needed = get_creditcard_promotion(df)
    logger.info("Extracting Credit card sms")
    data = get_cc_messages(df, data_not_needed, result, user_id)
    logger.info("Converting credit card dataframe into json")
    data_credit = convert_json(data, user_id, max_timestamp)

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
        # db.creditcard.insert_one(data_credit)
        db.creditcard.update({"cust_id": int(user_id)}, {"cust_id": int(user_id), "sms": data_credit['sms'],
                                                         'modified_at': str(
                                                             datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                         "timestamp": data_credit['timestamp']}, upsert=True)
        logger.info("Credit card sms of new user inserted successfully")
    else:
        for i in range(len(data_credit['sms'])):
            logger.info("Old User checked")
            db.creditcard.update({"cust_id": int(user_id)}, {"$push": {"sms": data_credit['sms'][i]}})
            logger.info("Credit card sms of old user updated successfully")
        db.creditcard.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                 upsert=True)
        logger.info("Timestamp of User updated")
    client.close()
