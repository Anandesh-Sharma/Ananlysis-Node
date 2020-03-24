import re
from HardCode.scripts.Util import conn, convert_json, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def get_cc_messages(data, data_needed, result, name):
    logger = logger_1("cc messages", name)
    index_of_messages = []
    pattern = '.*credit card.*'
    for i in range(data.shape[0]):
        if i in data_needed:
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

'''
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
'''

def get_confirm_cc_messages(data):
    cc_confirm_index_list = []
    pattern_1 = r'sbi\scardholder.*payment.*rs\.?\s?([0-9.?]+).*credit\scard.*successfully'   # grp(1) for amount
    pattern_6 = r'approve\stransaction.*rs\.?\s?([0-9.?]+).*a/c\sno\.?.*credit\scard'
    pattern_7 = r'(?:rs|inr)\.?\s?\s?([0-9.?]+).*debited.*credit\scard'
    pattern_9 = r'inr\s?([0-9.?]+).*paytm.*credit\scard'
    pattern_11 = r'txn\sof\s(?:inr|rs\.?)\s?([0-9.?]+).*credit\scard'
    pattern_12 = r'refund.*(?:rs\.?|inr)\s?([0-9.?]+).*credited.*credit\scard'
    pattern_13 = r'spent\s(?:rs\.?|inr)\s?([0-9.?]+).*credit\scard'
    pattern_15 = r'payment\sof\s(?:inr|rs\.?)\s?([0-9.?]+).*received.*credit\scard'
    pattern_17 = r'received.*payment.*(?:for|of)*(?:rs\.?|inr)\s?([0-9.?]+).*credit\scard'
    # will try to make 15 and 17 a single regex
    pattern_19 = r'.*charge\sof\s(?:rs\.?|inr)\s?([0-9.?]+).*initiated.*credit\scard.*'
    pattern_20 = r'.*internet\spayment.*(?:rs\.?|inr)\s?([0-9.,?]+).*credit\scard.*'
    
    # due
    pattern_2 = r'e-stmt.*sbi\scard.*total\samt\sdue:\srs\.?\s?([0-9.?]+).*min\samt\sdue:\srs\.?\s?([0-9.?]+)\sis\spayable'  # grp(1) for total amt grp(2) for min amt
    pattern_14 = r'payment.*credit\scard.*is\sdue.*total\samount\s(?:due|overdue:).*(?:rs|\s)\.?\s?([0-9.?]+).*minimum\samount\s(?:due|due:).*(?:rs|\s)\.?\s?([0-9.?]+)'   # grp(1) for total grp(2) for min
    pattern_16 = r'stmt.*total\s(?:amt|amount)\sdue.*credit\scard.*(?:inr|rs\.?)\s?([0-9.,?]+).*(?:minimum|min)\s(?:amt|amount)\sdue.*(?:inr|rs\.?)\s?([0-9.,?]+).*payable'
    # will try to make 14 and 16 a single regex 
    pattern_21 = r'.*(?:statement|stmt).*credit\scard.*total\s(?:amount|amt).*(?:rs\.?|inr)\s?([0-9.,?]+).*min.*(?:amount|amt).*(?:rs\.?|inr)\s?([0-9.,?]+).*due.*'
    pattern_23 = r'.*total\samount\sdue.*credit\scard.*(?:rs\.?|inr)\s?([0-9.,?]+).*'
    pattern_29 = r'.*payment.*credit\scard.*due.*(?:minimum|min).*rs\.?\s?\s?([0-9.,?]+).*total.*rs\.?\s?\s?([0-9.,?]+).*'
    pattern_30 = r'.*forward.*receiving\s?rs\.?\s?([0-9.,?]+).*credit\scard.*'
    pattern_32 = r'.*credit\scard.*payment.*rs\.?\s?([0-9.,?]+).*due.*min.*rs\.?\s?([0-9.,?]+).*'
    pattern_34 = r'.*credit\scard.*(?:statement|stmt).*rs\.?\s?([0-9.,?]+).*due.*min.*rs\.?\s?([0-9.,?]+).*'
    pattern_35 = r'.*payment.*credit\scard.*due.*(?:minimum|min).*rs\.?\s?([0-9]+[.,]?).*'
    pattern_36 = r'.*not\sreceived\spayment.*credit\scard.*rs\.?\s?([0-9]+).*'
    pattern_37 = r'.*necessary.*payment.*rs\.?\s?([0-9]+[.,]?).*credit\scard.*'
    pattern_38 = r'.*credit\scard\sdues.*unpaid.*rs\.?\s?([0-9]+[.,]?).*'
    
    # overdue
    pattern_3 = r'unable.*overdue\s(?:payment|pymt).*rs\.?\s?([0-9.?]+).*credit\scard'   # grp(1) for overdue amt 
    pattern_22 = r'.*payment.*overdue.*credit\scard.*(?:pl|please|pls)\spay.*total\s(?:amt|amount).*due.*(?:rs\.?|inr)\s?([0-9.,?]+).*min.*(?:amt|amount).*(?:rs\.?|inr)\s?([0-9.,?]+).*'
    pattern_24 = r'.*overdue\samount.*(?:rs\.?|inr)\s?([0-9.,?]+).*credit\scard.*'
    pattern_25 = r'.*payment.*credit\scard.*is\s(due|overdue).*total\samount\s(?:due|overdue:|outstanding).*(?:rs)\.?\s?\s?([0-9.?]+).*minimum\samount\s(?:due|due:).*(?:rs)\.?\s?\s?([0-9.?]+).*'
    # will try to make 14, 16 and 25 a single regex
    pattern_26 = r'.*account.*rs\.?\s?([0-9.,?]+).*overdue.*credit\scard.*'
    pattern_27 = r'.*credit\scard.*rs\.?\s?\s?([0-9.,?]+).*overdue.*minimum.*(?:due|payment).*rs\.?\s?\s?([0-9.,?]+).*'
    pattern_39 = r'.*repeated\sreminders.*credit\scard.*overdue.*pay\.?\s?\s?([0-9]+[.,]?).*immediately.*'

    # reject/declined
    pattern_4 = r'regret\sto\sinform.*unable\sto\s(?:issue|sanction).*credit\scard'
    pattern_5 = r'application.*credit\scard[s]?.*(?:reject[e]?[d]?|declined)'
    pattern_8 = r'regret\sto\sinform.*review[e]?[d]?.*application.*unable\sto\sgrant.*credit\scard'
    pattern_10 = r'txn.*credit\scard.*(?:rs\.?|inr)\s?([0-9.?]+).*declined'
    pattern_18 = r'.*(?:transaction|trxn|txn).*credit\scard.*(?:rs\.?|inr)\s?([0-9.?]+).*not\sapprove[d]?.*'
    pattern_28 = r'.*(?:txn|trxn).*rs\.?\s?([0-9.,?]+).*credit\scard.*.*declined.*'

    # blocked
    pattern_31 = r'.*credit\scard.*blocked.*total.*rs\.?\s?([0-9.,?]+).*minimum.*rs\.?\s?([0-9.,?]+).*'
    pattern_33 = r'.*credit\scard.*blocked.*immediate.*'

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
        matcher_30 = re.search(pattern_30, message)
        matcher_31 = re.search(pattern_31, message)
        matcher_32 = re.search(pattern_32, message)
        matcher_33 = re.search(pattern_33, message)
        matcher_34 = re.search(pattern_34, message)
        matcher_35 = re.search(pattern_35, message)
        matcher_36 = re.search(pattern_36, message)
        matcher_37 = re.search(pattern_37, message)
        matcher_38 = re.search(pattern_38, message)
        matcher_39 = re.search(pattern_39, message)

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_5 is not None\
        or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None or matcher_9 is not None or matcher_10 is not None\
        or matcher_11 is not None or matcher_12 is not None or matcher_13 is not None or matcher_14 is not None or matcher_15 is not None\
        or matcher_16 is not None or matcher_17 is not None or matcher_18 is not None or matcher_19 is not None or matcher_20 is not None\
        or matcher_21 is not None or matcher_22 is not None or matcher_23 is not None or matcher_24 is not None or matcher_25 is not None\
        or matcher_26 is not None or matcher_27 is not None or matcher_28 is not None or matcher_29 is not None or matcher_30 is not None\
        or matcher_31 is not None or matcher_32 is not None or matcher_33 is not None or matcher_34 is not None or matcher_35 is not None\
        or matcher_36 is not None or matcher_37 is not None or matcher_38 is not None or matcher_39 is not None:
            cc_confirm_index_list.append(i)

    return cc_confirm_index_list


def credit(df, result, user_id, max_timestamp, new):
    logger = logger_1("credit card", user_id)
    #logger.info("Removing credit card promotional sms")
    #data_not_needed = get_creditcard_promotion(df)
    logger.info("Extracting Credit card sms")
    data_needed = get_confirm_cc_messages(df)
    data = get_cc_messages(df, data_needed, result, user_id)
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
