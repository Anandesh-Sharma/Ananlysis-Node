# test_credit_card

import re
from HardCode.scripts.Util import conn, convert_json, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def get_confirm_cc_messages(data):
    cc_confirm_index_list = []
    all_patterns = [
        r'cardholder.*payment.*rs\.?\s?([0-9,]+[.]?[0-9]+).*credit\scard.*successfully',
        r'approve\stransaction.*rs\.?\s?([0-9,]+[.]?[0-9]+).*a/c\sno\.?.*credit\scard',
        r'(?:rs|inr)\.?\s?\s?([0-9,]+[.]?[0-9]+).*debited.*credit\scard',
        r'inr\s?([0-9,]+[.]?[0-9]+).*paytm.*credit\scard',
        r'txn\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*credit\scard',
        r'refund.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*credited.*credit\scard',
        r'spent\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*credit\scard',
        r'payment.*(?:rs\.?|inr)\s?\s?([0-9,]+[.]?[0-9]+).*received',
        r'received.*payment.*(?:for|of)*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*,
        r'(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*spent.*card.*(?:available|avl\.?).*(?:limit|lim\.?).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*',
        r'.*charge\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*initiated.*credit\scard.*',
        r'.*internet\spayment.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*credit\scard.*',
        r'e-stmt.*card.*total\samt\sdue:\srs\.?\s?([0-9,]+[.]?[0-9]+).*min\samt\sdue:\srs\.?\s?([0-9,]+[.]?[0-9]+)\sis\spayable',
        r'payment.*credit\scard.*is\sdue.*total\samount\s(?:due|overdue:).*(?:rs|\s)\.?\s?([0-9,]+[.]?[0-9]+).*minimum\samount\s(?:due|due:).*(?:rs|\s)\.?\s?([0-9,]+[.]?[0-9]+)',
        r'stmt.*total\s(?:amt|amount)\sdue.*credit\scard.*(?:inr|rs\.?)\s?([0-9.,?]+).*(?:minimum|min)\s(?:amt|amount)\sdue.*(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*payable',
        r'.*(?:statement|stmt).*credit\scard.*total\s(?:amount|amt).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*min.*('
        r'?:amount|amt).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*due.*',
        r'.*total\samount\sdue.*credit\scard.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*',
        r'.*payment.*credit\scard.*due.*(?:minimum|min).*rs\.?\s?\s?([0-9,]+[.]?[0-9]+).*total.*rs\.?\s?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*forward.*receiving\s?rs\.?\s?([0-9,]+[.]?[0-9]+).*credit\scard.*',
        r'.*credit\scard.*payment.*rs\.?\s?([0-9,]+[.]?[0-9]+).*due.*min.*rs\.?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*credit\scard.*(?:statement|stmt).*rs\.?\s?([0-9,]+[.]?[0-9]+).*due.*min.*rs\.?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*payment.*credit\scard.*due.*(?:minimum|min).*rs\.?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*not\sreceived\spayment.*credit\scard.*rs\.?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*necessary.*payment.*rs\.?\s?([0-9,]+[.]?[0-9]+).*credit\scard.*',
        r'.*credit\scard\sdues.*unpaid.*rs\.?\s?([0-9,]+[.]?[0-9]+).*',
        r'unable.*overdue\s(?:payment|pymt).*rs\.?\s?([0-9,]+[.]?[0-9]+).*credit\scard',
        r'.*payment.*overdue.*credit\scard.*(?:pl|please|pls)\spay.*total\s(?:amt|amount).*due.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*min.*(?:amt|amount).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*',
        r'.*overdue\samount.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*credit\scard.*',
        r'.*payment.*credit\scard.*is\s(due|overdue).*total\samount\s(?:due|overdue:|outstanding).*(?:rs)\.?\s?\s?([0-9,]+[.]?[0-9]+).*minimum\samount\s(?:due|due:).*(?:rs)\.?\s?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*account.*rs\.?\s?([0-9,]+[.]?[0-9]+).*overdue.*credit\scard.*',
        r'.*credit\scard.*rs\.?\s?\s?([0-9,]+[.]?[0-9]+).*overdue.*minimum.*(?:due|payment).*rs\.?\s?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*repeated\sreminders.*credit\scard.*overdue.*pay\.?\s?\s?([0-9,]+[.]?[0-9]+).*immediately.*',
        r'regret\sto\sinform.*unable\sto\s(?:issue|sanction).*credit\scard',
        r'application.*credit\scard[s]?.*(?:reject[e]?[d]?|declined)',
        r'regret\sto\sinform.*review[e]?[d]?.*application.*unable\sto\sgrant.*credit\scard',
        r'txn.*credit\scard.*(?:rs\.?|inr)\s?([0-9.?]+).*declined',
        r'.*(?:transaction|trxn|txn).*credit\scard.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*not\sapprove[d]?.*',
        r'.*(?:txn|trxn).*rs\.?\s?([0-9,]+[.]?[0-9]+).*credit\scard.*.*declined.*',
        r'.*credit\scard.*blocked.*total.*rs\.?\s?([0-9,]+[.]?[0-9]+).*minimum.*rs\.?\s?([0-9,]+[.]?[0-9]+).*',
        r'.*credit\scard.*blocked.*immediate.*',
        r'request\sto\sincrease.*credit\slimit.*initiated',
        r'convert.*(?:transaction|trxn|txn)\sof\s(?:rs\.?|inr)\s?([0-9]+[.]?[0-9]+).*into.*emi[s]?',
        r'transfer.*outstanding\scredit\scard.*personal\sloan',
    ]
    cc_list = []
    mask_needed = []
    credit_card_patterns = ["credit card", "sbi card", "rbl supercard"]
    for i,row in data.iterrows():
        message = str(row['body']).lower()
        k =False
        for pattern in credit_card_patterns:
            matcher = re.search(pattern, message)
            if matcher:
                for pattern in all_patterns:
                    matcher = re.search(pattern, message)
                    if matcher:
                        mask_needed.append(True)
                        k = True
                        cc_confirm_index_list.append(i)
                        break
            if k:
                break
        if not k:
            mask_needed.append(False)
    return cc_confirm_index_list,mask_needed

def credit(df, user_id, max_timestamp, new):
    logger = logger_1("credit card", user_id)
    # logger.info("Removing credit card promotional sms")
    # data_not_needed = get_creditcard_promotion(df)
    logger.info("Extracting Credit card sms")
    data_needed,mask_needed = get_confirm_cc_messages(df)
    
    data = df.copy()[mask_needed].reset_index(drop=True)
    logger.info("Converting credit card dataframe into json")
    data_credit = convert_json(data, user_id, max_timestamp)

    logger.info('making connection with db')
    client = conn()
    db = client.messagecluster
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
    return df.drop(data_needed).reset_index(drop=True)
