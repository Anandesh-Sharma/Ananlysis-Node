import re
from HardCode.scripts.Util import conn, convert_json, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def get_loan_closed_messages(data, loan_messages_filtered, result, name):
    logger = logger_1("loan closed messages", name)
    selected_rows = []
    pattern_1 = r'(.*)?loan(.*)?closed(.*)?'
    pattern_2 = r'(.*)?closed(.*)?successfully(.*)?'
    pattern_3 = r'successfully\sreceived\spayment'
    pattern_4 = r'loan.*?paid\sback'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue

        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None:
            selected_rows.append(i)
    logger.info("Loan closed sms extracted successfully")

    logger.info("Append name in result dictionary for loan closed")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan closed successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan closed")
    return data.copy()[mask].reset_index(drop=True)

def replace_parenthesis(message):
    # this was done as in some cases cmount was written in a bracket due to which garbage value was detected by the regex
    message = message.replace('(', '')
    message = message.replace(')', '')
    return message

def get_loan_messages(data):
    loan_messages = []
    pattern_1 = '(.*)?loan(.*)?'
    pattern_2 = 'kreditbee'
    pattern_3 = 'cashbean'
    pattern_4 = 'loanfront'
    pattern_5 = 'loanapp'
    pattern_6 = 'kissht'
    pattern_7 = 'gotocash'
    pattern_8 = 'cashmama'

    data['body'] = data['body'].apply(lambda m: replace_parenthesis(m) )
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

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None:
            loan_messages.append(i)
    return loan_messages


def get_loan_messages_promotional_removed(data, loan_messages):
    loan_messages_filtered = []

    pattern_1 = r'(.*)?apply(.*)?'
    pattern_2 = r'(.*)?offer(.*)?'
    pattern_3 = r'(.*)?offers(.*)?'
    pattern_4 = r'(.*)?avail(.*)?'
    pattern_5 = r'(.*)?instant(.*)?'
    pattern_6 = r'(.*)?instantly(.*)?'
    pattern_7 = r'(.*)?cashback(.*)?'
    pattern_8 = r'(.*)?voucher(.*)?'
    pattern_9 = r'(.*)?discount(.*)?'
    pattern_10 = r'(.*)?hurry(.*)?'
    pattern_11 = r'(.*)?get(.*)loan(.*)?'

    for i in range(data.shape[0]):
        if i not in loan_messages:
            continue
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

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_4 is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None or matcher_9 is not None or matcher_10 is not None or matcher_11 is not None:
            pass
        else:
            loan_messages_filtered.append(i)


    return loan_messages_filtered


def get_approval(data, loan_messages_filtered, result, name):
    logger = logger_1("loan approval", name)
    selected_rows = []
    pattern_1 = r'[^pre-]approved(.*)?'
    pattern_2 = r'succesfully(.*)?approved'
    pattern_3 = r'(.*)?has(.*)?been(.*)?approved'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)

        if matcher_1 != None or matcher_2 != None or matcher_3 != None:
            selected_rows.append(i)
    logger.info("Loan approval sms extracted successfully")

    logger.info("Append name in result dictionary for loan approval")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan approval successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan approval")
    return data.copy()[mask].reset_index(drop=True)


def get_disbursed(data, loan_messages_filtered, result, name):
    logger = logger_1("loan disbursed", name)
    selected_rows = []
    pattern_1 = r'has\sbeen\sdisburse[d]?'
    pattern_2 = r'disbursement\shas\sbeen\scredited'
    pattern_3 = r'has\sbeen\stransferred.*account'
    pattern_4 = r'disbursement.*has\sbeen \sinitiated'
    pattern_5 = r'is\stransferred.*account'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)

        if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None or matcher_5 != None:
            selected_rows.append(i)
    logger.info("Loan disbursed sms extracted successfully")

    logger.info("Append name in result dictionary for loan disbursed")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan disbursed successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan disbursed")
    return data.copy()[mask].reset_index(drop=True)


def get_loan_rejected_messages(data, loan_messages_filtered, result, name):
    logger = logger_1("loan rejection", name)
    selected_rows = []
    pattern_1 = r'(.*)?rejected(.*)?'
    pattern_2 = r'(.*)?reject(.*)?'
    pattern_3 = r'Declined[^\?]'
    pattern_4 = r'(.*)?decline(.*)?'
    pattern_5 = r'(.*)?not-approved(.*)?'
    pattern_6 = r'(.*)?low cibil score(.*)?'
    pattern_7 = r'low credit score'
    pattern_8 = r'declined\?'
    pattern_9 = r'not.*?approved'
    pattern_10 = r'.*regret.*'
    pattern_11 = r'application.*closed.*'
    pattern_12 = r'.*application.*re-apply.*'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
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

        if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None or matcher_9 != None or matcher_10 != None or matcher_11 != None or matcher_12 != None:
            if matcher_6 == None and matcher_7 == None and matcher_8 == None and matcher_5 == None:
                selected_rows.append(i)
    logger.info("Loan rejection sms extracted successfully")

    logger.info("Append name in result dictionary for loan rejction")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan rejection successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan rejection")
    return data.copy()[mask].reset_index(drop=True)


def get_over_due(data, loan_messages_filtered, result, name):
    logger = logger_1("loan due overdue", name)
    selected_rows = []
    pattern_1 = r'(.*)?immediate(.*)payment(.*)'
    pattern_2 = r'(.*)?delinquent(.*)?'
    # pattern_3 = r'(.*)?has(.*)?bounced(.*)?'
    pattern_4 = r'missed(.*)?payments'
    pattern_5 = r'(.*)?due(.*)?'
    pattern_6 = r'\sover-?due\s'
    # overdue
    pattern_7 = r'.*loan.*overdue.*repayable\sis\srs.\s?([0-9]+)'
    pattern_8 = r'.*loan.*rs\.\s([0-9]+).*overdue.*'
    pattern_9 = r'.*loan.*overdue.*repayment.*rs\.?\s([0-9]+)'

    # due
    pattern_10 = r'.*payment.*rs\.?.*?([0-9]+).*due.*'  # group(1) for amount
    pattern_11 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'  # group(1) for date and group(2) for amount
    pattern_12 = r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*'  # group(1) for amount and group(2) for date
    pattern_13 = r'due\s(?:on)?.*([0-9]+/[0-9]+).*'  # group(1) for date in cashbn
    pattern_14 = r'.*loan.*rs\.?.*?([0-9]+).*due.*'  # group(1) for loan amount
    pattern_15 = r'.*payment.*due.*'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        # matcher_3 = re.search(pattern_3, message)
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
        matcher_14= re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)

        if matcher_1 is not None or matcher_2 is not None or matcher_4 is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None or matcher_9 is not None or matcher_10 is not None or matcher_11 is not None or matcher_12 is not None or matcher_13 is not None or matcher_14 is not None or matcher_15 is not None :
            selected_rows.append(i)

    logger.info("Loan due overdue sms extracted successfully")

    logger.info("Append name in result dictionary for loan due overdue")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan due overdue successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan due overdue")
    return data.copy()[mask].reset_index(drop=True)


def loan(df, result, user_id, max_timestamp, new):
    logger = logger_1("loan_classifier", user_id)
    logger.info("get all loan messages")
    loan_messages = get_loan_messages(df)
    logger.info("remove all loan promotional messages")
    loan_messages_filtered = get_loan_messages_promotional_removed(df, loan_messages)

    logger.info("get all loan due overdue messages")
    data = get_over_due(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan due overdue dataframe into json")
    data_over_due = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan approval messages")
    data = get_approval(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan approval dataframe into json")
    data_approve = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan rejection messages")
    data = get_loan_rejected_messages(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan rejection dataframe into json")
    data_reject = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan disbursed messages")
    data = get_disbursed(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan disbursed dataframe into json")
    data_disburse = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan closed messages")
    data = get_loan_closed_messages(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan closed dataframe into json")
    data_closed = convert_json(data, user_id, max_timestamp)

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
        db.loanclosed.update({"cust_id": int(user_id)}, {"cust_id": int(user_id), 'timestamp': data_closed['timestamp'],
                                                         'modified_at': str(
                                                             datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                         "sms": data_closed['sms']}, upsert=True)
        db.loanapproval.update({"cust_id": int(user_id)},
                               {"cust_id": int(user_id), 'timestamp': data_approve['timestamp'],
                                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                "sms": data_approve['sms']}, upsert=True)
        db.loanrejection.update({"cust_id": int(user_id)},
                                {"cust_id": int(user_id), 'timestamp': data_reject['timestamp'],
                                 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                 "sms": data_reject['sms']}, upsert=True)
        db.disbursed.update({"cust_id": int(user_id)},
                            {"cust_id": int(user_id), 'timestamp': data_disburse['timestamp'],
                             'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                             "sms": data_disburse['sms']}, upsert=True)
        db.loandueoverdue.update({"cust_id": int(user_id)},
                                 {"cust_id": int(user_id), 'timestamp': data_over_due['timestamp'],
                                  'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                  "sms": data_over_due['sms']}, upsert=True)
        logger.info("All loan messages of new user inserted successfully")
    else:

        for i in range(len(data_approve['sms'])):
            logger.info("Old User checked")
            db.loanapproval.update({"cust_id": int(user_id)}, {"$push": {"sms": data_approve['sms'][i]}})
            logger.info("loan approval sms of old user updated successfully")
        db.loanapproval.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                   upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_reject['sms'])):
            logger.info("Old User checked")
            db.loanrejection.update({"cust_id": int(user_id)}, {"$push": {"sms": data_reject['sms'][i]}})
            logger.info("loan rejection sms of old user updated successfully")
        db.loanrejection.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                    upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_disburse['sms'])):
            logger.info("Old User checked")
            db.disbursed.update({"cust_id": int(user_id)}, {"$push": {"sms": data_disburse['sms'][i]}})
            logger.info("loan disbursed sms of old user updated successfully")
        db.disbursed.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_over_due['sms'])):
            logger.info("Old User checked")
            db.loandueoverdue.update({"cust_id": int(user_id)}, {"$push": {"sms": data_over_due['sms'][i]}})
            logger.info("loan due overdue sms of old user updated successfully")
        db.loandueoverdue.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                     upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_closed['sms'])):
            logger.info("Old User checked")
            db.loanclosed.update({"cust_id": int(user_id)}, {"$push": {"sms": data_closed['sms'][i]}})
            logger.info("loan closed sms of old user updated successfully")
        db.loanclosed.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                 upsert=True)
        logger.info("Timestamp of User updated")
    client.close()
