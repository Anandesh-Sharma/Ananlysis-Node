import re
from .Util import conn,read_json,convert_json
from tqdm import tqdm


def get_loan_closed_messages(data, loan_messages_filtered, result, name):
    selected_rows = []
    pattern_1 = '(.*)?loan(.*)?closed(.*)?'
    pattern_2 = '(.*)?closed(.*)?successfully(.*)?'
    pattern_3 = 'successfully\sreceived\spayment'
    pattern_4 = 'loan.*?paid\sback'

    for i in tqdm(range(data.shape[0])):
        if i not in loan_messages_filtered:
            continue

        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None:
            selected_rows.append(i)
    
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    return data.copy()[mask].reset_index(drop=True)


def get_loan_messages(data):
    loan_messages = []
    pattern = '(.*)?loan(.*)?'

    for i in tqdm(range(data.shape[0])):
        message = str(data['body'][i]).lower()
        matcher = re.search(pattern, message)

        if matcher is not None:
            loan_messages.append(i)
    
    return loan_messages


def get_loan_messages_promotional_removed(data, loan_messages):
    loan_messages_filtered = []

    pattern_1 = '(.*)?apply(.*)?'
    pattern_2 = '(.*)?offer(.*)?'
    pattern_3 = '(.*)?offers(.*)?'
    pattern_4 = '(.*)?avail(.*)?'
    pattern_5 = '(.*)?instant(.*)?'
    pattern_6 = '(.*)?instantly(.*)?'
    pattern_7 = '(.*)?cashback(.*)?'
    pattern_8 = '(.*)?voucher(.*)?'
    pattern_9 = '(.*)?discount(.*)?'
    pattern_10 = '(.*)?hurry(.*)?'
    pattern_11 = '(.*)?get(.*)loan(.*)?'

    for i in tqdm(range(data.shape[0])):
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
    selected_rows = []
    pattern_1 = '[^pre-]approved(.*)?'
    pattern_2 = 'succesfully(.*)?approved'
    pattern_3 = '(.*)?has(.*)?been(.*)?approved'

    for i in tqdm(range(data.shape[0])):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)

        if matcher_1 != None or matcher_2 != None or matcher_3 != None:
            selected_rows.append(i)

    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    return data.copy()[mask].reset_index(drop=True)

def get_disbursed(data, loan_messages_filtered, result, name):
    selected_rows = []
    pattern_1 = '(.*)?disbursed(.*)?'
    pattern_2 = '(.*)?disbursement(.*)?'
    pattern_3 = '(.*)?transferred(.*)?account(.*)?'

    for i in tqdm(range(data.shape[0])):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)

        if matcher_1 != None or matcher_2 != None or matcher_3 != None:
            selected_rows.append(i)

    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    return data.copy()[mask].reset_index(drop=True)


def get_loan_rejected_messages(data, loan_messages_filtered, result, name):
    selected_rows = []
    pattern_1 = '(.*)?rejected(.*)?'
    pattern_2 = '(.*)?reject(.*)?'
    pattern_3 = 'Declined[^\?]'
    pattern_4 = '(.*)?decline(.*)?'
    pattern_5 = '(.*)?not-approved(.*)?'
    pattern_6 = '(.*)?low cibil score(.*)?'
    pattern_7 = 'low credit score'
    pattern_8 = 'declined\?'

    for i in tqdm(range(data.shape[0])):
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

        if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None or matcher_4 != None:
            if matcher_6 == None and matcher_7 == None and matcher_8 == None and matcher_5==None:
                selected_rows.append(i)
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    return data.copy()[mask].reset_index(drop=True)


def get_over_due(data, loan_messages_filtered, result, name):
    selected_rows=[]
    pattern_1 = '(.*)?immediate(.*)payment(.*)'
    pattern_2 = '(.*)?delinquent(.*)?'
    pattern_3 = '(.*)?has(.*)?bounced(.*)?'
    pattern_4 = 'missed(.*)?payments'
    pattern_5 = '(.*)?due(.*)?'

    for i in tqdm(range(data.shape[0])):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_5 is not None:
            selected_rows.append(i)


    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    return data.copy()[mask].reset_index(drop=True)


def loan(df, result, name):
    loan_messages = get_loan_messages(df)
    loan_messages_filtered = get_loan_messages_promotional_removed(df, loan_messages)
    data = get_over_due(df, loan_messages_filtered, result, name)

    data_over_due = convert_json(data, name)
    #print('overdue')
    #print(data_over_due)


    data = get_approval(df, loan_messages_filtered, result, name)

    data_approve = convert_json(data, name)
    #print('approve')
    #print(data_approve)

    data = get_loan_rejected_messages(df, loan_messages_filtered, result, name)

    data_reject = convert_json(data, name)
    #print('reject')
    #print(data_reject)

    data = get_disbursed(df, loan_messages_filtered, result, name)

    data_disburse = convert_json(data, name)
    #print('disburse')
    #print(data_disburse)

    data = get_loan_closed_messages(df, loan_messages_filtered, result, name)

    data_closed = convert_json(data, name)
    #print('close')
    #print(data_closed)
    client = conn()
    db = client.messagecluster
    db.loanapproval.insert_one(data_approve)
    db.loanrejection.insert_one(data_reject)
    db.disbursed.insert_one(data_disburse)
    db.loandueoverdue.insert_one(data_over_due)
    db.loanclosed.insert_one(data_closed)
    client.close()
