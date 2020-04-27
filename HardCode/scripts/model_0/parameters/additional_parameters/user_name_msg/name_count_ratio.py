from HardCode.scripts.model_0.parameters.additional_parameters.user_name_msg.username_info import get_profile_name
from HardCode.scripts.Util import conn
import pandas as pd

import re


def get_user_messages(cust_id):
    client = conn()
    db = client.messagescluster

    approval_data = db.loanapproval
    disbursed_data = db.disbursed
    overdue_data = db.loandueoverdue
    closed_data = db.loanclosed
    trans_data = db.transaction
    # extra_data = db.extra
    reject_data = db.loanrejection
    creditcard_data = db.creditcard
    user_data = pd.DataFrame(columns=['sender', 'body', 'timestamp', 'read'])
    try:
        closed = closed_data.find_one({"cust_id": cust_id})
        trans = trans_data.find_one({"cust_id": cust_id})
        disbursed = disbursed_data.find_one({"cust_id": cust_id})
        approval = approval_data.find_one({"cust_id": cust_id})
        overdue = overdue_data.find_one({"cust_id": cust_id})
        extra = extra_data.find_one({"cust_id": cust_id})
        reject = reject_data.find_one({"cust_id": cust_id})
        creditcard = creditcard_data.find_one({"cust_id": cust_id})

        if len(closed['sms']) != 0:
            closed_df = pd.DataFrame(closed['sms'])
            user_data = user_data.append(closed_df)

        if len(trans_data['sms']) != None:
            transaction_df = pd.DataFrame(trans['sms'])
            user_data = user_data.append(transaction_df)

        if len(disbursed['sms']) != 0:
            disbursed_df = pd.DataFrame(disbursed['sms'])
            user_data = user_data.append(disbursed_df)

        if len(overdue['sms']) != 0:
            overdue_df = pd.DataFrame(overdue['sms'])
            user_data = user_data.append(overdue_df)

        if len(approval['sms']) != 0:
            approval_df = pd.DataFrame(approval['sms'])
            user_data = user_data.append(approval_df)
        '''
        if len(extra['sms']) != 0:
            extra_df = pd.DataFrame(extra['sms'])
            user_data = user_data.append(extra_df)
            '''
        if len(reject['sms']) != 0:
            reject_df = pd.DataFrame(reject['sms'])
            user_data = user_data.append(reject_df)

        if len(creditcard['sms']) != 0:
            creditcard_df = pd.DataFrame(creditcard['sms'])
            user_data = user_data.append(creditcard_df)

        user_data.sort_values(by=["timestamp"])
        user_data.reset_index(drop=True, inplace=True)
        client.close()
    except:
        client.close()
    finally:
        return user_data


def get_name_count(cust_id):
    name_count = 0
    user_data = get_user_messages(cust_id)
    if not user_data.empty:
        actual_name = get_profile_name(cust_id)
        actual_name = str(actual_name).split(' ')
        pattern = str(actual_name[0])
        for i in range(user_data.shape[0]):
            message = str(user_data['body'][i]).lower()
            matcher = re.search(pattern, message)
            if matcher is not None:
                name_count += 1
                break
    return name_count
