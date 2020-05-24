from HardCode.scripts.parameters_for_bl0.user_name_msg.username_info import get_profile_name
from HardCode.scripts.Util import conn
import pandas as pd
from datetime import datetime, date
import pytz

import re


def get_user_messages(user_id):
    client = conn()

    approval_data = client.messagecluster.loanapproval.find_one({"cust_id": user_id})
    disbursed_data = client.messagecluster.disbursed.find_one({"cust_id": user_id})
    overdue_data = client.messagecluster.loandueoverdue.find_one({"cust_id": user_id})
    closed_data = client.messagecluster.loanclosed.find_one({"cust_id": user_id})
    trans_data = client.messagecluster.transaction.find_one({"cust_id": user_id})
    # extra_data = db.extra
    reject_data = client.messagecluster.loanrejection.find_one({"cust_id": user_id})
    creditcard_data = client.messagecluster.creditcard.find_one({"cust_id": user_id})
    user_data = pd.DataFrame(columns=['sender', 'body', 'timestamp', 'read'])

    try:
        approval = approval_data['sms']
        disbursed = disbursed_data['sms']
        overdue =overdue_data['sms']
        closed =closed_data['sms']
        trans = trans_data['sms']
        reject = reject_data['sms']
        creditcard = creditcard_data['sms']

        if closed :
            closed_df = pd.DataFrame(closed)
            user_data = user_data.append(closed_df)

        if trans and trans['sms']:
            transaction_df = pd.DataFrame(trans['sms'])
            user_data = user_data.append(transaction_df)

        if disbursed and disbursed['sms']:
            disbursed_df = pd.DataFrame(disbursed['sms'])
            user_data = user_data.append(disbursed_df)

        if overdue and overdue['sms']:
            overdue_df = pd.DataFrame(overdue['sms'])
            user_data = user_data.append(overdue_df)

        if approval and approval['sms']:
            approval_df = pd.DataFrame(approval['sms'])
            user_data = user_data.append(approval_df)

        # if len(extra['sms']) != 0:
        #     extra_df = pd.DataFrame(extra['sms'])
        #     user_data = user_data.append(extra_df)
        if reject and reject['sms']:
            reject_df = pd.DataFrame(reject['sms'])
            user_data = user_data.append(reject_df)

        if creditcard and creditcard['sms']:
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

    try:
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
    except BaseException as e:
        return name_count
