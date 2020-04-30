import numpy as np
import pandas as pd
from HardCode.scripts.model_0.parameters.deduction_parameters.rejection_msgs.total_rejection_msg import get_defaulter
from HardCode.scripts.Util import conn
from HardCode.scripts.loan_analysis.my_modules import is_overdue


def get_user_messages_length(user_id):
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
        overdue = overdue_data['sms']
        closed = closed_data['sms']
        trans = trans_data['sms']
        reject = reject_data['sms']
        creditcard = creditcard_data['sms']

        if closed:
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
        return len(user_data)


def  legal_messages_count_ratio(user_id):
    ratio = -1
    legal_messages_count = 0
    try:
        connection = conn()
        # user_sms_count = connection.analysisresult.bl0.find_one({'cust_id':user_id})
        # user_sms_count = user_sms_count['result'][-1]
        # user_sms_count = user_sms_count['sms_count']
        user_sms_count = get_user_messages_length(user_id)
        defaulter, legal_messages_count = get_defaulter(user_id)
        ratio = legal_messages_count / user_sms_count

        return ratio, legal_messages_count
    except Exception as e:
        return ratio, legal_messages_count


def overdue_count_ratio(user_id):
    ratio = -1
    overdue_count = 0
    try:
        connection = conn()
        # user_sms_count = connection.analysisresult.bl0.find_one({'cust_id': user_id})
        # user_sms_count = user_sms_count['result'][-1]
        # user_sms_count = user_sms_count['sms_count']
        user_sms_count = get_user_messages_length(user_id)
        due_overdue_messages = connection.messagecluster.loandueoverdue.find_one({'cust_id': user_id})
        if due_overdue_messages:
            due_overdue_messages = pd.DataFrame(due_overdue_messages['sms'])
        else:
            return ratio, overdue_count
        if not due_overdue_messages.empty:
            for i in range(due_overdue_messages.shape[0]):
                message = str(due_overdue_messages['body'][i]).lower()
                if is_overdue(message):
                    overdue_count += 1

        ratio = overdue_count / user_sms_count
        return ratio, overdue_count
    except Exception as e:

        return ratio, overdue_count




