import pytz
import pandas as pd
from datetime import datetime
from HardCode.scripts.parameters_for_bl0.rejection_msgs.total_rejection_msg import get_defaulter
from HardCode.scripts.Util import conn
from HardCode.scripts.loan_analysis.my_modules import is_overdue, sms_header_splitter, grouping
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex, bank_headers

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


def  legal_messages_count_ratio(user_id,no_of_sms):
    ratio = -1
    legal_messages_count = 0
    try:
        legal_messages_count = get_defaulter(user_id)
        user_sms_count = no_of_sms
        if user_sms_count==0:
            ratio=0
        else:
            ratio = round(legal_messages_count / user_sms_count,4)
        return ratio
    except Exception as e:
        return ratio


def overdue_count_ratio(user_id,no_of_sms):
    ratio = -1
    overdue_count = 0
    connect = conn()
    try:
        user_sms_count = no_of_sms
        due_overdue_messages = connect.messagecluster.loandueoverdue.find_one({'cust_id': user_id})
        if due_overdue_messages:
            due_overdue_messages = pd.DataFrame(due_overdue_messages['sms'])
            sms_header_splitter(due_overdue_messages)
            app_list = list(due_overdue_messages["Sender-Name"].unique())
        else:
            return overdue_count,ratio
        if not due_overdue_messages.empty:
            for i in range(due_overdue_messages.shape[0]):
                message = str(due_overdue_messages['body'][i]).lower()
                app = due_overdue_messages["Sender-Name"][i]
                #app_name = app
                if app not in loan_apps_regex.keys() and app not in bank_headers:
                    app = 'OTHER'
                if app in loan_apps_regex.keys():
                    if is_overdue(message, app):
                        overdue_count += 1


        if user_sms_count==0:
            ratio=0
        else:
            ratio = round(overdue_count / user_sms_count,4)

        return overdue_count,ratio
    except Exception as e:
        return overdue_count, ratio
