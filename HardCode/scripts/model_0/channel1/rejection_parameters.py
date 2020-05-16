
from HardCode.scripts.Util import conn

def rejecting_parameters(user_id,sms_count):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})
    loan_app = parameters['parameters']['percentage_of_loan_apps']
    account_status_value = parameters['parameters']['account_status']
    bal = parameters['parameters']['available_balance']
    loan_due_days = parameters['parameters']['loan_info']['OVERDUE_DAYS']
    overdue_count = parameters['parameters']['overdue_msg_count']
    rejection_msg  = parameters['parameters']['legal_message_count']
    flag = parameters['parameters']['legal_message_status']

    user_sms_count = sms_count
    # bal = find_info(user_id)

    rejection_reasons = []
    if loan_app >= 0.70:
        msg = "the number of loan apps were greater than 70% of total apps"
        rejection_reasons.append(msg)

    if not account_status_value:
        msg = "written off nas suit filed found for the user"
        rejection_reasons.append(msg)


    if loan_due_days >= 15:
        msg = "user has overdue days more than 15 days"
        rejection_reasons.append(msg)



    if flag and rejection_msg == 0:
        msg = "user has msgs for overdue of more than 15 days"
        rejection_reasons.append(msg)

    if flag and rejection_msg >= 3:
        msg = "user has legal notice messages"
        rejection_reasons.append(msg)

    if user_sms_count < 100:
        msg = "user has insufficient msgs"
        rejection_reasons.append(msg)

    if bal['AC_NO'] == "0":
        msg = "user does not have account information in messages"
        rejection_reasons.append(msg)

    if overdue_count > 10:
        msg = "user has more than 10 overdue msgs"
        rejection_reasons.append(msg)


    return rejection_reasons


