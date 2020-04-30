from HardCode.scripts.model_0.parameters.deduction_parameters.loan_app.loan_app_count_validate import loan_app_count
from HardCode.scripts.model_0.parameters.deduction_parameters.account_status.status import get_acc_status
from HardCode.scripts.model_0.parameters.deduction_parameters.loan_limit.loan_info import loan_limit
from HardCode.scripts.model_0.parameters.deduction_parameters.rejection_msgs.total_rejection_msg import get_defaulter
from HardCode.scripts.model_0.parameters.deduction_parameters.rejection_msgs.get_ratio import overdue_count_ratio
from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.available_balance import find_info


def rejecting_parameters(user_id,cibil_df,sms_count):
    loan_app , loan_status = loan_app_count(user_id)
    account_status_value , ac_status = get_acc_status(cibil_df)
    max_limit, loan_due_days, no_of_loan_apps, loan_apps , overdue_ratio, loan_dates = loan_limit(user_id)
    flag , rejection_msg = get_defaulter(user_id)
    ratio , overdue_count = overdue_count_ratio(user_id)
    user_sms_count = sms_count
    bal = find_info(user_id)

    rejection_reasons = []
    if loan_app >= 0.70:
        msg = "the number of loan apps were greater than 70% of total apps"
        rejection_reasons.append(msg)

    if not account_status_value:
        msg = "written off nas suit filed found for the user"
        rejection_reasons.append(msg)


    if loan_due_days['more_than_15'] > 0:
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


