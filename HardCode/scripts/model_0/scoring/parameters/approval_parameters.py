from HardCode.scripts.model_0.approval_criteria.credit_card_limit.cc_limit import get_cc_limit
from HardCode.scripts.model_0.approval_criteria.loan_limit.max_limit import loan_limit
from HardCode.scripts.model_0.approval_criteria.secured_unsecured_loans.count import secure_unsecured_loan
from HardCode.scripts.model_0.approval_criteria.age_of_oldest_trade.age import age_oldest_trade
from HardCode.scripts.model_0.approval_criteria.active_close_status.active_closed_count import get_active_closed


def get_approval_parameters(user_id, cibil_df):
    """
    :returns dictionaries of approval parameters and their values
    :rtype: dict
    """
    max_loan_limit = loan_limit(user_id)
    cc_limit = get_cc_limit(user_id)
    secured_count, unsecured_count = secure_unsecured_loan(cibil_df)
    age_of_oldest_trade = age_oldest_trade(cibil_df)
    active_count, closed_count = get_active_closed(cibil_df)

    # >>==>> loan limit
    loan_limit_check = False
    if max_loan_limit >= 4500:
        loan_limit_check = True

    # >>==>> secured unsecured loan count check
    secured_unsecured_check = False
    if secured_count > 0:
        secured_unsecured_check = True

    # >>==>> age of oldest trade
    age_of_oldest_trade_check = False
    if age_of_oldest_trade >= 24:
        age_of_oldest_trade_check = True

    # >>==>> credit card limit
    cc_limit_check = False
    for i in cc_limit.keys():
        if cc_limit[i] >= 50000:
            cc_limit_check = True

    # >>==>> active closed account
    active_close_check = False
    if active_count <= (active_count+closed_count) *0.33:
        active_close_check = True



    approval_variables = {
        'loan_limit_check': loan_limit_check,
        'secured_unsecured_check': secured_unsecured_check,
        'age_of_oldest_trade_check': age_of_oldest_trade_check,
        'cc_limit_check': cc_limit_check,
        'active_close_check':active_close_check
    }

    approval_values = {

        'max_loan_limit': max_loan_limit,
        'secured_unsecured_loans_count': {'secured': secured_count, 'unsecured_count': unsecured_count},
        'age_of_oldest_trade': age_of_oldest_trade,
        'credit_card_limit': cc_limit,
        'active_count':active_count,
        'closed_count':closed_count
    }

    return approval_variables, approval_values
