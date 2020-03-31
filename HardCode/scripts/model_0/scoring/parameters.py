from HardCode.scripts.model_0.rejection_criteria.loan_app.loan_app_count_validate import loan_app_count
from HardCode.scripts.model_0.rejection_criteria.monthly_balance.balance import average_monthly_balance
from HardCode.scripts.model_0.rejection_criteria.reference_verification.validation.check_reference import validate
from HardCode.scripts.model_0.approval_criteria.credit_card_limit.cc_limit import get_cc_limit
from pprint import pprint

from HardCode.scripts.model_0.approval_criteria.loan_limit.max_limit import loan_limit
from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.model_0.approval_criteria.secured_unsecured_loans.count import secure_unsecured_loan


def get_parameters(user_id, cibil_df):
    monthly_balance = average_monthly_balance(user_id)
    max_loan_limit = loan_limit(user_id)
    loan_app_count_percentage = loan_app_count(user_id)

    reference = validate(user_id)
    rejection_result = check_rejection(user_id)
    del rejection_result['cust_id']

    cc_limit = get_cc_limit(user_id)
    secured_unsecured = secure_unsecured_loan(user_id, cibil_df)

    values = {
        'monthly_balance': monthly_balance,
        'max_loan_limit': max_loan_limit,
        'loan_app_count_percentage': loan_app_count_percentage,
        'reference': reference,
        'rejection_result': rejection_result,
        'cc_limit': cc_limit,
        'secured_unsecured': secured_unsecured

    }
    print("*********************************************************")

    print("monthly balance")
    pprint(monthly_balance)
    print('max loan limit')
    pprint(max_loan_limit)
    print('loan app percentage')
    pprint(loan_app_count_percentage)
    print('reference')
    pprint(reference)
    print('loan rejection')
    pprint(rejection_result)
    print('cc_limit')
    pprint(cc_limit)
    print("Secured Unsecured")
    pprint(secured_unsecured)
    print("*********************************************************")

    # >>==>> loan app count
    loan_app_count_check = True
    if loan_app_count_percentage >= 0.70:
        loan_app_count_check = False

    # >>==>> average monthly balance
    monthly_balance_check = True
    if monthly_balance <= 2000:
        monthly_balance_check = False

    # >>==>> reference_check
    reference_check = True
    if reference['status']:
        reference_check = reference['result']['verification']

    # >>==>> rejection check
    rejection_check = True
    if rejection_result['status']:
        if rejection_result['result']['rejected_loan_apps']:
            rejection_check = False

    # >>==>> loan limit
    loan_limit_check = False
    if max_loan_limit >= 4500:
        loan_limit_check = True

    rejection_variables = {
        'loan_app_count_check': loan_app_count_check,
        'monthly_balance_check': monthly_balance_check,
        'reference_check': reference_check,
        'rejection_check': rejection_check

    }

    approval_variables = {
        'loan_limit_check': loan_limit_check
    }

    variables = {
        'approval_variables': approval_variables,
        'rejection_variables': rejection_variables

    }

    return variables, values
# 280018
