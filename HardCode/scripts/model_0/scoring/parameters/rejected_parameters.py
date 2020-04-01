from HardCode.scripts.model_0.rejection_criteria.loan_app.loan_app_count_validate import loan_app_count
from HardCode.scripts.model_0.rejection_criteria.monthly_balance.balance import average_monthly_balance
from HardCode.scripts.model_0.rejection_criteria.reference_verification.validation.check_reference import validate

from HardCode.scripts.model_0.rejection_criteria.email_id.get_email import get_email_details
from HardCode.scripts.model_0.rejection_criteria.due_days.max_days import max_due_days
from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.model_0.rejection_criteria.account_status.status import get_acc_status
from HardCode.scripts.model_0.rejection_criteria.relative_verification.relative_validation import rel_validate


def get_rejection_parameters(user_id, cibil_df):
    """
    :returns dictionaries of rejected parameters and their values
    :rtype: dict
    """
    monthly_balance = average_monthly_balance(user_id)
    loan_app_count_percentage = loan_app_count(user_id)
    reference = validate(user_id)
    rejection_result = check_rejection(user_id)
    del rejection_result['cust_id']

    email_id = get_email_details(user_id)
    due_days = max_due_days(cibil_df)

    # relative_verification_result = rel_validate(user_id)
    relative_verification_result = {}
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

    # >>==>> Email ID check
    email_check = True
    if email_id:
        count_of_digits = sum([i.isdigit() for i in email_id])
        if count_of_digits > 4:
            email_check = False

    # >>==>> due days check
    due_days_check = True
    if due_days >= 60:
        due_days_check = False

    # >>==>> account status
    account_status_check = get_acc_status(cibil_df)

    # >>==>> relative verification
    relative_validation_check = True
    # if relative_verification_result['status']:
    #     relative_validation_check = relative_verification_result['result']['verification']

    rejection_variables = {
        'loan_app_count_check': loan_app_count_check,
        'monthly_balance_check': monthly_balance_check,
        'reference_check': reference_check,
        'rejection_check': rejection_check,
        'email_check': email_check,
        'due_days_check': due_days_check,
        'account_status_check': account_status_check,
        'relative_validation_check': relative_validation_check

    }
    rejection_values = {

        'monthly_balance': monthly_balance,
        'loan_app_count_percentage': loan_app_count_percentage,
        'reference': reference,
        'rejection_result': rejection_result,
        'email_id': email_id,
        'due_days': due_days,
        'account_type_default': account_status_check,
        'relative_contact_verification' : relative_verification_result

    }

    return rejection_variables, rejection_values
