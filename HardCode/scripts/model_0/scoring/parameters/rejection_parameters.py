from HardCode.scripts.model_0.rejection_criteria.loan_app.loan_app_count_validate import loan_app_count
from HardCode.scripts.model_0.rejection_criteria.account_status.status import get_acc_status
from HardCode.scripts.model_0.rejection_criteria.reference_verification.validation.check_reference import validate
from HardCode.scripts.model_0.rejection_criteria.payment_rating.pay_rating import get_payment_rating
from HardCode.scripts.model_0.rejection_criteria.due_days.max_days import max_due_days
from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.model_0.rejection_criteria.ecs_bounce.ecs_bounce import get_count_ecs
from HardCode.scripts.model_0.rejection_criteria.loan_limit.loan_info import loan_limit

def rejecting_parameters(user_id,cibil_df):

    loan_app = loan_app_count(user_id)
    account_status = get_acc_status(cibil_df)
    reference = validate(user_id)
    payment_rating = get_payment_rating(cibil_df)
    due_days = max_due_days(cibil_df)
    rejection_app = check_rejection(user_id)
    ecs_count = get_count_ecs(user_id)
    max_limit, loan_due_days, no_of_loan_apps, premium_apps = loan_limit(user_id)


    rejection_reasons = []
    if loan_app >= 0.70:
        msg = "the number of loan apps were greater than 70% of total apps"
        rejection_reasons.append(msg)

    if not account_status:
        msg = "written off nas suit filed found for the user"
        rejection_reasons.append(msg)

    if reference['status']:
        if not reference['result']['verification']:
            msg = "parents information did not match in the contact list"
            rejection_reasons.append(msg)

    if payment_rating['pay_rating']:
        if not payment_rating['status']:
            msg =" payment rating not appropriate"
            rejection_reasons.append(msg)
    elif due_days >= 60:
        msg = "due days over 60 days"
        rejection_reasons.append(msg)

    if rejection_app:
        msg = "rejected from other apps as well"
        rejection_reasons.append(msg)

    if ecs_count >= 4 :
        msg = "rejected as there are more than 4 ecs related msgs"
        rejection_reasons.append(msg)

    if loan_due_days >= 9:
        msg = "rejected as due days for a loan are more than 9 days"
        rejection_reasons.append(msg)


    return rejection_reasons


