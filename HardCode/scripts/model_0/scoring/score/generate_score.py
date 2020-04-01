from HardCode.scripts.model_0.scoring.parameters.all_params import get_parameters
from pprint import pprint


def scoring_rejection(rejection_variables):

    global score
    loan_app_count_check = rejection_variables['loan_app_count_check']
    monthly_balance_check = rejection_variables['monthly_balance_check']
    reference_check = rejection_variables['reference_check']
    rejection_check = rejection_variables['rejection_check']
    email_check = rejection_variables['email_check']
    due_days_check = rejection_variables['due_days_check']
    account_status_check = rejection_variables['account_status_check']

    # >>==>> rejection criteria
    if not loan_app_count_check:
        print(1)
        score -= 200

    if not monthly_balance_check:
        print(2)
        score -= 200

    if not reference_check:
        print(3)
        score -= 300

    if not rejection_check:
        print(4)
        score -= 200

    if not email_check:
        print(5)
        score -= 50

    if not due_days_check:
        print(6)
        score -= 300
    if not account_status_check:
        score -= 400


def scoring_approval(approval_variables):

    global score
    loan_limit_check = approval_variables['loan_limit_check']
    secured_unsecured_check = approval_variables['secured_unsecured_check']
    age_of_oldest_trade_check = approval_variables['age_of_oldest_trade_check']
    cc_limit_check = approval_variables['cc_limit_check']

    if loan_limit_check:
        score += 200

    if secured_unsecured_check:
        score += 100

    if age_of_oldest_trade_check:
        score += 100

    if cc_limit_check:
        score += 200


def get_score(user_id, cibil_df):

    global score
    score = 1000
    status = True
    values = {}
    try:
        variables, values = get_parameters(user_id, cibil_df)
        scoring_rejection(variables['rejection_variables'])
        scoring_approval(variables['approval_variables'])

    except BaseException as e:
        print(f"Error in scoring model : {e}")
        status = False
    finally:
        model_0 = {
            'parameters': values,
            'score': score
        }
        return {'cust_id': user_id, 'Model_0': model_0, 'status': status}
