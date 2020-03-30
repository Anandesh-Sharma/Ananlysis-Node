from HardCode.scripts.model_0.parameters import get_parameters
from pprint import pprint


def get_score(user_id, cibil_file):
    variables = get_parameters(user_id, cibil_file)

    pprint(variables)

    loan_app_count_check = variables['rejection_variables']['loan_app_count_check']
    monthly_balance_check = variables['rejection_variables']['monthly_balance_check']
    reference_check = variables['rejection_variables']['reference_check']
    rejection_check = variables['rejection_variables']['rejection_check']
    loan_limit_check = variables['approval_variables']['loan_limit_check']
    score = 1000

    # >>==>> rejection criteria
    if not loan_app_count_check:
        score -= 200

    if not monthly_balance_check:
        score -= 200

    if not reference_check:
        score -= 300

    if not rejection_check:
        score -= 200

    # >>==>> approval criteria
    if loan_limit_check:
        score += 200

    print(score)

# 236499
