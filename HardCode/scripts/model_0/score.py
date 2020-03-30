from HardCode.scripts.model_0.parameters import get_parameters
from pprint import pprint


def get_score(user_id, cibil_file):
    status = True
    score = 1000
    values = {}
    try:
        variables, values = get_parameters(user_id, cibil_file)

        # pprint(variables)

        loan_app_count_check = variables['rejection_variables']['loan_app_count_check']
        monthly_balance_check = variables['rejection_variables']['monthly_balance_check']
        reference_check = variables['rejection_variables']['reference_check']
        rejection_check = variables['rejection_variables']['rejection_check']
        loan_limit_check = variables['approval_variables']['loan_limit_check']

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

    except BaseException as e:
        print(f"Error in scoring model : {e}")
        status = False
    finally:
        model_0 = {
            'parameters': values,
            'score': score
        }
        return {'cust_id': user_id, 'Model_0': model_0, 'status': status}
