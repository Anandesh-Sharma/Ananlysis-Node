# from HardCode.scripts.model_0.parameters.deduction_parameters.loan_app.loan_app_count_validate import loan_app_count
from HardCode.scripts.Util import conn
def loan_app_count_check(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})['parameters'][-1]
    # loan_app_count_percentage , status = loan_app_count(user_id)
    loan_app_count_percentage = parameters['percentage_of_loan_apps']
    # >>==>> loan app count
    loan_app_count_check1 = False
    loan_app_count_check2 = False
    loan_app_count_check3 = False
    loan_app_count_check4 = False
    loan_app_count_check = False


    if loan_app_count_percentage >= 0.70:
        loan_app_count_check1 = True
    if 0.70 > loan_app_count_percentage >= 0.60:
        loan_app_count_check2 = True
    if 0.60 > loan_app_count_percentage >= 0.50:
        loan_app_count_check3 = True
    if loan_app_count_percentage < 0.50:
        loan_app_count_check4 = True

    else:
        loan_app_count_check = True
    connect.close()
    variables = {
        'loan_app_count_check1': loan_app_count_check1,
        'loan_app_count_check2': loan_app_count_check2,
        'loan_app_count_check3': loan_app_count_check3,
        'loan_app_count_check4': loan_app_count_check4,
        'loan_app_count_check': loan_app_count_check
    }

    values = {
        'loan_app_count' : loan_app_count_percentage
    }
    return variables,values