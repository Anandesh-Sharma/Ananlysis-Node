# from HardCode.scripts.model_0.parameters.deduction_parameters.loan_limit.loan_info import loan_limit
# from HardCode.scripts.model_0.parameters.deduction_parameters.loan_limit.last_loan_details import get_final_loan_details
from HardCode.scripts.Util import conn
def loan_check(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})['parameters'][-1]
    loans = connect.analysis.loan.find_one({'cust_id': user_id})
    max_limit =  parameters['loan_info']['MAX_AMOUNT']
    due_days =  parameters['loan_info']['OVERDUE_DAYS']
    no_of_loan_apps = parameters['loan_info']['TOTAL_LOAN_APPS']
    loan_apps =  loans['user_app_list']
    overdue_ratio =  parameters['loan_info']['OVERDUE_RATIO']
    report =  parameters['loan_info']['OVERDUE_DAYS']  # TODO change this for actual report from loan analysis
    # max_limit, due_days, no_of_loan_apps, loan_apps ,overdue_ratio, loan_dates, total_loans = loan_limit(user_id)
    # report = get_final_loan_details(user_id)


    #>>==>> loan limit
    loan_limit_check1 = False
    loan_limit_check2 = False
    loan_limit_check3 = False
    loan_limit_check4 = False
    loan_limit_check5 = False
    loan_limit_check6 = False
    loan_limit_check = False
    if max_limit != -1:
        if max_limit >= 6000:
            loan_limit_check1 = True
        if 6000 > max_limit > 5000:
            loan_limit_check2 = True
        if 5000 > max_limit > 4000:
            loan_limit_check3 = True
        if 4000 > max_limit > 3000:
            loan_limit_check4 = True
        if 3000 > max_limit > 2000:
            loan_limit_check5 = True
        if 2000 > max_limit > 1000:
            loan_limit_check6 = True

    if max_limit == -1:
        loan_limit_check = True

    # >>==>> due days
    loan_due_check1 = False
    loan_due_check2 = False
    loan_due_check3 = False
    loan_due_check4 = False
    loan_due_check5 = False
    #loan_due_check = False


    if due_days > 3:
        loan_due_check1 = True
    if 3 > due_days > 7:
        loan_due_check2 = True
    if 7 > due_days > 12:
        loan_due_check3 = True
    if 12 > due_days > 15:
        loan_due_check4 = True
    if due_days > 15:
        loan_due_check5 = True

    # if due_days == -1:
    #     loan_due_check = True



    connect.close()
    variables = {
        'loan_limit_check1': loan_limit_check1,
        'loan_limit_check2': loan_limit_check2,
        'loan_limit_check3': loan_limit_check3,
        'loan_limit_check4': loan_limit_check4,
        'loan_limit_check5': loan_limit_check5,
        'loan_limit_check6': loan_limit_check6,
        'loan_limit_check': loan_limit_check,
        'loan_due_check1': loan_due_check1,
        'loan_due_check2': loan_due_check2,
        'loan_due_check3': loan_due_check3,
        'loan_due_check4': loan_due_check4,
        'loan_due_check5': loan_due_check5,
        #'loan_due_check': loan_due_check,
    }

    values = {
        'max_limit' : max_limit,
        'due_days': due_days,
        'no_of_loan_apps': no_of_loan_apps,
        'loan_app_list': loan_apps,
        'overdue_ratio':overdue_ratio,
        'last_loan_detail': report

    }

    return variables,values