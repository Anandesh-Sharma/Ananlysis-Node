from HardCode.scripts.model_0.parameters.deduction_parameters.loan_limit.loan_info import loan_limit

def loan_check(user_id):

    max_limit, due_days, no_of_loan_apps, premium_apps ,overdue_ratio = loan_limit(user_id)


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
            loan_limit_check2 = True
        if 3000 > max_limit > 2000:
            loan_limit_check2 = True
        if 2000 > max_limit > 1000:
            loan_limit_check2 = True

    if max_limit == -1:
        loan_limit_check = True

    # >>==>> due days
    loan_due_check1 = False
    loan_due_check2 = False
    loan_due_check3 = False
    loan_due_check4 = False
    loan_due_check5 = False
    loan_due_check = False

    if overdue_ratio < 0.05:
        if  due_days != -1:
            if due_days == 0:
                loan_due_check1 = True
            if 3 > due_days >= 1:
                loan_due_check2 = True
            if 6 > due_days >= 3:
                loan_due_check3 = True
            if 9 > due_days >= 6:
                loan_due_check4 = True
            if due_days >= 9:
                loan_due_check5 = True

        if due_days == -1:
            loan_due_check = True

    else:
        loan_due_check = True

    # >>==>> no. of loan apps
    loan_app_no_check1 = False
    loan_app_no_check2 = False
    loan_app_no_check3 = False
    loan_app_no_check4 = False
    loan_app_no_check = False

    if no_of_loan_apps >= 4:
        loan_app_no_check1 = True
    if no_of_loan_apps == 3:
        loan_app_no_check2 = True
    if no_of_loan_apps == 2:
        loan_app_no_check3 = True
    if no_of_loan_apps == 1:
        loan_app_no_check4 = True
    if no_of_loan_apps == 0:
        loan_app_no_check = True



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
        'loan_due_check': loan_due_check,
        'loan_app_no_check1': loan_app_no_check1,
        'loan_app_no_check2': loan_app_no_check2,
        'loan_app_no_check3': loan_app_no_check3,
        'loan_app_no_check4': loan_app_no_check4,
        'loan_app_no_check': loan_app_no_check,


    }

    values = {
        'max_limit' : max_limit,
        'due_days': due_days,
        'no_of_loan_apps': no_of_loan_apps,

    }

    return variables,values