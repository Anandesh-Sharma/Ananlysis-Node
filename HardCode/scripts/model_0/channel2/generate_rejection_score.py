

def deduction_score(deduction_variables):
    score = 800
    weights = {}

    reference_check = deduction_variables['reference_var']['reference_check']
    relatives_check1 = deduction_variables['reference_var']['relatives_check1']
    relatives_check2 = deduction_variables['reference_var']['relatives_check2']
    relatives_check3 = deduction_variables['reference_var']['relatives_check3']

    # loan_app_check1 = deduction_variables['loan_app_count_var']['loan_app_count_check1']
    # loan_app_check2 = deduction_variables['loan_app_count_var']['loan_app_count_check2']
    # loan_app_check3 = deduction_variables['loan_app_count_var']['loan_app_count_check3']
    # loan_app_check = deduction_variables['loan_app_count_var']['loan_app_count_check']


    loan_limit_check2 = deduction_variables['loan_var']['loan_limit_check2']
    loan_limit_check3 = deduction_variables['loan_var']['loan_limit_check3']
    loan_limit_check4 = deduction_variables['loan_var']['loan_limit_check4']
    loan_limit_check5 = deduction_variables['loan_var']['loan_limit_check5']
    loan_limit_check6 = deduction_variables['loan_var']['loan_limit_check6']
    loan_limit_check = deduction_variables['loan_var']['loan_limit_check']

    loan_due_check2 = deduction_variables['loan_var']['loan_due_check2']
    loan_due_check3 = deduction_variables['loan_var']['loan_due_check3']
    loan_due_check4 = deduction_variables['loan_var']['loan_due_check4']
    loan_due_check5 = deduction_variables['loan_var']['loan_due_check5']
    # loan_due_check = deduction_variables['loan_var']['loan_due_check']


    ecs_check = deduction_variables['ecs_var']['ecs_check']
    chq_check = deduction_variables['ecs_var']['cb_check']


    available_balance_check2 = deduction_variables['available_balance_var']['available_balance_check2']
    available_balance_check3 = deduction_variables['available_balance_var']['available_balance_check3']
    available_balance_check4 = deduction_variables['available_balance_var']['available_balance_check4']
    available_balance_check5 = deduction_variables['available_balance_var']['available_balance_check5']
    available_balance_check6 = deduction_variables['available_balance_var']['available_balance_check6']
    available_balance_check7 = deduction_variables['available_balance_var']['available_balance_check7']



    if not reference_check and not relatives_check1:
        score -= 150
        weights['reference'] = '-150'

    elif not reference_check or not relatives_check1:
        score -= 75
        weights['reference'] = '-75'


    if relatives_check2:
        score -= 45
        weights['reference'] = '-45'

    if relatives_check3:
        score -= 20
        weights['reference'] = '-20'

    # if loan_app_check3:
    #     score -= 20
    #     weights['loan_app_percent'] = '-20'
    #
    # if loan_app_check2:
    #     score -= 50
    #     weights['loan_app_percent'] = '-50'
    #
    #
    # if loan_app_check1:
    #     score -= 100
    #     weights['loan_app_percent'] = '-100'



    if loan_limit_check2:
        score -= 20
        weights['loan_limit'] = '-20'

    if loan_limit_check3:
        score -= 40
        weights['loan_limit'] = '-40'

    if loan_limit_check4:
        score -= 60
        weights['loan_limit'] = '-60'

    if loan_limit_check5:
        score -= 80
        weights['loan_limit'] = '-80'

    if loan_limit_check6:
        score -= 100
        weights['loan_limit'] = '-100'

    # if loan_limit_check:
    #     score -= 150
    #     weights['loan_limit'] = '-150'

    if loan_due_check2:
        score -= 40
        weights['loan_due_days'] = '-40'

    if loan_due_check3:
        score -= 70
        weights['loan_due_days'] = '-70'

    if loan_due_check4:
        score -= 100
        weights['loan_due_days'] = '-100'

    if loan_due_check5:
        score -= 200
        weights['loan_due_days'] = '-200'



    if ecs_check or chq_check:
        score -= 40
        weights['ecs_count'] = '-40'


    if available_balance_check2:
        score -= 170
        weights['available_balance'] = '-170'

    if available_balance_check3:
        score -= 130
        weights['available_balance'] = '-130'

    if available_balance_check4:
        score -= 100
        weights['available_balance'] = '-100'

    if available_balance_check5:
        score -= 70
        weights['available_balance'] = '-70'

    if available_balance_check6:
        score -= 50
        weights['available_balance'] = '-50'

    if available_balance_check7:
        score -= 200
        weights['available_balance'] = '-200'

    return score ,weights