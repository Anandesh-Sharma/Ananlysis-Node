

def deduction_score(deduction_variables):
    score = 1000
    weights = {}

    #secured_unsecured_check1 = deduction_variables['secured_unsecured_var']['secured_unsecured_check1']
    secured_unsecured_check2 = deduction_variables['secured_unsecured_var']['secured_unsecured_check2']
    secured_unsecured_check3 = deduction_variables['secured_unsecured_var']['secured_unsecured_check3']
    secured_unsecured_check4 = deduction_variables['secured_unsecured_var']['secured_unsecured_check4']
    secured_unsecured_check5 = deduction_variables['secured_unsecured_var']['secured_unsecured_check5']
    secured_unsecured_check6 = deduction_variables['secured_unsecured_var']['secured_unsecured_check6']
    secured_unsecured_check = deduction_variables['secured_unsecured_var']['secured_unsecured_check']

    #age_of_oldest_trade_check1 = deduction_variables['age_of_oldest_trade_check1']
    age_of_oldest_trade_check2 = deduction_variables['age_of_oldest_trade_var']['age_of_oldest_trade_check2']
    age_of_oldest_trade_check3 = deduction_variables['age_of_oldest_trade_var']['age_of_oldest_trade_check3']
    age_of_oldest_trade_check4 = deduction_variables['age_of_oldest_trade_var']['age_of_oldest_trade_check4']
    age_of_oldest_trade_check5 = deduction_variables['age_of_oldest_trade_var']['age_of_oldest_trade_check5']
    age_of_oldest_trade_check6 = deduction_variables['age_of_oldest_trade_var']['age_of_oldest_trade_check6']
    age_of_oldest_trade_check7 = deduction_variables['age_of_oldest_trade_var']['age_of_oldest_trade_check7']
    age_of_oldest_trade_check = deduction_variables['age_of_oldest_trade_var']['age_of_oldest_trade_check']

    active_close_check1 = deduction_variables['active_close_var']['active_close_check1']
    active_close_check2 = deduction_variables['active_close_var']['active_close_check2']
    active_close_check3 = deduction_variables['active_close_var']['active_close_check3']
    active_close_check4 = deduction_variables['active_close_var']['active_close_check4']
    active_close_check5 = deduction_variables['active_close_var']['active_close_check5']
    active_close_check = deduction_variables['active_close_var']['active_close_check']

    #monthly_balance_check1 = deduction_variables["monthly_bal_check1"]
    # monthly_balance_check2 = deduction_variables['monthly_balance_var']["monthly_bal_check2"]
    # monthly_balance_check3 = deduction_variables['monthly_balance_var']["monthly_bal_check3"]
    # monthly_balance_check4 = deduction_variables['monthly_balance_var']["monthly_bal_check4"]
    # monthly_balance_check5 = deduction_variables['monthly_balance_var']["monthly_bal_check5"]
    # monthly_balance_check = deduction_variables['monthly_balance_var']["monthly_bal_check"]

    reference_check = deduction_variables['reference_var']['reference_check']
    reference_check1 = deduction_variables['reference_var']['reference_check1']

    loan_app_check1 = deduction_variables['loan_app_count_var']['loan_app_count_check1']
    loan_app_check2 = deduction_variables['loan_app_count_var']['loan_app_count_check2']
    loan_app_check3 = deduction_variables['loan_app_count_var']['loan_app_count_check3']
    loan_app_check = deduction_variables['loan_app_count_var']['loan_app_count_check']

    payment_rating_check1 = deduction_variables['payment_rating_var']['pay_rating_check1']
    payment_rating_check2 = deduction_variables['payment_rating_var']['pay_rating_check2']
    payment_rating_check3 = deduction_variables['payment_rating_var']['pay_rating_check3']
    payment_rating_check4 = deduction_variables['payment_rating_var']['pay_rating_check4']
    payment_rating_check = deduction_variables['payment_rating_var']['pay_rating_check']

    loan_limit_check1 = deduction_variables['loan_var']['loan_limit_check1']
    loan_limit_check2 = deduction_variables['loan_var']['loan_limit_check2']
    loan_limit_check3 = deduction_variables['loan_var']['loan_limit_check3']
    loan_limit_check4 = deduction_variables['loan_var']['loan_limit_check4']
    loan_limit_check5 = deduction_variables['loan_var']['loan_limit_check5']
    loan_limit_check6 = deduction_variables['loan_var']['loan_limit_check6']
    loan_limit_check = deduction_variables['loan_var']['loan_limit_check']

    loan_due_check1 = deduction_variables['loan_var']['loan_due_check1']
    loan_due_check2 = deduction_variables['loan_var']['loan_due_check2']
    loan_due_check3 = deduction_variables['loan_var']['loan_due_check3']
    loan_due_check4 = deduction_variables['loan_var']['loan_due_check4']
    loan_due_check5 = deduction_variables['loan_var']['loan_due_check5']
    loan_due_check = deduction_variables['loan_var']['loan_due_check']

    loan_app_no_check1 = deduction_variables['loan_var']['loan_app_no_check1']
    loan_app_no_check2 = deduction_variables['loan_var']['loan_app_no_check2']
    loan_app_no_check3 = deduction_variables['loan_var']['loan_app_no_check3']
    loan_app_no_check4 = deduction_variables['loan_var']['loan_app_no_check4']
    loan_app_no_check = deduction_variables['loan_var']['loan_app_no_check']

    premium_apps_check = deduction_variables['loan_var']['premium_apps_check']

    ecs_check1 = deduction_variables['ecs_var']['ecs_check1']
    ecs_check2 = deduction_variables['ecs_var']['ecs_check2']
    ecs_check3 = deduction_variables['ecs_var']['ecs_check3']
    ecs_check = deduction_variables['ecs_var']['ecs_check']




    if secured_unsecured_check2:
        score -= 20
        weights['secured_unsecured'] = '-20'

    if secured_unsecured_check3:
        score -= 30
        weights['secured_unsecured'] = '-30'

    if secured_unsecured_check4:
        score -= 35
        weights['secured_unsecured'] = '-35'

    if secured_unsecured_check5:
        score -= 40
        weights['secured_unsecured'] = '-40'

    if secured_unsecured_check6:
        score -= 45
        weights['secured_unsecured'] = '-45'

    if secured_unsecured_check:
        score -= 50
        weights['secured_unsecured'] = '-50'

    if age_of_oldest_trade_check2:
        score -= 20
        weights['age_old_trade'] = '-20'

    if age_of_oldest_trade_check3:
        score -= 40
        weights['age_old_trade'] = '-40'

    if age_of_oldest_trade_check4:
        score -= 60
        weights['age_old_trade'] = '-60'

    if age_of_oldest_trade_check5:
        score -= 80
        weights['age_old_trade'] = '-80'

    if age_of_oldest_trade_check6:
        score -= 90
        weights['age_old_trade'] = '-90'

    if age_of_oldest_trade_check7 or age_of_oldest_trade_check:
        score -= 100
        weights['age_old_trade'] = '-100'




    if active_close_check1:
        score -= 20
        weights['active_close'] = '-20'

    if active_close_check2:
        score -= 40
        weights['active_close'] = '-40'

    if active_close_check3:
        score -= 60
        weights['active_close'] = '-60'

    if active_close_check4:
        score -= 80
        weights['active_close'] = '-80'

    if active_close_check5 or active_close_check:
        score -= 100
        weights['active_close'] = '-100'



    # if monthly_balance_check2:
    #     score -= 30
    #     weights['monthly_balance'] = '-30'
    #
    # if monthly_balance_check3:
    #     score -= 50
    #     weights['monthly_balance'] = '-50'
    #
    # if monthly_balance_check4:
    #     score -= 70
    #     weights['monthly_balance'] = '-70'
    #
    # if monthly_balance_check5:
    #     score -= 100
    #     weights['monthly_balance'] = '-100'

    if not reference_check or reference_check1:
        score -= 100
        weights['reference'] = '-100'

    if loan_app_check1:
        score -= 20
        weights['loan_app_percent'] = '-20'

    if loan_app_check2:
        score -= 30
        weights['loan_app_percent'] = '-30'


    if loan_app_check3 or loan_app_check:
        score -= 40
        weights['loan_app_percent'] = '-40'



    if payment_rating_check2:
        score -= 20
        weights['payment_rating'] = '-20'

    if payment_rating_check3:
        score -= 40
        weights['payment_rating'] = '-40'

    if payment_rating_check4 or payment_rating_check:
        score -= 60
        weights['payment_rating'] = '-60'

    if loan_limit_check2:
        score -= 40
        weights['loan_limit'] = '-40'

    if loan_limit_check3:
        score -= 60
        weights['loan_limit'] = '-60'

    if loan_limit_check4:
        score -= 80
        weights['loan_limit'] = '-80'

    if loan_limit_check5:
        score -= 100
        weights['loan_limit'] = '-100'

    if loan_limit_check6:
        score -= 130
        weights['loan_limit'] = '-130'

    if loan_limit_check:
        score -= 150
        weights['loan_limit'] = '-150'

    if loan_due_check2:
        score -= 20
        weights['loan_due_days'] = '-20'

    if loan_due_check3:
        score -= 40
        weights['loan_due_days'] = '-40'

    if loan_due_check4:
        score -= 80
        weights['loan_due_days'] = '-80'

    if loan_due_check5 or loan_due_check:
        score -= 100
        weights['loan_due_days'] = '-100'

    if loan_app_no_check2:
        score -= 40
        weights['loan_apps_count'] = '-40'

    if loan_app_no_check3:
        score -= 60
        weights['loan_apps_count'] = '-60'

    if loan_app_no_check4:
        score -= 80
        weights['loan_apps_count'] = '-80'

    if loan_app_no_check:
        score -= 100
        weights['loan_apps_count'] = '-100'

    # if not premium_apps_check:
    #     score -= 50
    #     weights['premium_apps'] = '-50'

    if ecs_check1:
        score -= 20
        weights['ecs_count'] = '-20'

    if ecs_check2:
        score -= 40
        weights['ecs_count'] = '-40'

    if ecs_check3 or ecs_check:
        score -= 60
        weights['ecs_count'] = '-60'

    return score ,weights