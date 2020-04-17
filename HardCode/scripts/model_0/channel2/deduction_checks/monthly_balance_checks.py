from HardCode.scripts.model_0.parameters.deduction_parameters.monthly_balance.balance import average_monthly_balance

def monthly_bal_check(user_id):
    monthly_balance , status = average_monthly_balance(user_id)


    # >>==>> average monthly balance
    monthly_balance_check1 = False
    monthly_balance_check2 = False
    monthly_balance_check3 = False
    monthly_balance_check4 = False
    monthly_balance_check5 = False

    if monthly_balance >= 3000:
        monthly_balance_check1 = True

    if 3000 > monthly_balance >= 2500:
        monthly_balance_check2 = True

    if 2500 > monthly_balance >= 2000:
        monthly_balance_check3 = True

    if 2000 > monthly_balance >= 1500:
        monthly_balance_check4 = True

    if monthly_balance <= 1500:
        monthly_balance_check5 = True

    variables = {
        'monthly_bal_check1': monthly_balance_check1,
        'monthly_bal_check2': monthly_balance_check2,
        'monthly_bal_check3': monthly_balance_check3,
        'monthly_bal_check4': monthly_balance_check4,
        'monthly_bal_check5': monthly_balance_check5,
    }

    values = {
        'monthly_bal' : monthly_balance
    }

    return variables,values