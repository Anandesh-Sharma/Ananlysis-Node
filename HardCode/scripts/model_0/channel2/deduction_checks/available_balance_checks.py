from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.available_balance import find_info
from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.mean_available_balance import mean_available
from datetime import datetime

def available_balance_check(user_id):
    available_balance = find_info(user_id)

    bal,third_last_peak_bal,scnd_last_peak_bal,last_peak_bal = mean_available(user_id)

    available_balance_check1 = False
    available_balance_check2 = False
    available_balance_check3 = False
    available_balance_check4 = False
    available_balance_check5 = False
    available_balance_check6 = False
    available_balance_check7 = False

    if 40000 > bal > 30000:
        available_balance_check1 = True
    if 30000 > bal > 20000:
        available_balance_check2 = True
    if 20000 > bal > 10000:
        available_balance_check3 = True
    if 10000 > bal > 5000:
        available_balance_check4 = True
    if 5000 > bal > 1000:
        available_balance_check5 = True

    if 1000 > bal > 0:
        available_balance_check6 = True

    if bal <= 0 or bal > 40000:
        available_balance_check7 = True

    variables = {
        'available_balance_check1':available_balance_check1,
        'available_balance_check2': available_balance_check2,
        'available_balance_check3': available_balance_check3,
        'available_balance_check4': available_balance_check4,
        'available_balance_check5': available_balance_check5,
        'available_balance_check6': available_balance_check6,
        'available_balance_check7': available_balance_check7
    }

    values = {
        'available_balance' : available_balance,
        'mean_available_balance': bal,
        'last_month_peak_bal' :last_peak_bal,
        'scnd_last_month_peak_bal': scnd_last_peak_bal,
        'third_last_month_peak_bal': third_last_peak_bal,


    }

    return variables, values