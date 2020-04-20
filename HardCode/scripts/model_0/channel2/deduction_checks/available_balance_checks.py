from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.available_balance import find_info
from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.mean_available_balance import mean_available
from datetime import datetime

def available_balance_check(user_id):
    date = datetime.now()
    available_balance = find_info(user_id)

    mean_bal,third_last_peak_bal,scnd_last_peak_bal,last_peak_bal = mean_available(user_id)


    bal = available_balance['balance_on_loan_date']
    last_month = available_balance['last_month_bal']
    scnd_last_month = available_balance['second_last_month_bal']
    third_last_month = available_balance['third_last_month_bal']
    available_balance_check1 = False
    available_balance_check2 = False
    available_balance_check3 = False
    available_balance_check4 = False
    available_balance_check5 = False
    available_balance_check6 = False

    if bal > 35000 or bal < 0:
        available_balance_check1 = True
    if last_month ==0 or scnd_last_month ==0 or third_last_month == 0:
        available_balance_check2 = True
    if 35000 > bal > 25000:
        available_balance_check3 = True
    if 25000 > bal > 15000:
        available_balance_check4 = True
    if 15000 > bal >5000:
        available_balance_check5 = True

    if mean_bal < 5000:
        available_balance_check6 = True

    variables = {
        'available_balance_check1':available_balance_check1,
        'available_balance_check2': available_balance_check2,
        'available_balance_check3': available_balance_check3,
        'available_balance_check4': available_balance_check4,
        'available_balance_check5': available_balance_check5,
        'available_balance_check6': available_balance_check6
    }

    values = {
        'available_balance' : available_balance,
        'mean_available_balance': mean_bal,
        'last_month_peak_bal' :last_peak_bal,
        'scnd_last_month_peak_bal': scnd_last_peak_bal,
        'third_last_month_peak_bal': third_last_peak_bal,


    }

    return variables, values