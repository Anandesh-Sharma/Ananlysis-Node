# from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.available_balance import find_info
# from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.mean_available_balance import mean_available
# from datetime import datetime
from HardCode.scripts.Util import conn
def available_balance_check(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id': user_id})['parameters'][-1]
    bal = parameters['mean_bal']
    last_peak_bal = parameters['last_month_peak']
    scnd_last_peak_bal = parameters['second_last_month_peak']
    third_last_peak_bal = parameters['third_last_month_peak']
    avg_bal = parameters['avg_balance']
    available_balance = parameters['available_balance']
    # available_balance = find_info(user_id)

    # bal,third_last_peak_bal,scnd_last_peak_bal,last_peak_bal,avg_bal = mean_available(user_id)

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
    connect.close()
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
        'avg_bal_of_3_month':avg_bal


    }

    return variables, values