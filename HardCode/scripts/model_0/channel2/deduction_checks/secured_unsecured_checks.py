# from HardCode.scripts.parameters_for_bl0.secured_unsecured_loans import secure_unsecured_loan
from HardCode.scripts.Util import conn

def secured_unsecured_check(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})['parameters'][-1]
    secured_count = parameters['secured_loans']
    unsecured_count = parameters['unsecured_loans']
    # secured_count, unsecured_count , status = secure_unsecured_loan(cibil_df)

    # >>==>> secured unsecured loan count check
    secured_unsecured_check1 = False
    secured_unsecured_check2 = False
    secured_unsecured_check3 = False
    secured_unsecured_check4 = False
    secured_unsecured_check5 = False
    secured_unsecured_check6 = False
    secured_unsecured_check = False
    total = secured_count + unsecured_count


    if unsecured_count < total * 0.50:
        secured_unsecured_check1 = True
    if total * 0.50 < unsecured_count < total * 0.60:
        secured_unsecured_check2 = True
    if total * 0.60 < unsecured_count < total * 0.70:
        secured_unsecured_check3 = True
    if total * 0.70 < unsecured_count < total * 0.80:
        secured_unsecured_check4 = True
    if total * 0.80 < unsecured_count < total * 0.90:
        secured_unsecured_check5 = True
    if total * 0.90 < unsecured_count:
        secured_unsecured_check6 = True
    else:
        secured_unsecured_check = True

    connect.close()
    varibales = {
        'secured_unsecured_check1': secured_unsecured_check1,
        'secured_unsecured_check2': secured_unsecured_check2,
        'secured_unsecured_check3': secured_unsecured_check3,
        'secured_unsecured_check4': secured_unsecured_check4,
        'secured_unsecured_check5': secured_unsecured_check5,
        'secured_unsecured_check6': secured_unsecured_check6,
        'secured_unsecured_check': secured_unsecured_check
    }
    values = {
        'secured_unsecured_loans_count': {'secured': secured_count, 'unsecured_count': unsecured_count},
    }

    return varibales,values