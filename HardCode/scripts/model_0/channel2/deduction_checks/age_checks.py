# from HardCode.scripts.parameters_for_bl0.age_of_oldest_trade import age_oldest_trade
from HardCode.scripts.Util import conn

def age_check(user_id):
    # age_of_oldest_trade , status = age_oldest_trade(cibil_df)
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})
    age_of_oldest_trade = parameters['parameters']['age_of_oldest_trade']
    # >>==>> age of oldest trade
    age_of_oldest_trade_check1 = False
    age_of_oldest_trade_check2 = False
    age_of_oldest_trade_check3 = False
    age_of_oldest_trade_check4 = False
    age_of_oldest_trade_check5 = False
    age_of_oldest_trade_check6 = False
    age_of_oldest_trade_check7 = False
    age_of_oldest_trade_check = False


    if age_of_oldest_trade >= 36:
        age_of_oldest_trade_check1 = True
    if 36 > age_of_oldest_trade >= 28:
        age_of_oldest_trade_check2 = True
    if 28 > age_of_oldest_trade >= 24:
        age_of_oldest_trade_check3 = True
    if 24 > age_of_oldest_trade >= 16:
        age_of_oldest_trade_check4 = True
    if 16 > age_of_oldest_trade >= 12:
        age_of_oldest_trade_check5 = True
    if 12 > age_of_oldest_trade >= 6:
        age_of_oldest_trade_check6 = True
    if 6 > age_of_oldest_trade > 0:
        age_of_oldest_trade_check7 = True

    else:
        age_of_oldest_trade_check = True
    connect.close()
    variables ={
        'age_of_oldest_trade_check1': age_of_oldest_trade_check1,
        'age_of_oldest_trade_check2': age_of_oldest_trade_check2,
        'age_of_oldest_trade_check3': age_of_oldest_trade_check3,
        'age_of_oldest_trade_check4': age_of_oldest_trade_check4,
        'age_of_oldest_trade_check5': age_of_oldest_trade_check5,
        'age_of_oldest_trade_check6': age_of_oldest_trade_check6,
        'age_of_oldest_trade_check7': age_of_oldest_trade_check7,
        'age_of_oldest_trade_check': age_of_oldest_trade_check
    }

    values = {
        'age_of_oldest_trade': age_of_oldest_trade
    }

    return variables,values