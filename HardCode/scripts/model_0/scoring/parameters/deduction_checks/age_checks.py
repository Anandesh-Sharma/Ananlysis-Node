from HardCode.scripts.model_0.rejection_criteria.age_of_oldest_trade.age import age_oldest_trade

def age_check(cibil_df):
    age_of_oldest_trade = age_oldest_trade(cibil_df)

    # >>==>> age of oldest trade
    age_of_oldest_trade_check1 = False
    age_of_oldest_trade_check2 = False
    age_of_oldest_trade_check3 = False
    age_of_oldest_trade_check4 = False
    age_of_oldest_trade_check5 = False
    age_of_oldest_trade_check6 = False
    age_of_oldest_trade_check7 = False

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
    if age_of_oldest_trade < 6:
        age_of_oldest_trade_check7 = True

    variables ={
        'age_of_oldest_trade_check1': age_of_oldest_trade_check1,
        'age_of_oldest_trade_check2': age_of_oldest_trade_check2,
        'age_of_oldest_trade_check3': age_of_oldest_trade_check3,
        'age_of_oldest_trade_check4': age_of_oldest_trade_check4,
        'age_of_oldest_trade_check5': age_of_oldest_trade_check5,
        'age_of_oldest_trade_check6': age_of_oldest_trade_check6,
        'age_of_oldest_trade_check7': age_of_oldest_trade_check7
    }

    values = {
        'age_of_oldest_trade': age_of_oldest_trade
    }

    return variables,values