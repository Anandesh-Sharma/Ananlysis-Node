# from HardCode.scripts.parameters_for_bl0.payment_rating import get_payment_rating
from HardCode.scripts.Util import conn

def payment_rating_check(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})['parameters'][-1]
    payment_rating = parameters['payment_rating']
    # payment_rating = get_payment_rating(cibil_df)


    pay_rating_check1 = False
    pay_rating_check2 = False
    pay_rating_check3 = False
    pay_rating_check4 = False
    pay_rating_check = False


    # if payment_rating['status'] and not payment_rating['data_status']:
    #     pay_rating_check4 = True

    # if payment_rating['status'] and payment_rating['data_status']:
    if payment_rating == '0':
        pay_rating_check1 = True
    if payment_rating == '1':
        pay_rating_check2 =True
    if payment_rating == '2':
        pay_rating_check3 = True


    # if not payment_rating['status']:
    #     pay_rating_check = True
    connect.close()
    variables = {
        'pay_rating_check1': pay_rating_check1,
        'pay_rating_check2': pay_rating_check2,
        'pay_rating_check3': pay_rating_check3,
        'pay_rating_check4': pay_rating_check4,
        'pay_rating_check': pay_rating_check,


    }

    values = {

        'payment_rating': payment_rating
    }

    return variables,values

