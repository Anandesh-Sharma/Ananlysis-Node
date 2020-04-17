from HardCode.scripts.model_0.parameters.deduction_parameters.payment_rating.pay_rating import get_payment_rating


def payment_rating_check(cibil_df):
    payment_rating = get_payment_rating(cibil_df)


    pay_rating_check1 = False
    pay_rating_check2 = False
    pay_rating_check3 = False
    pay_rating_check4 = False
    pay_rating_check = False


    if payment_rating['status'] and not payment_rating['data_status']:
        pay_rating_check4 = True

    if payment_rating['status'] and payment_rating['data_status']:
        if payment_rating['pay_rating'] == '0':
            pay_rating_check1 = True
        if payment_rating['pay_rating'] == '1':
            pay_rating_check2 =True
        if payment_rating['pay_rating'] == '2':
            pay_rating_check3 = True


    if not payment_rating['status']:
        pay_rating_check = True

    variables = {
        'pay_rating_check1': pay_rating_check1,
        'pay_rating_check2': pay_rating_check2,
        'pay_rating_check3': pay_rating_check3,
        'pay_rating_check4': pay_rating_check4,
        'pay_rating_check': pay_rating_check,


    }

    values = {

        'payment_rating': payment_rating['pay_rating']
    }

    return variables,values

