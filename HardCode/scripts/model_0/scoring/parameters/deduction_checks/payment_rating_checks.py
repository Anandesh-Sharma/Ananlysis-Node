from HardCode.scripts.model_0.rejection_criteria.payment_rating.pay_rating import get_payment_rating
from HardCode.scripts.model_0.rejection_criteria.due_days.max_days import max_due_days

def payment_rating_check(cibil_df):
    payment_rating = get_payment_rating(cibil_df)
    due_days = max_due_days(cibil_df)

    pay_rating_check1 = False
    pay_rating_check2 = False
    pay_rating_check3 = False
    pay_rating_check4 = False

    due_days_check1 = False
    due_days_check2 = False
    due_days_check3 = False
    due_days_check4 = False

    if not payment_rating['pay_rating']:
        if not payment_rating['status']:
            pay_rating_check4 = True

        else:

            if payment_rating['pay_rating'] == '0':
                pay_rating_check1 = True
            if payment_rating['pay_rating'] == '1':
                pay_rating_check2 =True
            if payment_rating['pay_rating'] == '2':
                pay_rating_check3 = True

    else:
        if due_days < 30:
            due_days_check1 = True
        if 30 < due_days < 50:
            due_days_check2 = True
        if 50 < due_days < 60:
            due_days_check3 = True
        if due_days >60:
            due_days_check4 = True

    variables = {
        'pay_rating_check1': pay_rating_check1,
        'pay_rating_check2': pay_rating_check2,
        'pay_rating_check3': pay_rating_check3,
        'pay_rating_check4': pay_rating_check4,
        'due_days_check1': due_days_check1,
        'due_days_check2': due_days_check2,
        'due_days_check3': due_days_check3,
        'due_days_check4': due_days_check4

    }

    values = {
        'due_days': due_days,
        'payment_rating': payment_rating['pay_rating']
    }

    return variables,values

