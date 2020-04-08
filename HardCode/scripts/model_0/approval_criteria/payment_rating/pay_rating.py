

def get_payment_rating(cibil_df):
    """
    :returns false if payment rating is 3,4,5,6  otherwise returns true
    :rtype: bool
    """
    status = True
    bad_rating = ['3', '4', '5', '6', 'L', 'D']
    pay_rating = []
    if cibil_df['data'] is not None:
        if not cibil_df['data'].empty:
            if cibil_df['message'] == 'None':
                status = False
            else:
                for i in cibil_df['data']['payment_rating']:
                    pay_rating.append(i)
                for pr in pay_rating:
                    for br in bad_rating:
                        if str(pr) == br:
                            status = False
                            break
    else:
        status = False

    return {'status': status, 'pay_rating': pay_rating}