

def get_payment_rating(cibil_df):
    """
    :returns false if payment rating is 3,4,5,6  otherwise returns true
    :rtype: bool
    """

    data_status = True
    good_rating = ['0','1','2']
    bad_rating = ['3', '4', '5', '6', 'L', 'D']
    pay_rating = []
    rating = ""
    if cibil_df:
        if cibil_df['data'] is not None:
            if not cibil_df['data'].empty:
                if cibil_df['message'] == 'None':
                    data_status = False
                else:
                    for i in cibil_df['data']['payment_rating']:
                        pay_rating.append(i)
                    for pr in pay_rating:
                        for gr in good_rating:
                            if str(pr) == gr:
                                rating = str(pr)
                                data_status = True
                        for br in bad_rating:
                            if str(pr) == br:
                                rating = str(pr)
                                data_status = False
                                break
    return rating