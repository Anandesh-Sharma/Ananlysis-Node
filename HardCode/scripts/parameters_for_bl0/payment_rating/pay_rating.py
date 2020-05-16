from datetime import datetime, date
from HardCode.scripts.Util import conn
import pytz

def get_payment_rating(user_id, cibil_df):
    """
    :returns false if payment rating is 3,4,5,6  otherwise returns true
    :rtype: bool
    """
    status = False
    data_status = True
    good_rating = ['0','1','2']
    bad_rating = ['3', '4', '5', '6', 'L', 'D']
    pay_rating = []
    rating = ""
    connect = conn()
    db  = connect.analysis.parameters
    parameters = {}
    msg = "no data found"
    if cibil_df:
        if cibil_df['data'] is not None:
            if not cibil_df['data'].empty:
                if cibil_df['message'] == 'None':
                    data_status = False
                else:
                    status = True
                    msg = 'success'
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
        else:
            data_status = False
    parameters['cust_id'] = user_id
    db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                              'parameters.payment_rating': rating}}, upsert=True)
    return {'status':True,'message':msg}