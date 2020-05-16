from datetime import datetime, date
from HardCode.scripts.Util import conn
import pytz


def secure_unsecured_loan(user_id,cibil_df):
    """
    :param user_id , cibil_df
    :returns the count of secured and unsecured loans calculated from the cibil dataframe
    :rtype: dict
    """
    secured_loan = 0
    unsecured_loan = 0
    connect = conn()
    db  = connect.analysis.parameters
    parameters = {}
    status = False
    msg = 'no data found'
    if cibil_df:
        if cibil_df['data'] is not None:  # ==>> this check is added cause in case cibil file is not uploaded
            if not cibil_df['data'].empty:  # ==> dataframe is returned as None instead of an empty df
                secured_loan = int(cibil_df['data']['secured_loan'].iloc[-1])
                unsecured_loan = int(cibil_df['data']['unsecured_loan'].iloc[-1])
                status =True
                msg = 'success'
    parameters['cust_id'] = user_id
    db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                              'parameters.secured_loans': secured_loan,'parameters.unsecured_loans': unsecured_loan}}, upsert=True)
    return {'status':True,'message':msg}
