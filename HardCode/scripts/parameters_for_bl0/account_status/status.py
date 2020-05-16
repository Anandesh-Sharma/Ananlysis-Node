from HardCode.scripts.parameters_for_bl0.account_status.account_types import acc_types
from datetime import  datetime
import pytz
from HardCode.scripts.Util import conn


def get_acc_status(user_id , cibil_df):
    """
    :returns true if account type matches with anyone of the categories
             mentioned in the account_types, otherwise returns false
    :rtype: bool
    """
    account_status = True
    status = False
    parameters = {}
    connect = conn()
    db = connect.analysis.parameters
    msg = "no data found"
    if cibil_df:
        if cibil_df['data'] is not None:  # ==>> this check is added cause in case cibil file is not uploaded
            if not cibil_df['data'].empty:  # ==> dataframe is returned as None instead of an empty df
                account = cibil_df['data']['account_status']
                for acc in account:
                    for c in acc_types.keys():
                        if str(acc) == c:
                            account_status = False
                            status = True
                            msg = 'success'
    parameters['cust_id'] = user_id
    db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                              'parameters.account_status': account_status}}, upsert=True)

    return {"status":True,'message':msg}
