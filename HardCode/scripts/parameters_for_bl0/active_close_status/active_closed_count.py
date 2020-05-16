from HardCode.scripts.parameters_for_bl0.active_close_status.account_type import closed , active
from HardCode.scripts.Util import conn
import pytz
from datetime import datetime

def get_active_closed(user_id,cibil_df):
    """
    :returns true if account status matches with anyone of the categories
             mentioned in the acc_status, otherwise returns false
    :rtype: bool
    """

    count_closed = 0
    count_active = 0
    status = False
    parameters = {}
    connect = conn()
    db = connect.analysis.parameters
    if cibil_df:
        if cibil_df['data'] is not None:
            if not cibil_df['data'].empty:
                if cibil_df['data'].shape[0] >= 5:
                    status = True
                    status_data = cibil_df['data']['account_status']
                    for st in status_data:
                        for a in closed.keys():
                            if str(st) == a:
                                count_closed += 1
                    for st in status_data:
                        for b in active.keys():
                            if str(st) == b:
                                count_active += 1
    parameters['cust_id'] = user_id
    db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                              'parameters.active': count_active,
                                              'parameters.closed': count_closed}}, upsert=True)

    return {'status':True,'message':'success'}

