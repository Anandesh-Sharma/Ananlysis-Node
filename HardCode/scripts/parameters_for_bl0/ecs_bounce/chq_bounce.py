import pandas as pd
import re
from HardCode.scripts.Util import conn
import pytz
from datetime import datetime, timedelta

def get_chq_bounce_data(cust_id):
    try:
        connect = conn()
        db = connect.messagecluster.cheque_bounce_msgs
        msgs = db.find_one({'cust_id': cust_id})
        cb_data = pd.DataFrame(msgs['sms'])
        cb_data = cb_data.sort_values(by = 'timestamp')
        cb_data.reset_index(drop = True, inplace = True)
        date = datetime.strptime('2020-03-20 00:00:00', '%Y-%m-%d %H:%M:%S')
        last_date1 = date - timedelta(weeks=13)
        mask = []
        for i in range(cb_data.shape[0]):
            mask.append(date >= datetime.strptime(cb_data['timestamp'][i], '%Y-%m-%d %H:%M:%S') > last_date1)
        cb_data = cb_data[mask]
        cb_data.reset_index(drop=True, inplace=True)
    except:
        cb_data = pd.DataFrame(columns = ['user_id', 'body', 'sender', 'timestamp', 'read'])
    return cb_data


def get_count_cb(cust_id):
    cb = get_chq_bounce_data(cust_id)
    count = 0
    try:
        if not cb.empty:
            i = 0

            while i < cb.shape[0]:
                date = datetime.strptime(cb['timestamp'][i], "%Y-%m-%d %H:%M:%S")
                j=i+1

                while j < cb.shape[0]:
                    nxt_date= datetime.strptime(cb['timestamp'][j], "%Y-%m-%d %H:%M:%S")
                    diff = (nxt_date - date).days
                    if diff < 1:
                        pass
                    else:
                        i=j
                        count +=1
                        status = True
                        break
                    j=j+1
                i=i+1

    except BaseException as e:
        count = 0
    return count
