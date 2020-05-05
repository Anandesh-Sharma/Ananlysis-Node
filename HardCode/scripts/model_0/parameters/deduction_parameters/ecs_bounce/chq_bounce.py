import pandas as pd
import re
from HardCode.scripts.Util import conn
from datetime import datetime, timedelta

def get_chq_bounce_data(cust_id):
    try:
        connect = conn()
        db = connect.messagecluster.extra
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

def get_chq_bounce(cust_id):
    cb_data = get_chq_bounce_data(cust_id)
    chq_bounce_list = []
    mask = []
    pattern_1 = r'auto(?:\-|\s)debit\sattempt\sfailed.*cheque\s(?:bounce[d]?|dishono[u]?r)\scharge[s]?'
    pattern_2 = r'cheque.*(?:dishono[u]?red|bounce[d]?).*insufficient\s(?:balance|fund[s]?|bal)'

    if not cb_data.empty:
        for i in range(cb_data.shape[0]):
            message = str(cb_data['body'][i].encode('utf-8')).lower()
            matcher_1 = re.search(pattern_1, message)
            matcher_2 = re.search(pattern_2, message)

            if matcher_1 is not None or matcher_2 is not None:
                chq_bounce_list.append(i)
                mask.append(True)
            else:
                mask.append(False)
    else:
        pass
    return cb_data.copy()[mask].reset_index(drop = True)

def get_count_cb(cust_id):
    cb = get_chq_bounce(cust_id)
    count = 0
    status = False
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

    return count , status
