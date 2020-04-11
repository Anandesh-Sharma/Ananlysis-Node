import pandas as pd
import re
from HardCode.scripts.Util import conn
from datetime import datetime

def get_ecs_data(cust_id):
    try:
        connect = conn()
        db = connect.messagecluster.extra
        msgs = db.find_one({'cust_id': cust_id})
        ecs_data = pd.DataFrame(msgs['sms'])
        ecs_data = ecs_data.sort_values(by = 'timestamp')
        ecs_data.reset_index(drop = True, inplace = True)
    except:
        ecs_data = pd.DataFrame(columns = ['user_id', 'body', 'sender', 'timestamp', 'read'])
    return ecs_data

def get_ecs_bounce(cust_id):
    ecs_data = get_ecs_data(cust_id)
    ecs_bounce_list = []
    mask = []
    pattern_1 = r'ecs\sbounce\sho\schuka\shai'
    pattern_2 = r'ecs\s(?:transaction|request).*(rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*returned.*insufficient\s(?:balance|fund[s]?)'
    pattern_3 = r'unable\sto\sprocess.*ecs\srequest.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*insufficient\s(?:balance|fund[s]?)'
    if not ecs_data.empty:
        for i in range(ecs_data.shape[0]):
            message = str(ecs_data['body'][i].encode('utf-8')).lower()
            matcher_1 = re.search(pattern_1, message)
            matcher_2 = re.search(pattern_2, message)
            matcher_3 = re.search(pattern_3, message)

            if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None:
                ecs_bounce_list.append(i)
                mask.append(True)
            else:
                mask.append(False)
    else:
        pass
    return ecs_data.copy()[mask].reset_index(drop = True)

def get_count_ecs(cust_id):
    ecs = get_ecs_bounce(cust_id)
    count = 0
    if not ecs.empty:
        i = 0

        while i < ecs.shape[0]:
            date = datetime.strptime(ecs['timestamp'][i], "%Y-%m-%d %H:%M:%S")
            j=i+1

            while j < ecs.shape[0]:
                nxt_date= datetime.strptime(ecs['timestamp'][j], "%Y-%m-%d %H:%M:%S")
                diff = (nxt_date - date).days
                if diff < 24:
                    pass
                else:
                    i=j
                    count +=1
                    break
                j=j+1
            i=i+1

    return count





















