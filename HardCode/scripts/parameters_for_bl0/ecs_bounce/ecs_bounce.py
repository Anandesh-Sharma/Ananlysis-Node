import pandas as pd
import re
from HardCode.scripts.Util import conn
import pytz
from datetime import datetime,timedelta

def get_ecs_data(cust_id):
    try:
        connect = conn()
        db = connect.messagecluster.ecs_msgs
        msgs = db.find_one({'cust_id': cust_id})
        ecs_data = pd.DataFrame(msgs['sms'])
        ecs_data = ecs_data.sort_values(by = 'timestamp')
        ecs_data.reset_index(drop = True, inplace = True)
        date = datetime.strptime('2020-03-20 00:00:00', '%Y-%m-%d %H:%M:%S')
        last_date = date - timedelta(weeks=13)
        mask = []
        for i in range(ecs_data.shape[0]):
            mask.append(date >= datetime.strptime(ecs_data['timestamp'][i], '%Y-%m-%d %H:%M:%S') > last_date)

        ecs_data = ecs_data[mask]
        ecs_data.reset_index(drop=True, inplace=True)
    except BaseException as e:
        ecs_data = pd.DataFrame(columns = ['user_id', 'body', 'sender', 'timestamp', 'read'])
    return ecs_data



def get_count_ecs(cust_id):
    ecs = get_ecs_data(cust_id)
    ecs_count = 0

    try:
        if not ecs.empty:
            i = 0
            while i < ecs.shape[0]:
                start_date = datetime.strptime(ecs['timestamp'][i], "%Y-%m-%d %H:%M:%S")
                end_date = datetime.strptime(f"{start_date.year}-{start_date.month}-28 00:00:00", "%Y-%m-%d %H:%M:%S")
                count = 0
                j = i + 1
                while j < ecs.shape[0]:
                    nxt_date = datetime.strptime(ecs['timestamp'][j], "%Y-%m-%d %H:%M:%S")
                    if nxt_date < end_date:
                        count += 1
                        if count > 1:
                            ecs_count += 1
                            break
                    else:
                        i=j
                        break
                    j += 1
                i += 1

    except BaseException as e:
        ecs_count = 0

    return ecs_count





















