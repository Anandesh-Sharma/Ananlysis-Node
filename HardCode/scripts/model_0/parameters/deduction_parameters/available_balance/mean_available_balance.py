from HardCode.scripts.Util import conn
from datetime import datetime
import itertools
from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.available_balance import find_info

def mean_available(user_id):
    connect = conn()
    date = datetime.now()
    mean_bal = 0
    data = connect.analysis.balance_sheet.find_one({'cust_id':user_id})
    if not data:
        return mean_bal
    data = data['sheet']
    bal = find_info(date,user_id)
    ac_no = bal['AC_NO']
    avbl_bal = []
    if ac_no:
        for i in range(len(data)):
            if data[i]['acc_no'] == ac_no:
                avbl_bal.append(data[i])

    avbl_bal.sort(key=lambda x: x['timestamp'])
    dfs = []
    key = lambda datum: datum['timestamp'].rsplit('-', 1)[0]

    for key, group in itertools.groupby(avbl_bal, key):
        dfs.append({'time': key, 'data': list(group)})

    all_max_bal = []
    for i in range(len(dfs)):
        list_bal = []
        for j in range(len(dfs[i]['data'])):
            if dfs[i]['data'][j]['Available Balance'] != 0:
                list_bal.append(dfs[i]['data'][j]['Available Balance'])
        all_max_bal.append(max(list_bal))

    if all_max_bal:
        mean_bal = sum(all_max_bal) / len(all_max_bal)


    return mean_bal