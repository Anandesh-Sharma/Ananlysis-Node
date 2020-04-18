from HardCode.scripts.Util import conn
from datetime import datetime
import itertools
from HardCode.scripts.model_0.parameters.deduction_parameters.available_balance.available_balance import find_info

def mean_available(user_id):
    connect = conn()
    date = datetime.now()
    mean_bal = 0
    third_last_month = {}
    scnd_last_month = {}
    last_month = {}
    data = connect.analysis.balance_sheet.find_one({'cust_id':user_id})
    if not data:
        return mean_bal,third_last_month,scnd_last_month,last_month
    data = data['sheet']
    bal = find_info(date,user_id)
    ac_no = bal['AC_NO']
    avbl_bal = []
    if ac_no:
        for i in range(len(data)):
            acn = str(data[i]['acc_no'])[-3:]
            if acn == str(ac_no):
                avbl_bal.append(data[i])
    avbl_bal.sort(key=lambda x: x['timestamp'])
    dfs = []
    key = lambda datum: datum['timestamp'].rsplit('-', 1)[0]

    for key, group in itertools.groupby(avbl_bal, key):
        dfs.append({'time': key, 'data': list(group)})

    all_max_bal = []
    all_max_time = []
    all_max_msg = []
    dfs = dfs[-3:]

    for i in range(len(dfs)):
        list_bal = []
        list_time = []
        list_msg = []

        for j in range(len(dfs[i]['data'])):
            if dfs[i]['data'][j]['Available Balance'] != 0:
                list_bal.append(dfs[i]['data'][j]['Available Balance'])
                list_time.append(dfs[i]['data'][j]['timestamp'])
                list_msg.append(dfs[i]['data'][j]['body'])
        if list_bal:
            maxpos = list_bal.index(max(list_bal))
            all_max_bal.append(max(list_bal))
            all_max_time.append(list_time[maxpos])
            all_max_msg.append(list_msg[maxpos])

    if all_max_bal:
        mean_bal = sum(all_max_bal) / len(all_max_bal)
    try:
        third_last_month = {'max_amt':all_max_bal[0],'datetime':all_max_time[0],'msg':all_max_msg[0]}
    except:
        third_last_month = {}
    try:
        scnd_last_month = {'max_amt': all_max_bal[1], 'datetime': all_max_time[1], 'msg': all_max_msg[1]}
    except:
        scnd_last_month = {}
    try:
        last_month = {'max_amt': all_max_bal[2], 'datetime': all_max_time[2], 'msg': all_max_msg[2]}
    except:
        last_month = {}



    return mean_bal,third_last_month,scnd_last_month,last_month