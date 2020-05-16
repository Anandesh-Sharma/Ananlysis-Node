from HardCode.scripts.Util import conn
from datetime import datetime
import itertools
import pytz
from HardCode.scripts.parameters_for_bl0.available_balance.available_balance import find_info

def mean_available(user_id):
    connect = conn()
    mean_bal = -1
    avg_bal = -1
    third_last_month = {}
    scnd_last_month = {}
    last_month = {}
    parameters = {}
    output = {}
    data = connect.analysis.balance_sheet.find_one({'cust_id':user_id})
    db = connect.analysis.parameters
    if not data:
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.mean_bal': mean_bal,
                                                  'parameters.last_month_peak': {},
                                                  'parameters.second_last_month_peak': {},
                                                  'parameters.third_last_month_peak': {},
                                                  'parameters.avg_balance': avg_bal}}, upsert=True)
        return {'status':True,'message':'balance sheet not found'}
    data = data['sheet']
    parameters = connect.analysis.parameters.find_one({'cust_id':user_id})
    ac_no = parameters['parameters']['available_balance']['AC_NO']
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
    monthly_avg_bal = []
    dfs = dfs[-3:]

    for i in range(len(dfs)):
        list_bal = []
        list_time = []
        list_msg = []

        for j in range(len(dfs[i]['data'])):
            if dfs[i]['data'][j]['Available Balance'] != 0:
                list_bal.append(int(dfs[i]['data'][j]['Available Balance']))
                list_time.append(dfs[i]['data'][j]['timestamp'])
                list_msg.append(dfs[i]['data'][j]['body'])
        if list_bal:
            monthly_avg_bal.append(sum(list_bal)/len(list_bal))
            maxpos = list_bal.index(max(list_bal))
            all_max_bal.append(max(list_bal))
            all_max_time.append(list_time[maxpos])
            all_max_msg.append(list_msg[maxpos])

    if all_max_bal:
        mean_bal = sum(all_max_bal) / len(all_max_bal)

    if monthly_avg_bal:
        avg_bal = sum(monthly_avg_bal)/len(monthly_avg_bal)
    try:
        third_last_month = {'max_amt':all_max_bal[-3],'datetime':all_max_time[-3],'msg':all_max_msg[-3]}
    except:
        third_last_month = {}
    try:
        scnd_last_month = {'max_amt': all_max_bal[-2], 'datetime': all_max_time[-2], 'msg': all_max_msg[-2]}
    except:
        scnd_last_month = {}
    try:
        last_month = {'max_amt': all_max_bal[-1], 'datetime': all_max_time[-1], 'msg': all_max_msg[-1]}
    except:
        last_month = {}

    parameters['cust_id'] = user_id
    db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                   'parameters.mean_bal':mean_bal,
                                              'parameters.last_month_peak':last_month,
                                              'parameters.second_last_month_peak': scnd_last_month,
                                            'parameters.third_last_month_peak':  third_last_month,
                                            'parameters.avg_balance': avg_bal}}, upsert=True)
    return {'status':True,'message':'success'}

