from HardCode.scripts.Util import conn
from datetime import datetime


def average_balance(user_id):

    connect = conn()
    user_data = connect.analysis.balance_sheet.find_one({'cust_id':user_id})
    timestamp_bal = {}
    max_timestamp = datetime.now()
    if(max_timestamp.strftime("%m") == '01'):
        prev_month = '12'
        prev_year = int(max_timestamp.strftime("%Y")) - 1
    else:
        prev_month = str(int(max_timestamp.strftime("%m")) - 1)
        if len(prev_month) == 1:
            prev_month = "0" + prev_month
        prev_year = max_timestamp.strftime("%Y")

    if user_data and user_data['sheet']:
        user_data = user_data['sheet']
    else:
        return {'status': True, 'message': 'success', 'last_avbl_bal': 0,
                'last_avbl_bal_feb': 0, 'last_avbl_bal_mar': 0}

    for data in user_data:
        timestamp = datetime.strptime(data['timestamp'],"%Y-%m-%d %H:%M:%S")
        key = str(timestamp.strftime("%m")) +"_"+str(timestamp.strftime("%Y"))
        try:
            x = timestamp_bal[key]
            if float(data['Available Balance']) != 0:
                timestamp_bal[key].append(float(data['Available Balance']))
        except KeyError:
            if float(data['Available Balance']) != 0:
                timestamp_bal[key] = [float(data['Available Balance'])]
            else:
                timestamp_bal[key] = []

    key = str(prev_month) + "_" + str(prev_year)
    key1 = '02_2020'
    key2 = '03_2020'
    if key in timestamp_bal.keys():
        avl_balance = sorted(timestamp_bal[key], reverse=True)
    else:
        avl_balance = []
    if key1 in timestamp_bal.keys():
        avl_balance_feb = sorted(timestamp_bal[key1], reverse=True)
    else:
        avl_balance_feb = []
    if key2 in timestamp_bal.keys():
        avl_balance_mar = sorted(timestamp_bal[key2], reverse=True)
    else:
        avl_balance_mar = []

    if len(avl_balance) < 3:
        index = len(avl_balance)
    else:
        index = 3
    if len(avl_balance_feb) < 3:
        index1 = len(avl_balance_feb)
    else:
        index1 = 3
    if len(avl_balance_mar) < 3:
        index2 = len(avl_balance_mar)
    else:
        index2 = 3

    if index == 0:
        last_avbl_bal= 0
    else:
        last_avbl_bal = (sum(avl_balance[:index]))/index

    if index1 == 0:
        last_avbl_bal_feb = 0
    else:
        last_avbl_bal_feb = (sum(avl_balance_feb[:index1])) / index1
    if index2 == 0:
        last_avbl_bal_mar = 0
    else:
        last_avbl_bal_mar = (sum(avl_balance_mar[:index2])) / index2


    return {'status': True, 'message': 'success', 'last_avbl_bal': last_avbl_bal,
            'last_avbl_bal_feb':last_avbl_bal_feb,'last_avbl_bal_mar':last_avbl_bal_mar}


