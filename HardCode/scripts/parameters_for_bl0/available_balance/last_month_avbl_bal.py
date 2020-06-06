from HardCode.scripts.Util import conn
from datetime import datetime


def average_balance(user_id):
    connect = conn()
    user_data = connect.analysis.balance_sheet.find_one({'cust_id':user_id})['sheet']
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
    try:
        avl_balance = sorted(timestamp_bal[key], reverse=True)
    except KeyError as e:
        return {'status': False , 'message':str(e),'last_avbl_bal':0}
    if len(avl_balance) < 3:
        index = len(avl_balance)
    else:
        index = 3
    if index == 0:
        return {'status': True , 'message':'success','last_avbl_bal':0}
    else:
        return {'status': True, 'message': 'success', 'last_avbl_bal': (sum(avl_balance[:index]))/index}


