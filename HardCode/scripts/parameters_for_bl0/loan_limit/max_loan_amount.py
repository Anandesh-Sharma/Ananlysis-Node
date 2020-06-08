from HardCode.scripts.Util import conn
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_loan_max_amount(cust_id):
    connect = conn()
    loan_info = connect.analysis.loan.find_one({'cust_id': cust_id})
    data = loan_info['complete_info']
    current_date = datetime.now()
    start_date = current_date + relativedelta(months = 6)
    amount_list = []
    amount = 0
    try:
        for app in data.keys():
            for i in data[app].keys():
                disbursed_amount = data[app][i]['loan_disbursed_amount']
                due_amount = data[app][i]['loan_due_amount']
                closed_amount = data[app][i]['loan_closed_amount']
                disbursed_date = data[app][i]['disbursed_date']
                due_date = data[app][i]['due_date']

                if disbursed_date != -1:
                    disbursed_date = datetime.strptime(str(disbursed_date), "%Y-%m-%d %H:%M:%S")
                    if disbursed_date > start_date:
                        if disbursed_amount != -1:
                            amount_list.append(disbursed_amount)
                        if due_amount != -1:
                            amount_list.append(due_amount)
                        if closed_amount != -1:
                            amount_list.append(closed_amount)
                elif due_date != -1:
                    due_date = datetime.strptime(str(due_date), "%Y-%m-%d %H:%M:%S")
                    if due_date > start_date:
                        if disbursed_amount != -1:
                            amount_list.append(disbursed_amount)
                        if due_amount != -1:
                            amount_list.append(due_amount)
                        if closed_amount != -1:
                            amount_list.append(closed_amount)
        if amount_list:
            amount = max(amount_list)
        status = True
        msgs = 'success'
    except BaseException as e:
        status = False
        msg = str(e)
        pass
    finally:
        return {'status':status,'message':msg,'loan_amount':amount}

