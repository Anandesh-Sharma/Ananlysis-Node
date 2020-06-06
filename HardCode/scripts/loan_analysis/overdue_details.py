from datetime import datetime

import numpy as np
import pandas as pd
import pytz

from HardCode.scripts.Util import conn


def get_overdue_details(cust_id):
    overdue_days_list = []
    script_status = {"status":True}
    overdue_ratio = 0
    total_loans = 0
    report = {}
    connect = conn()

    try:

        loan_info = connect.analysis.loan.find_one({'cust_id': cust_id})
        data = loan_info['complete_info']

        for i in data.keys():
            for j in data[i].keys():
                if data[i][j]['disbursed_date'] != -1:
                    disbursed_date = datetime.strptime(str(data[i][j]['disbursed_date']), '%Y-%m-%d %H:%M:%S')
                    start_date = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                    if disbursed_date > start_date:
                        if data[i][j]['overdue_days'] != -1:
                            overdue_days_list.append(data[i][j]['overdue_days'])
                            total_loans += 1
                elif data[i][j]['due_date'] != -1:
                    due_date = datetime.strptime(str(data[i][j]['due_message_date']), '%Y-%m-%d %H:%M:%S')
                    start_date = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                    if due_date > start_date:
                        if data[i][j]['overdue_days'] != -1:
                            overdue_days_list.append(data[i][j]['overdue_days'])
                            total_loans += 1
        if total_loans != 0:
            overdue_ratio = np.round(len(overdue_days_list)/total_loans, 4)
        else:
            overdue_ratio = 0
        report['overdue_ratio'] = overdue_ratio
        report['overdue_days_list'] = overdue_days_list
        report['total_loans'] = total_loans
        script_status['report']=report
    except BaseException as e:
        script_status['status']=False
        script_status['message']=str(e)
    finally:
        connect.close()
        return script_status

