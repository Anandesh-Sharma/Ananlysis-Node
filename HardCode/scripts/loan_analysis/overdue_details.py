#from HardCode.scripts.loan_analysis.preprocessing import preprocessing
#from HardCode.scripts.loan_analysis.my_modules import *
from HardCode.scripts.Util import conn
import numpy as np
import pandas as pd
from datetime import datetime
import pytz

def get_overdue_details(cust_id):
    overdue_days_list = []
    script_status = {}
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
                            #overdue_days_list["overdue_days"].append(data[i][j]['overdue_days'])
                            #overdue_days_list["date"].append(str(data[i][j]['disbursed_date']))
                            overdue_days_list.append(data[i][j]['overdue_days'])
                            total_loans += 1
                elif data[i][j]['due_message_date'] != -1:
                    due_date = datetime.strptime(str(data[i][j]['due_message_date']), '%Y-%m-%d %H:%M:%S')
                    start_date = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                    if due_date > start_date:
                        if data[i][j]['overdue_days'] != -1:
                            #overdue_days_list['overdue_days'].append(data[i][j]['overdue_days'])
                            #overdue_days_list["date"].append(str(data[i][j]['disbursed_date']))
                            overdue_days_list.append(data[i][j]['overdue_days'])
                            total_loans += 1
                else:
                    pass
        if total_loans != 0:
            overdue_ratio = np.round(len(overdue_days_list)/total_loans, 4)
        else:
            overdue_ratio = 0
        report['overdue_ratio'] = overdue_ratio
        report['overdue_days_list'] = overdue_days_list
        report['total_loans'] = total_loans

    except BaseException as e:
        pass
    finally:
        connect.close()
        return report


