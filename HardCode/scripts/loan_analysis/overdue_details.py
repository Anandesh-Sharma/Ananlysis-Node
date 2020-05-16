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

    try:
        connect = conn()
        loan_info = connect.analysis.loan.find_one({'cust_id': cust_id})
        data = loan_info['complete_info']

        for i in data.keys():
            for j in data[i].keys():
                total_loans += 1
                if data[i][j]['disburse_date'] != -1:
                    disbursed_date = datetime.strptime(str(data[i][j]['disbursed_date']), '%Y-%m-%d %H:%M:%S')
                    current_date = datetime.strptime('2020-03-20 00:00:00', '%Y-%m-%d %H:%M:%S')
                    days = (current_date - disbursed_date).days
                    if data[i][j]['overdue_days'] != -1:
                        overdue_days_list.append(data[i][j]['overdue_days'])

        overdue_ratio = np.round(len(overdue_days_list)/total_loans, 4)
        report['overdue_ratio'] = overdue_ratio
        report['overdue_days_list'] = overdue_days_list
        report['total_loans'] = total_loans
        report['cust_id'] = cust_id
        db = connect.analysis.parameters

        db.update_one({"cust_id" : cust_id}, {"$set" : {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), "parameters.loan_info ": report}})
        script_status = {"status" : True, "message" : "successfully updated overdue details on database"}

    except BaseException as e:
        script_status = {"status" : False, "message" : str(e)}
    finally:
        return script_status
