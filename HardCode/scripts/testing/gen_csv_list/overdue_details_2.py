from HardCode.scripts.Util import conn
import numpy as np
from datetime import datetime


def get_overdue_details(cust_id):
    overdue_days_list = []
    overdue_list = {'0_3_days': -1, '3_7_days': -1, '7_12_days': -1, '12_15_days': -1, 'more_than_15': -1}
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
                    current_date = datetime.strptime('2020-03-20 00:00:00', '%Y-%m-%d %H:%M:%S')
                    days = (current_date - disbursed_date).days
                    if days < 90:
                        total_loans += 1
                        if data[i][j]['overdue_days'] != -1:
                            overdue_days_list.append(data[i][j]['overdue_days'])

        for i in overdue_days_list:
            if i <= 3:
                overdue_list['0_3_days'] += 1
            elif i > 3 and i <= 7:
                overdue_list['3_7_days'] += 1
            elif i > 7 and i <= 12:
                overdue_list['7_12_days'] += 1
            elif i > 12 and i <= 15:
                overdue_list['12_15_days'] += 1
            else:
                overdue_list['more_than_15'] += 1
        if total_loans != 0:
            overdue_ratio = np.round(len(overdue_days_list) / total_loans, 4)
        else:
            overdue_ratio = 0
        report['overdue_ratio'] = overdue_ratio
        report['overdue_days_list'] = overdue_days_list
        report['total_loans'] = total_loans
        report['overdue_days_segment'] = overdue_list
        script_status = {"status": True, "message": "successfully updated overdue details on database"}


    except BaseException as e:

        script_status = {"status": False, "message": str(e)}
    finally:
        connect.close()
        return report
