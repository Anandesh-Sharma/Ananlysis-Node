from HardCode.scripts.loan_analysis.preprocessing import preprocessing
from HardCode.scripts.loan_analysis.my_modules import *
import numpy as np
import pandas as pd
from datetime import datetime


def get_overdue_details(cust_id):
    overdue_days_list = []
    overdue_ratio_3_months = 0
    total_loans_within_3_months = 0
    overdue_report = {
        '0-3_days': 0,
        '3-7_days': 0,
        '7-12_days': 0,
        '12-15_days': 0,
        'more_than_15': 0
    }

    data,loan_list = preprocessing(cust_id)
    try:
        for i in data.keys():
            for j in data[i].keys():
                if data[i][j]['disburse_date'] != -1:
                    disbursed_date = datetime.strptime(str(data[i][j]['disbursed_date']), '%Y-%m-%d %H:%M:%S')
                    current_date = datetime.strptime('2020-03-20 00:00:00', '%Y-%m-%d %H:%M:%S')
                    days = (current_date - disbursed_date).days
                    if days < 90:
                        total_loans_within_3_months += 1
                        if data[i][j]['overdue_days'] != -1:
                            overdue_days_list.append(data[i][j]['overdue_days'])

                    for i in overdue_days_list:
                        if i <= 3:
                            overdue_report['0-3_days'] += 1
                        elif (i > 3 and i <= 7):
                            overdue_report['3-7_days'] += 1
                        elif (i > 7 and i <= 12):
                            overdue_report['7-12_days'] += 1
                        elif (i > 12 and i <= 15):
                            overdue_report['12-15_days'] += 1
                        else:
                            overdue_report['more_than_15'] += 1
        overdue_ratio_3_months = np.round(len(overdue_days_list)/total_loans_within_3_months , 4)
        return overdue_ratio_3_months, overdue_report, total_loans_within_3_months
    except:
        return overdue_ratio_3_months, overdue_report,total_loans_within_3_months