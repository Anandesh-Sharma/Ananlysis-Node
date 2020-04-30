import numpy as np
from HardCode.scripts.loan_analysis.my_modules import *
from HardCode.scripts.Util import logger_1, conn
from HardCode.scripts.loan_analysis.preprocessing import preprocessing
from datetime import datetime
import pytz
import warnings

warnings.filterwarnings('ignore')


def get_final_loan_details(cust_id):
    data, app_list = preprocessing(cust_id)
    result = {}
    for app in data.keys():
        report = ''
        if data[app]:
            try:
                last_index = list(data[app].keys())[-1]
                target_loan = data[app][last_index]
                disbursed_date = datetime.strptime(str(target_loan['disbursed_date']), '%Y-%m-%d %H:%M:%S')
                current_date = datetime.now()
                if target_loan['closed_date'] == -1:
                    days = (current_date - disbursed_date).days
                    if days > 30:
                        report = 'has overdue and still not paid'
                    else:
                        report = 'currently open'
                else:
                    closed_date = datetime.strptime(str(target_loan['closed_date']), '%Y-%m-%d %H:%M:%S')
                    loan_duration = (closed_date - disbursed_date).days
                    if loan_duration < 15:
                        report = 'loan closed'
                    else:
                        overdue_days = (loan_duration - 15)
                        report = f'closed after done overdue for {overdue_days} days'
                result[app] = str(report)
            except BaseException as e:
                print(e)
                result[app] = report

    return result