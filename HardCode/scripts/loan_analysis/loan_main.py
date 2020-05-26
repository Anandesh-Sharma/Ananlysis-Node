import numpy as np
from HardCode.scripts.loan_analysis.my_modules import *
from HardCode.scripts.Util import logger_1, conn
from datetime import datetime
import pytz
import warnings

warnings.filterwarnings('ignore')

timezone = pytz.timezone('Asia/Kolkata')

script_status = {}


def final_output(cust_id):
    '''
    Function for final output
    Parameters:
        df(dictionary)         :
            multi dictionary consists user's loan apps details
            disbursed_date(datetime) : date of disbursal
            closed_date(datetime)    : date of closed
            due_date(datetime)       : date of due
            loan_closed_amount(str)  : amount received at the closing time
            loan_disbursed_amount(str) : amount recieved at the disbursal time
            loan_due_amount(str)     : due messages amount info
            overdue_max_amount(str)  : maximum overdue amount
            loan_duration(int)       : duration of loan
        Returns:
            report(dictionary):
                pay_within_30_days(bool) :    if pay within 30 days
                current_open_amount      :    if loan is open than amount of loan
                total_loan               :    total loans
                current_open             :    current open loans
                max_amount               :    maximum loan amount in all loans
    '''
    #a, user_app_list = preprocessing(cust_id)
    logger = logger_1('final_output', cust_id)
    user_id = cust_id
    loan_disbursal_flow = {
        'app' : [],
        'disbursal_date' : []
    }
    report = {
        'TOTAL_LOAN_APPS': 0,
        'CURRENT_OPEN': 0,
        'TOTAL_LOANS': 0,
        'PAY_WITHIN_30_DAYS': True,
        'OVERDUE_DAYS': -1,
        'OVERDUE_RATIO': 0,
        'LOAN_DATES':loan_disbursal_flow,
        'AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS': -1,
        'CURRENT_OPEN_AMOUNT': [],
        'MAX_AMOUNT': -1,
        'empty': False
    }



    # final output
    li = []
    li_ovrdue = []
    client = conn()
    try:
        loan_cluster = client.analysis.loan.find_one({"cust_id" : cust_id})
        a = loan_cluster['complete_info']
        report['TOTAL_LOAN_APPS'] = len(a.keys())
        if report['TOTAL_LOAN_APPS'] != 0:
            for i in a.keys():
                report['TOTAL_LOANS'] = report['TOTAL_LOANS'] + len(a[i].keys())
                for j in a[i].keys():
                    loan_disbursal_flow['app'].append(str(i))
                    loan_disbursal_flow['disbursal_date'].append(a[i][j]['disbursed_date'])

                    if a[i][j]['overdue_days'] != -1:
                        li_ovrdue.append(int(a[i][j]['overdue_days']))
                    li.append(float(a[i][j]['loan_disbursed_amount']))
                    li.append(float(a[i][j]['loan_closed_amount']))
                    li.append(float(a[i][j]['loan_due_amount']))
                    if a[i][j]['loan_duration'] > 30:
                        report['PAY_WITHIN_30_DAYS'] = False

                    now = datetime.now()
                    now = timezone.localize(now)
                    if a[i][j]['disbursed_date'] != -1:
                        disbursed_date = timezone.localize(pd.to_datetime(a[i][j]['disbursed_date']))
                        days = (now - disbursed_date).days
                    else:
                        days = 31

                    if a[i][j]['closed_date'] == -1:
                        if days < 30:
                            report['CURRENT_OPEN'] += 1

                            disbursed_amount = float(a[i][j]['loan_disbursed_amount'])

                            disbursed_amount_from_due = float(a[i][j]['loan_due_amount'])

                            if int(disbursed_amount) != -1 and int(disbursed_amount_from_due) == -1:
                                report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount)

                            elif int(disbursed_amount) == -1 and int(disbursed_amount_from_due) != -1:
                                report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount_from_due)

                            elif int(disbursed_amount) == -1 and int(disbursed_amount_from_due) == -1:
                                report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount)

                            if int(disbursed_amount) != -1 and int(disbursed_amount_from_due) != -1:
                                report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount)

                    else:
                        continue

            report['LOAN_DATES'] = loan_disbursal_flow
            report['MAX_AMOUNT'] = float(max(li))

            if len(li_ovrdue) > 0:
                report['OVERDUE_DAYS'] = max(li_ovrdue)
                report['OVERDUE_RATIO'] = np.round(len(li_ovrdue) / report['TOTAL_LOANS'], 2)
            else:
                report['OVERDUE_DAYS'] = -1
                report['OVERDUE_RATIO'] = -1

            if len(li_ovrdue) > 1:
                report['AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS'] = np.round(sum(li_ovrdue) - max(li_ovrdue) / (len(li_ovrdue) - 1),2)
            else:
                report['AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS'] = -1

        else:
            pass

        return report
    except BaseException as e:
        import traceback
        traceback.print_tb(e.__traceback__)
        r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
        client.analysisresult.exception_bl0.insert_one(r)
    finally:
        client.close()
        return report
