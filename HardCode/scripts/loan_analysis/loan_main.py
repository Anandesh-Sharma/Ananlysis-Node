import numpy as np
from datetime import datetime
import warnings
import pytz
from HardCode.scripts.Util import logger_1, conn
from HardCode.scripts.loan_analysis.my_modules import *
from HardCode.scripts.loan_analysis.preprocessing import preprocessing

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
    a, user_app_list = preprocessing(cust_id)
    logger = logger_1('final_output', cust_id)
    report = {
        'TOTAL_LOAN_APPS': 0,
        'LOAN_APP_LIST': user_app_list,
        'CURRENT_OPEN': 0,
        'TOTAL_LOANS': 0,
        'PAY_WITHIN_30_DAYS': True,
        'OVERDUE_DAYS': -1,
        'OVERDUE_RATIO': 0,
        'AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS': -1,
        'CURRENT_OPEN_AMOUNT': [],
        'MAX_AMOUNT': -1,
        'empty': False
    }

    loan_disbursal_flow = {
        'app' : [],
        'disbursal_date' : []
    }

    # final output
    li = []
    li_ovrdue = []
    for i in a.keys():
        report['TOTAL_LOANS'] = report['TOTAL_LOANS'] + len(a[i].keys())
        #loan_disbursal_flow['app'].append(str(i))
        try:
            report['TOTAL_LOAN_APPS'] = len(a.keys())
            # freport['LOAN_APP_LIST'].append(str(i)
        except:
            logger.info("no loan apps")
        for j in a[i].keys():
            try:
                loan_disbursal_flow['app'].append(str(i))
                loan_disbursal_flow['disbursal_date'].append(a[i][j]['disbursed_date'])

            except:
                pass
            try:
                if a[i][j]['overdue_days'] != -1:
                    li_ovrdue.append(int(a[i][j]['overdue_days']))
            except:
                pass
            try:
                li.append(float(a[i][j]['loan_disbursed_amount']))
                li.append(float(a[i][j]['loan_closed_amount']))
                li.append(float(a[i][j]['loan_due_amount']))
            except:
                pass
            if a[i][j]['loan_duration'] > 30:
                report['PAY_WITHIN_30_DAYS'] = False

            now = datetime.now()
            now = timezone.localize(now)
            disbursed_date = timezone.localize(pd.to_datetime(a[i][j]['disbursed_date']))
            days = (now - disbursed_date).days

            if a[i][j]['closed_date'] == -1:
                if days < 30:
                    report['CURRENT_OPEN'] += 1

                    try:
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

                        # report['CURRENT_OPEN_AMOUNT'].append(float(a[i][j]['loan_disbursed_amount']))
                        #
                        # report['CURRENT_OPEN_AMOUNT'].append(float(a[i][j]['loan_due_amount']))

                    except BaseException as e:
                        continue
            else:
                continue

    try:
        report['OVERDUE_DAYS'] = max(li_ovrdue)
    except:
        pass
    try:
        report['AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS'] = np.round(sum(li_ovrdue) - max(li_ovrdue) / (len(li_ovrdue) - 1),2)
    except:
        pass
    try:
        report['OVERDUE_RATIO'] = np.round(len(li_ovrdue) / report['TOTAL_LOANS'], 2)
    except:
        report['OVERDUE_RATIO'] = 0
    try:
        report['LOAN_DATES'] = loan_disbursal_flow
    except:
        report['LOAN_DATES'] = {}

    try:
        report['MAX_AMOUNT'] = float(max(li))
    except:
        logger.info('no amount detect')
        report['empty'] = True
        script_status = {'status': True, 'message': 'success', 'result': report}
    try:
        client = conn()
        logger.info('Successfully connect to the database')
        report['modified_at'] = str(timezone.localize(datetime.now()))
        report['cust_id'] = cust_id
        report['complete_info'] = a

        client.analysis.loan.update_one({"cust_id": cust_id}, {"$set": report}, upsert=True)

        logger.info('Successfully upload result to the database')

        script_status = {'status': True, "message": "success", 'result': report}
    except Exception as e:
        logger.critical('Unable to connect to the database')
        return {'status': False, "message": str(e)}
    finally:
        client.close()

    return script_status
