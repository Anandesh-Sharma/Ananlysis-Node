import numpy as np
from HardCode.scripts.loan_analysis.my_modules import *
from HardCode.scripts.Util import logger_1, conn
from HardCode.scripts.loan_analysis.preprocessing import preprocessing
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
    report = {
        'TOTAL_LOAN_APPS': 0,
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
    try:
        client = conn()
        loan_cluster = client.analysis.loan.find_one({"cust_id" : cust_id})
        a = loan_cluster['complete_info']
        for i in a.keys():
            report['TOTAL_LOANS'] = report['TOTAL_LOANS'] + len(a[i].keys())
            #loan_disbursal_flow['app'].append(str(i))
            try:
                report['TOTAL_LOAN_APPS'] = len(a.keys())
                # freport['LOAN_APP_LIST'].append(str(i)
            except Exception as e:
                r = {'status': False, 'message': str(e),
                    'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
                client.analysisresult.exception_bl0.insert_one(r)
                logger.info("no loan apps")
            for j in a[i].keys():
                try:
                    loan_disbursal_flow['app'].append(str(i))
                    loan_disbursal_flow['disbursal_date'].append(a[i][j]['disbursed_date'])

                except Exception as e:
                    r = {'status': False, 'message': str(e),
                        'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
                    client.analysisresult.exception_bl0.insert_one(r)
                try:
                    if a[i][j]['overdue_days'] != -1:
                        li_ovrdue.append(int(a[i][j]['overdue_days']))
                except Exception as e:
                    r = {'status': False, 'message': str(e),
                        'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
                    client.analysisresult.exception_bl0.insert_one(r)
                try:
                    li.append(float(a[i][j]['loan_disbursed_amount']))
                    li.append(float(a[i][j]['loan_closed_amount']))
                    li.append(float(a[i][j]['loan_due_amount']))
                except Exception as e:
                    r = {'status': False, 'message': str(e),
                        'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
                    client.analysisresult.exception_bl0.insert_one(r)
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
        except Exception as e:
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
            client.analysisresult.exception_bl0.insert_one(r)
        try:
            report['AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS'] = np.round(sum(li_ovrdue) - max(li_ovrdue) / (len(li_ovrdue) - 1),2)
        except Exception as e:
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
            client.analysisresult.exception_bl0.insert_one(r)
        try:
            report['OVERDUE_RATIO'] = np.round(len(li_ovrdue) / report['TOTAL_LOANS'], 2)
        except Exception as e:
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
            client.analysisresult.exception_bl0.insert_one(r)
            report['OVERDUE_RATIO'] = 0
        try:
            report['LOAN_DATES'] = loan_disbursal_flow
        except Exception as e:
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
            client.analysisresult.exception_bl0.insert_one(r)
            report['LOAN_DATES'] = {}

        try:
            report['MAX_AMOUNT'] = float(max(li))
        except Exception as e:
            logger.info('no amount detect')
            report['empty'] = True
        try:
            client.analysis.parameters.update_one({"cust_id" : cust_id}, {"$set" : {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                                                    "parameters.loan_info": report}},  upsert = True)
            logger.info("successfully updated loan info data on database")
        except Exception as e:
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
            client.analysisresult.exception_bl0.insert_one(r)
            logger.info("unable to update loan info data on database")
        script_status = {'status': True, 'message': 'success', 'result': report}
    except BaseException as e:
        r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
        client.analysisresult.exception_bl0.insert_one(r)
        script_status = {"status" : False, "message" : str(e)}
    client.close()
    return script_status
