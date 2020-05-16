#import numpy as np
#from HardCode.scripts.loan_analysis.my_modules import *
from HardCode.scripts.Util import logger_1, conn
#from HardCode.scripts.loan_analysis.preprocessing import preprocessing
from datetime import datetime
import pytz
import warnings

warnings.filterwarnings('ignore')

'''client
Particular app repay categories are
1)Repay msg captured successfully before taking loan from our app
2)client having status overdue or legal msg
3)client taken loan but due dates not over
4)Last msg from particular app was overdue then no msg retrieved from the same app (means msg deleted)
'''

def get_final_loan_details(cust_id):
    connect = conn()
    db = connect.analysis.parameters
    parameters = {}
    loan_info = connect.analysis.loan.find_one({'cust_id': cust_id})
    data = loan_info['complete_info']
    result = {}
    output = {}
    try:
        for app in data.keys():
            report = ''
            if data[app]:
                try:
                    last_index = list(data[app].keys())[-1]
                    target_loan = data[app][last_index]
                    if target_loan['disbursed_date'] != -1:
                        disbursed_date = datetime.strptime(str(target_loan['disbursed_date']), '%Y-%m-%d %H:%M:%S')
                        current_date = datetime.now()
                        if target_loan['closed_date'] == -1:
                            days = (current_date - disbursed_date).days
                            if target_loan['overdue_check'] > 0:
                                report = 'Client having status overdue or legal msg'
                            elif days > 25:
                                report = 'Has overdue and still not paid or maybe messags are deleted'
                            else:
                                report = 'Client taken loan but due dates not over'
                        else:
                            closed_date = datetime.strptime(str(target_loan['closed_date']), '%Y-%m-%d %H:%M:%S')
                            loan_duration = (closed_date - disbursed_date).days
                            if loan_duration < 15:
                                report = 'Loan closed successfully before taking loan from our app'
                            else:
                                overdue_days = (loan_duration - 15)
                                report = f'Loan closed after done overdue for {overdue_days} days'
                        result[app] = str(report)
                except:
                    r = {'status': False, 'message': str(e),
                        'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
                    connect.analysisresult.exception_bl0.insert_one(r)
                    result[app] = report
        # parameters['cust_id'] = cust_id
        parameters['last_loan_details'] = result
        # del parameters['cust_id']
        db.update({'cust_id': cust_id},
                  {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                            'parameters.loan_details': parameters}}, upsert=True)
        return {'status': True, 'message': 'success'}

    except BaseException as e:
        # print(e)
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
        connect.analysisresult.exception_bl0.insert_one(r)
        # parameters['cust_id'] = cust_id
        parameters['last_loan_details'] = result
        # del parameters['cust_id']
        db.update({'cust_id': cust_id},
                  {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                            'parameters.loan_details': parameters}}, upsert=True)

        return {'status':False,'message':str(e)}
    finally:
        connect.close()