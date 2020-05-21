#import numpy as np
from HardCode.scripts.loan_analysis.my_modules import sms_header_splitter, grouping, is_disbursed, is_closed, is_due, is_overdue, is_rejected
from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex, bank_headers
from HardCode.scripts.loan_analysis.current_open_details import get_current_open_details
from HardCode.scripts.Util import logger_1, conn
from datetime import datetime
import pytz
import warnings
#from pprint import pprint

warnings.filterwarnings('ignore')

'''
client
Particular app repay categories are
1)Repay msg captured successfully before taking loan from our app
2)client having status overdue or legal msg
3)client taken loan but due dates not over
4)Last msg from particular app was overdue then no msg retrieved from the same app (means msg deleted)
'''


def get_final_loan_details(cust_id):
    connect = conn()
    db = connect.analysis.parameters
    loan_data = fetch_user_data(cust_id)
    sms_header_splitter(loan_data)
    loan_data_grouped = grouping(loan_data)
    parameters = {}
    report = {
        "app" : [],
        "status" : [],
        "date" : [],
        "message" : [],
        "category" : []
    }
    try:
        current_date = datetime.now()
        start_date = datetime.strptime("2020-03-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        for app, data in loan_data_grouped:
            app_name = app
            data = data.sort_values(by = 'timestamp')
            data = data.reset_index(drop = True)
            if app not in list(loan_apps_regex.keys()) and app not in bank_headers:
                app = 'OTHER'
            if not data.empty and app not in bank_headers:
                last_message = str(data['body'].iloc[-1]).lower()
                last_message_date = datetime.strptime(str(data['timestamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
                report["message"].append(last_message)
                if last_message_date > start_date:
                    if is_disbursed(last_message, app):
                        if (current_date - last_message_date).days < 15:
                            report["app"].append(app_name)
                            report["status"].append('client taken loan but due dates not over')
                            report["date"].append(str(data['timestamp'].iloc[-1]))
                            report["category"].append(1)
                        else:
                            report["app"].append(app_name)
                            report["status"].append('last loan message was disbursed message and than no messgage even after 15 days (means msg deleted)')
                            report["date"].append(str(data['timestamp'].iloc[-1]))
                            report["category"].append(1)
                    elif is_closed(last_message, app):
                        report["app"].append(app_name)
                        report["status"].append('Repay msg captured successfully before taking loan from our app')
                        report["date"].append(str(data['timestamp'].iloc[-1]))
                        report["category"].append(0)
                    elif is_due(last_message, app):
                        if (current_date - last_message_date).days < 10:
                            report["app"].append(app_name)
                            report["status"].append('client taken loan but due dates not over')
                            report["date"].append(str(data['timestamp'].iloc[-1]))
                            report["category"].append(1)
                        else:
                            report["app"].append(app_name)
                            report["status"].append('Last msg from particular app was due/overdue then no msg retrieved from the same app (means msg deleted)')
                            report["date"].append(str(data['timestamp'].iloc[-1]))
                            report["category"].append(1)
                    elif is_overdue(last_message, app):
                        report["app"].append(app_name)
                        report["status"].append('Last msg from particular app was due/overdue then no msg retrieved from the same app (means msg deleted)')
                        report["date"].append(str(data['timestamp'].iloc[-1]))
                        report["category"].append(1)
                    elif is_rejected(last_message, app):
                        report["app"].append(app_name)
                        report["status"].append('user was rejected by this loan app')
                        report["date"].append(str(data['timestamp'].iloc[-1]))
                        report["category"].append(0)
                    else:
                        report["app"].append(app_name)
                        report["status"].append('no information detected')
                        report["date"].append(str(data['timestamp'].iloc[-1]))
                        report["category"].append(0)
        #pprint(report)
        parameters['last_loan_details'] = report
        db.update({'cust_id': cust_id},
                {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                            'parameters.loan_details': parameters}}, upsert=True)
        connect.close()
        get_current_open_details(cust_id)
        r = {'status': True, 'message': 'success'}
    except BaseException as e:
        print("error in last laon")
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
        connect.analysisresult.exception_bl0.insert_one(r)
        parameters['last_loan_details'] = report
        db.update({'cust_id': cust_id},
                    {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                            'parameters.loan_details': parameters}}, upsert=True)
        connect.close()

        return {'status':False,'message':str(e)}
    finally:
        return r

