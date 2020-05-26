from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex
from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.loan_analysis.my_modules import is_rejected, sms_header_splitter, grouping
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex, bank_headers
from HardCode.scripts.Util import conn
import pandas as pd
from datetime import datetime
import pytz

def get_rejection_count(cust_id):
    #target_apps = ['CASHBN', 'KREDTB', 'KREDTZ', 'LNFRNT', 'NIRAFN', 'SALARY']
    #premium_app_rejection_count = 0
    #normal_app_rejection_count = 0
    report = {}
    res = {}
    client = conn()
    db = client.analysis.loan
    parameters = {}
    status = True
    try:
        rejection_data = client.messagecluster.loanrejection.find_one({"cust_id" : cust_id})
        rejection_data = pd.DataFrame(rejection_data['sms'])
        if not rejection_data.empty:
            sms_header_splitter(rejection_data)
            rejection_data_grouped = grouping(rejection_data)
            for app, data in rejection_data_grouped:
                data = data.sort_values(by = 'timestamp')
                data = data.reset_index(drop = True)
                app_name = app
                message_dict = {
                    "date" : -1,
                    "message" : ""
                }
                if app not in list(loan_apps_regex.keys()) and app not in bank_headers:
                    app = 'OTHER'
                if app in list(loan_apps_regex.keys()):
                    msg_count = data.shape[0]
                    report[app_name] = msg_count
                    for i in range(data.shape[0]):
                        message = str(data['body'][i])
                        date = str(data['timestamp'][i])
                        message_dict["date"] = date
                        message_dict["message"] = message
                res[app_name] = message_dict
        #print(premium_app_rejection_count, normal_app_rejection_count)
        parameters["rejection_count"] = report
        msg = 'success'
    except BaseException as e:
        status = False
        msg = str(e)
        parameters["rejection_count"] = report
    finally:
        db.update_one({'cust_id': cust_id},
                  {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                            'loan_rejection': parameters}}, upsert=True)
        client.close()
        # return premium_app_rejection_count, normal_app_rejection_count, message_list
        return {'status':status, 'message':msg}

