from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex
from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.loan_analysis.my_modules import is_rejected, sms_header_splitter, grouping
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex, bank_headers
from HardCode.scripts.Util import conn
import pandas as pd
from datetime import datetime
import pytz

def get_rejection_count(cust_id):
    target_apps = ['CASHBN', 'KREDTB', 'KREDTZ', 'LNFRNT', 'NIRAFN', 'SALARY']
    premium_app_rejection_count = 0
    normal_app_rejection_count = 0
    message_list = []
    client = conn()
    db = client.analysis.parameters
    parameters = {}
    status = True
    try:
        rejection_data = client.messagecluster.loanrejection.find_one({"cust_id" : cust_id})
        rejection_data = pd.DataFrame(rejection_data['sms'])
        if not rejection_data.empty:
            rejection_data = rejection_data.sort_values(by = 'timestamp')
            rejection_data = rejection_data.reset_index(drop = True)
            sms_header_splitter(rejection_data)
            #print(rejection_data)
            for i in range(rejection_data.shape[0]):
                app = rejection_data['Sender-Name'][i]
                app_name = app
                if app not in list(loan_apps_regex.keys()) and app not in bank_headers:
                    app = 'OTHER'
                if app in list(loan_apps_regex.keys()):
                    message = str(rejection_data['body'][i]).lower()
                    if app_name in target_apps:
                        premium_app_rejection_count += 1
                        message_list.append(message)
                    else:
                        normal_app_rejection_count += 1
                        message_list.append(message)
        #print(premium_app_rejection_count, normal_app_rejection_count)
        parameters['premium_app_rejection'] = premium_app_rejection_count
        parameters['normal_app_rejection'] = normal_app_rejection_count
        parameters['rejection_messages'] = message_list
        msg = 'success'
    except BaseException as e:
        status = False
        msg = str(e)
        parameters['premium_app_rejection'] = premium_app_rejection_count
        parameters['normal_app_rejection'] = normal_app_rejection_count
        parameters['rejection_messages'] = message_list
    finally:
        db.update({'cust_id': cust_id},
                  {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                            'parameters.loan_rejection': parameters}}, upsert=True)
        client.close()
        # return premium_app_rejection_count, normal_app_rejection_count, message_list
        return {'status':status, 'message':msg}

