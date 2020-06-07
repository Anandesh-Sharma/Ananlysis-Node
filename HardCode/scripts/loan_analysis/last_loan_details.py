#import numpy as np
import pandas as pd
from HardCode.scripts.loan_analysis.my_modules import sms_header_splitter, grouping, is_disbursed, is_closed, is_due, is_overdue, is_rejected
from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex, bank_headers
# from HardCode.scripts.loan_analysis.current_open_details import get_current_open_details
from HardCode.scripts.Util import logger_1, conn
from datetime import datetime
import pytz
import warnings
import re
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

def is_promotional(extra_data_grouped, app, last_message_date):
    result = False
    last_extra_msg = ''
    for app_name, data in extra_data_grouped:
        if app == app_name:
            if not data.empty:
                data = data.sort_values(by = 'timestamp')
                data = data.reset_index(drop = True)
                last_extra_msg_date = datetime.strptime(str(data['timestamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
                last_extra_msg = data['body'].iloc[-1]
                if last_extra_msg_date > last_message_date:
                    result = True
    return result, last_extra_msg

def is_important_promotional(message):
    patterns = [
        r'received.*?application\sfor\s?(?:personal)?\sloan',
        r'hold.*loan\srepayments.*without\sany\spenalty',
        r'pay\s(?:rs\.?|inr).*get\s[0-9]+\s?days\sextra',
        r'avail\supto\srs.*?discount',
        r'(?:get)?\s?flat\srs.*cashback.*?loan',
        r'congratulations.*?your\scredit.*?upgraded',
        r'save\smoney\son\syour\snext.*?loan',
        r'eligible\sto\spostpone\spayment',
        r'loan.*?rescheduled\ssuccessfully',
        r'(?:offering|grant).*?moratorium\son\syour\s(?:emi|loan)',
        r'avail.*?\%\sdiscount\son\slate\sfee\scharges',
        r'moratorium\srequest\sis\son\shold',
        r'emi\sis\sin\sdelay.*?limited\speriod\soffer',
        r'successfully\ssigned.*?nach\sauto\sdebit',
        r'sent\sdhani\spoints.*?towards\snew\sloan',
        r'welcome\sgift\sfor\syou',
        r'pre-approved.*?loan.*?during\slockdown',
        r'welcome.*get\scash\sloan',
        r'loan\sapplication.*?submitted.*being\sverified',
        r'loan.*?temporarily\scancel[l]?ed.*?will\sget\sback',
        r'we\sare\sback.*?coupon\spackage\sadded',
        r'(?:credit|limit)\s(?:got|has\sbeen|has)\sincreased',
        r'increased\syour\scredit\slimit',
        r'[0-9]+\smins\saway\sfrom.*?disbursal'
    ]
    for pattern in patterns:
        message = message.lower()
        matcher = re.search(pattern, message)
        if matcher:
            return True
    else:
        return False


def get_final_loan_details(cust_id):
    connect = conn()

    loan_data = fetch_user_data(cust_id)
    sms_header_splitter(loan_data)
    loan_data_grouped = grouping(loan_data)
    extra_data = connect.messagecluster.extra.find_one({"cust_id" : cust_id})
    extra_data = pd.DataFrame(extra_data['sms'])
    sms_header_splitter(extra_data)
    extra_data_grouped = grouping(extra_data)
    report = {}
    try:
        current_date = datetime.now()
        for app, data in loan_data_grouped:
            #app_name = app
            data = data.sort_values(by = 'timestamp')
            data = data.reset_index(drop = True)
            #if app not in list(loan_apps_regex.keys()) and app not in bank_headers:
            #    app = 'OTHER'
            if not data.empty and app not in bank_headers:
                r = {
                "date" : -1,
                "status" : "",
                "category" : False,
                "message" : ""
                }
                print(app)
                last_message = str(data['body'].iloc[-1]).lower()
                last_message_date = datetime.strptime(str(data['timestamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
                category = data["category"].iloc[-1]
                if (current_date - last_message_date).days < 30:
                    print(app)
                    if category == "disbursed":
                        status, extra_msg = is_promotional(extra_data_grouped, app, last_message_date)
                        if status:
                            if is_important_promotional(extra_msg):
                                category = True
                                r["status"] = "after disbursed msg, promotional msg found which is related to loan, loan is open"
                            else:
                                category = False
                                r["status"] = "after disbursed msg, promotional msg found which is not related to loan, loan is closed by user"
                            r["date"] = str(data['timestamp'].iloc[-1])
                            r["category"] = category
                            r["message"] = extra_msg
                        else:
                            if (current_date - last_message_date).days < 15:
                                r["date"] = str(data['timestamp'].iloc[-1])
                                r["status"] = "client taken loan but due dates not over"
                                r["category"] = False
                                r["message"] = last_message
                            else:
                                r["date"] = str(data['timestamp'].iloc[-1])
                                r["status"] = "last loan message was disbursed message and than no messgage even after 15 days (means msg deleted)over"
                                r["category"] = True
                                r["message"] = last_message
                    elif category == "closed":
                        r["date"] = str(data['timestamp'].iloc[-1])
                        r["status"] = "Repay msg captured successfully before taking loan from our app"
                        r["category"] = False
                        r["message"] = last_message
                    elif category == "due":
                        status, extra_msg = is_promotional(extra_data_grouped, app, last_message_date)
                        if status:
                            if is_important_promotional(extra_msg):
                                category = True
                                r["status"] = "after due msg, promotional msg found which is related to loan, loan is open"
                            else:
                                category = False
                                r["status"] = "after due msg, promotional msg found which is not related to loan, loan is closed by user"
                            r["date"] = str(data['timestamp'].iloc[-1])
                            r["category"] = category
                            r["message"] = extra_msg
                        else:
                            if (current_date - last_message_date).days < 10:
                                r["date"] = str(data['timestamp'].iloc[-1])
                                r["status"] = "client taken loan but due dates not over"
                                r["category"] = False
                                r["message"] = last_message
                            else:
                                r["date"] = str(data['timestamp'].iloc[-1])
                                r["status"] = "Last msg from particular app was due then no msg retrieved from the same app (means msg deleted)"
                                r["category"] = True
                                r["message"] = last_message
                    elif category == "overdue":
                        status, extra_msg = is_promotional(extra_data_grouped, app, last_message_date)
                        if status:
                            if is_important_promotional(extra_msg):
                                category = True
                                r["status"] = "after overdue msg, promotional msg found which is related to loan, loan is open"
                            else:
                                category = False
                                r["status"] = "after overdue msg, promotional msg found which is not related to loan, loan is closed by user"
                            r["date"] = str(data['timestamp'].iloc[-1])
                            r["category"] = category
                            r["message"] = extra_msg
                        else:
                            r["date"] = str(data['timestamp'].iloc[-1])
                            r["status"] = "Last msg from particular app was overdue then no msg retrieved from the same app (means msg deleted)"
                            r["category"] = True
                            r["message"] = last_message
                    elif category == "rejected":
                        r["date"] = str(data['timestamp'].iloc[-1])
                        r["status"] = "user was rejected by this loan app"
                        r["category"] = False
                        r["message"] = last_message
                    else:
                        r["date"] = str(data['timestamp'].iloc[-1])
                        r["status"] = "no information detected"
                        r["category"] = False
                        r["message"] = last_message
                else:
                    print(app)
                    r["date"] = str(data['timestamp'].iloc[-1])
                    r["status"] = "no message found from more than last 30 days, hence considered as closed"
                    r["category"] = False
                    r["message"] = last_message
                report[app] = r
        connect.close()
    except BaseException as e:
        import traceback
        traceback.print_tb(e.__traceback__)
        print("error in last loan")
        res= {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
        connect.analysisresult.exception_bl0.insert_one(res)
        connect.close()
    finally:
        return report