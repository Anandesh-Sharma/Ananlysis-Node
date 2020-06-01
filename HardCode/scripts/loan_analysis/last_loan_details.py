#import numpy as np
from HardCode.scripts.loan_analysis.my_modules import sms_header_splitter, grouping, is_disbursed, is_closed, is_due, is_overdue, is_rejected
from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex, bank_headers
# from HardCode.scripts.loan_analysis.current_open_details import get_current_open_details
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

    loan_data = fetch_user_data(cust_id)
    sms_header_splitter(loan_data)
    loan_data_grouped = grouping(loan_data)

    report = {}
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
                r = {
                "date" : -1,
                "status" : "",
                "category" : False,
                "message" : ""
                }
                last_message = str(data['body'].iloc[-1]).lower()
                last_message_date = datetime.strptime(str(data['timestamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
                # report["message"] = (last_message)
                category = data["category"].iloc[-1]
                if last_message_date > start_date:
                    if category == "disbursed":
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
                        if (current_date - last_message_date).days < 10:
                            r["date"] = str(data['timestamp'].iloc[-1])
                            r["status"] = "client taken loan but due dates not over"
                            r["category"] = False
                            r["message"] = last_message
                        else:
                            r["date"] = str(data['timestamp'].iloc[-1])
                            r["status"] = "Last msg from particular app was due/overdue then no msg retrieved from the same app (means msg deleted)"
                            r["category"] = True
                            r["message"] = last_message
                    elif category == "overdue":
                        r["date"] = str(data['timestamp'].iloc[-1])
                        r["status"] = "Last msg from particular app was due/overdue then no msg retrieved from the same app (means msg deleted)"
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
                    report[app_name] = r
        #pprint(report)
        connect.close()
        # get_current_open_details(cust_id)
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

