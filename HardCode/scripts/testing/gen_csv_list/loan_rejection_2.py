from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex
from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.loan_analysis.my_modules import is_rejected, sms_header_splitter, grouping
from datetime import datetime


def get_rejection_count(cust_id):
    target_apps = ['CASHBN', 'KREDTB', 'KREDTZ', 'LNFRNT', 'NIRAFN', 'SALARY']
    loan_data = fetch_user_data(cust_id)
    loan_data = sms_header_splitter(loan_data)
    grouped_data = grouping(loan_data)
    premium_app_rejection_count = 0
    normal_app_rejection_count = 0
    message_list = []
    start_date = datetime.strptime("2020-03-21 00:00:00", "%Y-%m-%d %H:%M:%S")
    for app, data in grouped_data:
        try:
            if app in target_apps and app in list(loan_apps_regex.keys()):
                for _,row in data.iterrows():
                    message = str(row['body'].encode('utf-8')).lower()
                    date = datetime.strptime(str(row['timestamp']), "%Y-%m-%d %H:%M:%S")
                    days = (start_date - date).days
                    if days < 90:
                        if is_rejected(message, app):
                            premium_app_rejection_count += 1
                            message_list.append(message)
                            break
            else:
                if app in list(loan_apps_regex.keys()):
                    for _,row in data.iterrows():
                        message = str(row['body'].encode('utf-8')).lower()
                        date = datetime.strptime(str(row['timestamp']), "%Y-%m-%d %H:%M:%S")
                        days = (start_date - date).days
                        if days < 90:
                            if is_rejected(message, app):
                                normal_app_rejection_count += 1
                                message_list.append(message)
                                break
        except BaseException as e:
            import traceback
            traceback.print_tb(e.__traceback__)
            print(e)
    return premium_app_rejection_count, normal_app_rejection_count, message_list
