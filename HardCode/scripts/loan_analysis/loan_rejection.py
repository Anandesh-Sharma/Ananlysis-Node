from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex
from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.loan_analysis.my_modules import is_rejected, sms_header_splitter, grouping

def get_rejection_count(cust_id):
    target_apps = ['CASHBN', 'KREDTB', 'KREDTZ', 'LNFRNT', 'NIRAFN', 'SALARY']
    loan_data = fetch_user_data(cust_id)
    loan_data = sms_header_splitter(loan_data)
    grouped_data = grouping(loan_data)
    premium_app_rejection_count = 0
    normal_app_rejection_count = 0

    for app, data in grouped_data:
        if app in target_apps and app in list(loan_apps_regex.keys()):
            i = 0
            while i < len(data):
                message = str(data['body'][i].encode('utf-8')).lower()
                if is_rejected(message, app):
                    premium_app_rejection_count += 1
                    break
                i += 1
        else:
            if app in list(loan_apps_regex.keys()):
                i = 0
                while i < len(data):
                    message = str(data['body'][i].encode('utf-8')).lower()
                    if is_rejected(message, app):
                        normal_app_rejection_count += 1
                        break
                    i += 1
    return premium_app_rejection_count, normal_app_rejection_count
    
    