from HardCode.scripts.loan_analysis.preprocessing import preprocessing
from datetime import datetime
from HardCode.scripts.loan_analysis.last_loan_details import get_final_loan_details
from HardCode.scripts.Util import conn

def rule_quarantine(cust_id):
    report = {
        'currently_open' : 0,
    }
    count = 0
    open_apps = []
    try:
        last_loan_details = get_final_loan_details(cust_id)
        initial_date = datetime.strptime("2020-03-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        for i in last_loan_details.keys():
            last_message_date = datetime.strptime(last_loan_details[i]["date"], "%Y-%m-%d %H:%M:%S")
            if last_message_date > initial_date:
                if last_loan_details[i]["category"]:
                    open_apps.append(last_loan_details[i])
                    count += 1
        report["currently_open"] = count

    except BaseException as e:
        print(e)
    if report['currently_open'] != 0:
        return False,open_apps
    return True,open_apps