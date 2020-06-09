from datetime import datetime
from HardCode.scripts.loan_analysis.last_loan_details import get_final_loan_details


def new_current_open(cust_id):

    count = 0
    try:
        last_loan_details = get_final_loan_details(cust_id)
        initial_date = datetime.strptime("2020-03-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        for i in last_loan_details.keys():
            last_message_date = datetime.strptime(last_loan_details[i]["date"], "%Y-%m-%d %H:%M:%S")
            if last_message_date > initial_date:
                if last_loan_details[i]["category"]:
                    count += 1
        return count
    except BaseException as e:
        return count