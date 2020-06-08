from HardCode.scripts.Util import conn
from HardCode.scripts.rule_based_model.phase2 import rule_quarantine
from HardCode.scripts.parameters_for_bl0.available_balance.last_month_avbl_bal import average_balance
from datetime import datetime
import pytz


# from HardCode.scripts.testing.all_repeated_ids import *


def rule_phase1(user_id):
    connect = conn()
    params = connect.analysis.parameters.find_one({'cust_id': user_id})
    # params = params['result'][-1]
    loan_app_count_percentage = params['parameters']['percentage_of_loan_apps']
    # avg_bal  = params['parameters']['avg_balance']
    # similarity = params['parameters']['reference']['result']['similarity_score']
    # relatives = params['parameters']['no_of_relatives']
    day_3_7 = params['parameters']['overdue_days']['3-7_days']
    day_7_12 = params['parameters']['overdue_days']['7-12_days']
    day_12_15 = params['parameters']['overdue_days']['12-15_days']
    more_than_15 = params['parameters']['overdue_days']['more_than_15']
    total_loans = params['parameters']['total_loans']
    cr_day_0_3 = params['parameters']['credicxo_overdue_days']['0-3_days']
    cr_day_3_7 = params['parameters']['credicxo_overdue_days']['3-7_days']
    cr_day_7_12 = params['parameters']['credicxo_overdue_days']['7-12_days']
    cr_day_12_15 = params['parameters']['credicxo_overdue_days']['12-15_days']
    cr_more_than_15 = params['parameters']['credicxo_overdue_days']['more_than_15']
    cr_pending_emi = params['parameters']['credicxo_pending_emi']
    cr_total_loan = params['parameters']['credicxo_total_loans']

    # if not similarity >= 0.8:
    #     return False
    # if not relatives > 3:
    #     return False
    if total_loans > 12:
        return False
    if total_loans < 3:
        return False
    if loan_app_count_percentage < 0.7:
        return False
    # if avg_bal < 4000:
    #     return False
    if more_than_15 != 0:
        return False
    if day_12_15 != 0:
        return False
    if day_7_12 > 2:
        return False
    if day_3_7 > 3:
        return False
    if cr_day_0_3 >= 2:
        return False
    if cr_day_3_7 >= 1:
        return False
    if cr_day_7_12 != 0:
        return False
    if cr_day_12_15 != 0:
        return False
    if cr_more_than_15 != 0:
        return False
    if cr_total_loan <= 1:
        return False
    if cr_pending_emi != 0:
        return False
    else:
        return True

def quarantine_sal(user_id):
    connect = conn()
    salary = connect.analysis.salary.find_one({'cust_id': user_id})
    sal = -1
    try:
        if salary:
            month_list = list(salary['salary'].keys())
            if "April 2020" in month_list:
                index = month_list.index("April 2020")
                new_month_list = month_list[index:]
                for i in new_month_list:
                    if salary['salary'][i]['salary'] > sal:
                        sal = salary['salary'][i]['salary']
        connect.close()
        return sal
    except:
        connect.close()
        return sal

def rule_engine_main(user_id):
    try:
        # phase1 = rule_phase1(user_id)
        reason = []
        phase2 = rule_quarantine(user_id)
        # params = connect.analysis.parameters.find_one({'cust_id': user_id})
        salary = quarantine_sal(user_id)
        if salary > 0:
            phase1 = True
        else:
            phase1 = False
        avl_bal = average_balance(user_id)
        if not avl_bal['status']:
            connect = conn()
            connect.analysis.exception_bl0.insert_one({"cust_id":user_id,"message":"error in avl bal average - "+str(avl_bal['message']),'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))})
            connect.close()
        if avl_bal['last_avbl_bal'] > 4000:
            phase3 = True
        else:
            phase3 = False
        result_pass = (phase1 or phase3) and phase2
        if not phase1:
            reason.append("Quarantine Salary")
        if not phase2:
            reason.append("Loan open")
        if not phase3:
            reason.append("avbl bal")
        if result_pass:
            print("approved")
        else:
            print("rejected by rule engine")
        dict_update={"quarntine_salary":salary,"Last_open":phase2,"avbl_open":avl_bal['last_avbl_bal']}
    except BaseException as e:
        return {"status": False, "cust_id": user_id, "result": False, 'message': str(e)}
    return {"status": True, "cust_id": user_id, "result": result_pass,"reason":reason,"dict_update":dict_update}
