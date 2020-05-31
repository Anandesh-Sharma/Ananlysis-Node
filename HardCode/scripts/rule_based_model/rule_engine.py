from HardCode.scripts.Util import conn
from HardCode.scripts.rule_based_model.phase2 import rule_quarantine
from pymongo import MongoClient
from tqdm import tqdm
import pandas as pd
from HardCode.scripts.testing.all_repeated_ids import *



def rule_phase1(user_id):
    connect = conn()
    params = connect.analysis.parameters.find_one({'cust_id':user_id})
    # params = params['result'][-1]
    loan_app_count_percentage = params['parameters']['percentage_of_loan_apps']
    # avg_bal  = params['parameters']['avg_balance']
    #similarity = params['parameters']['reference']['result']['similarity_score']
    #relatives = params['parameters']['no_of_relatives']
    day_3_7 = params['parameters']['overdue_days']['3-7_days']
    day_7_12 = params['parameters']['overdue_days']['7-12_days']
    day_12_15 = params['parameters']['overdue_days']['12-15_days']
    more_than_15= params['parameters']['overdue_days']['more_than_15']
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


def rule_engine_main(user_id):
    try:
        # phase1 = rule_phase1(user_id)
        phase2 = rule_quarantine(user_id)
        connect = conn()
        params = connect.analysis.parameters.find_one({'cust_id':user_id})
        connect.close()
        salary = params['parameters'][-1]['quarantine_salary']
        if salary > 0:
            phase1=True
        else:
            phase1=False
        result_pass = phase1 and phase2
        if result_pass:
            print("approved")
        else:
            print("rejected by rule engine")
    except BaseException as e:
        return {"status":False,"message":str(e)}
    return {"status":True,"result":result_pass}

