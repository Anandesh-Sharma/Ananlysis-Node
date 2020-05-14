#from HardCode.scripts.Util import conn
from HardCode.scripts.rule_based_model.phase2 import rule_quarantine
from pymongo import MongoClient
from tqdm import tqdm
import pandas as pd
from HardCode.scripts.testing.all_repeated_ids import *

def conn():
    # Create MONGO_SUPERUSER and MONGO_SUPERPASS global varaible in local environment for MongoDB

    connection = MongoClient(
        f"mongodb://god:Cr3dicxo%40321@176.223.138.76:27017/?authSource=admin"
                                       f"&readPreference=primary&ssl=false"
,
        socketTimeoutMS=900000)
    return connection

def rule_phase1(user_id):
    connect = conn()
    params = connect.analysis.scoring_model.find_one({'cust_id':user_id})
    params = params['result'][-1]
    loan_app_count_percentage = params['parameters']['deduction_parameters']['loan_app_count_val']['loan_app_count']
    avg_bal  = params['parameters']['deduction_parameters']['available_balance_val']['avg_bal_of_3_month']
    #similarity = params['parameters']['deduction_parameters']['reference_val']['reference']['result']['similarity_score']
    relatives = params['parameters']['deduction_parameters']['reference_val']['relatives']['length']
    day_3_7 = params['parameters']['deduction_parameters']['loan_val']['due_days']['3-7_days']
    day_7_12 = params['parameters']['deduction_parameters']['loan_val']['due_days']['7-12_days']
    day_12_15 = params['parameters']['deduction_parameters']['loan_val']['due_days']['12-15_days']
    more_than_15= params['parameters']['deduction_parameters']['loan_val']['due_days']['more_than_15']
    total_loans = params['parameters']['deduction_parameters']['due_days_interval_val']['total_loans']
    cr_day_0_3 = params['parameters']['additional_parameters']['crdcxo_overdue_report']['0-3_days']
    cr_day_3_7 = params['parameters']['additional_parameters']['crdcxo_overdue_report']['3-7_days']
    cr_day_7_12 = params['parameters']['additional_parameters']['crdcxo_overdue_report']['7-12_days']
    cr_day_12_15 = params['parameters']['additional_parameters']['crdcxo_overdue_report']['12-15_days']
    cr_more_than_15 = params['parameters']['additional_parameters']['crdcxo_overdue_report']['more_than_15']
    cr_pending_emi = params['parameters']['additional_parameters']['crdcxo_pending']
    cr_total_loan = params['parameters']['additional_parameters']['crdcxo_total_laons']




    # if not similarity >= 0.8:
    #     return False
    # if not relatives > 3:
    #     return False
    if total_loans > 12:
        return False,relatives
    if total_loans < 3:
        return False,relatives
    if loan_app_count_percentage < 0.7:
        return False,relatives
    if avg_bal < 4000:
        return False,relatives
    if more_than_15 != 0:
        return False,relatives
    if day_12_15 != 0:
        return False,relatives
    if day_7_12 > 2:
        return False,relatives
    if day_3_7 > 3:
        return False,relatives
    if cr_day_0_3 >= 2:
        return False,relatives
    if cr_day_3_7 >= 1:
        return False,relatives
    if cr_day_7_12 != 0:
        return False,relatives
    if cr_day_12_15 != 0:
        return False,relatives
    if cr_more_than_15 != 0:
        return False,relatives
    if cr_total_loan <= 1:
        return False,relatives
    if cr_pending_emi != 0:
        return False,relatives
    else:
        return True,relatives


def rule_engine_main(user_id):
    phase1,relatives = rule_phase1(user_id)
    phase2 = rule_quarantine(user_id)
    result_pass = phase1 and phase2
    if result_pass:
        print("approved")
    else:
        print("rejected by rule engine")
    return result_pass,relatives


for id in tqdm(user_ids):
    a = []
    try:
        result,relatives = rule_engine_main(id)
        df = {'user_id':id,'result_pass':[result],'relatives':[relatives]}
        data = pd.DataFrame.from_dict(df)
        data.to_csv("result_14.csv", mode='a', header=False)
    except:
        pass
        a.append(id)
print(a)