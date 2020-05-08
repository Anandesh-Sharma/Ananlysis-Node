from HardCode.scripts.Util import conn
from HardCode.scripts.rule_based_model.phase2 import rule_quarantine


def rule_phase1(user_id):
    connect = conn()
    params = connect.analysis.scoring_model.find_one({'cust_id':user_id})
    params = params['result'][-1]
    loan_app_count_percentage = params['parameters']['deduction_parameters']['loan_app_count_val']['loan_app_count']
    avg_bal  = params['parameters']['deduction_parameters']['available_balance_val']['avg_bal_of_3_month']
    similarity = params['parameters']['deduction_parameters']['reference_val']['reference']['result']['similarity_score']
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




    if not similarity >= 0.8:
        return False
    if not relatives > 3:
        return False
    if not total_loans < 12:
        return False
    if not loan_app_count_percentage < 0.7:
        return False
    if not avg_bal > 4000:
        return False
    if not more_than_15 == 0:
        return False
    if not day_12_15 == 0:
        return False
    if not day_7_12 < 2:
        return False
    if not day_3_7 < 3:
        return False
    if not cr_day_0_3 <= 2:
        return False
    if not cr_day_3_7 <= 1:
        return False
    if not cr_day_7_12 == 0:
        return False
    if not cr_day_12_15 == 0:
        return False
    if not cr_more_than_15 == 0:
        return False
    if not cr_total_loan >= 1:
        return False
    if not cr_pending_emi == 0:
        return False
    else:
        return True


def rule_engine_main(user_id):
    phase1 = rule_phase1(user_id)
    phase2 = rule_quarantine(user_id)
    result_pass = phase1 and phase2
    return result_pass



