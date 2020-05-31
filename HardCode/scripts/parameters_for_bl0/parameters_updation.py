from HardCode.scripts.parameters_for_bl0.account_status.status import get_acc_status
from HardCode.scripts.parameters_for_bl0.active_close_status.active_closed_count import get_active_closed
from HardCode.scripts.parameters_for_bl0.age_of_oldest_trade.age import age_oldest_trade
from HardCode.scripts.parameters_for_bl0.age_of_user.user_age import get_age
from HardCode.scripts.parameters_for_bl0.available_balance.available_balance import find_info
from HardCode.scripts.parameters_for_bl0.available_balance.mean_available_balance import mean_available
from HardCode.scripts.parameters_for_bl0.credit_card_limit.cc_limit import get_cc_limit
from HardCode.scripts.parameters_for_bl0.ecs_bounce.chq_bounce import get_count_cb
from HardCode.scripts.parameters_for_bl0.ecs_bounce.ecs_bounce import get_count_ecs
from HardCode.scripts.parameters_for_bl0.loan_app.loan_app_count_validate import loan_app_count
from HardCode.scripts.parameters_for_bl0.payment_rating.pay_rating import get_payment_rating
from HardCode.scripts.parameters_for_bl0.reapyment_history.repayment_history import repayment_history
from HardCode.scripts.parameters_for_bl0.reference_verification.validation.check_reference import validate
from HardCode.scripts.parameters_for_bl0.rejection_msgs.get_ratio import *
from HardCode.scripts.parameters_for_bl0.rejection_msgs.rejecting_apps_count import get_app_rejection_count
from HardCode.scripts.parameters_for_bl0.rejection_msgs.total_rejection_msg import get_defaulter
from HardCode.scripts.parameters_for_bl0.relative_verification.relative_validation import rel_validate
from HardCode.scripts.parameters_for_bl0.salary.salary_count import *
from HardCode.scripts.parameters_for_bl0.secured_unsecured_loans.count import secure_unsecured_loan
from HardCode.scripts.parameters_for_bl0.user_name_msg.name_count_ratio import get_name_count
from HardCode.scripts.loan_analysis.loan_main import final_output
from HardCode.scripts.loan_analysis.last_loan_details import get_final_loan_details
from HardCode.scripts.loan_analysis.overdue_details import get_overdue_details
from HardCode.scripts.cibil.cibil_analysis import cibil_analysis
from HardCode.scripts.Util import conn
import pytz


def parameters_updation(user_id,cibil_df,no_of_sms):

    connect = conn()
    db = connect.analysis.parameters

    try:
        account_status = get_acc_status(cibil_df)
    except BaseException as e:
        account_status = False
        print('error in account status-' + str(e))

    try:
        actives,close = get_active_closed(cibil_df)
    except BaseException as e:
        actives = 0
        close = 0
        print('error in active close-' + str(e))

    try:
        oldest_trade = age_oldest_trade(cibil_df)
    except BaseException as e:
        oldest_trade = 0
        print('error in aage of oldest trade-' + str(e))

    try:
        age_of_user = get_age(user_id)
    except BaseException as e:
        age_of_user = 0
        print('error in user age-' + str(e))

    try:
        available_bal = find_info(user_id)
    except BaseException as e:
        available_bal = {'AC_NO':'','balance_on_loan_date':-1,
                'last_month_bal':-1,'second_last_month_bal':-1,
                'third_last_month_bal':-1,'count_creditordebit_msg':-1,
                'no_of_accounts':-1}
        print('error in available balance-' + str(e))

    try:
        mean_balance,last_peak,second_last_peak,third_last_peak,avg_bal = mean_available(user_id)
    except BaseException as e:
        mean_balance = -1
        last_peak = {}
        second_last_peak = {}
        third_last_peak = {}
        avg_bal = -1
        print('error in mean balance-' + str(e))

    try:
        creditcard = get_cc_limit(user_id)
    except BaseException as e:
        creditcard = {}
        print('error in credit card-' + str(e))

    try:
        chq_count = get_count_cb(user_id)
    except BaseException as e:
        chq_count = 0
        print('error in chq bounce-' + str(e))

    try:
        ecs_count = get_count_ecs(user_id)
    except BaseException as e:
        ecs_count = 0
        print('error in ecs bounce-' + str(e))

    try:
        percent_of_loan_apps = loan_app_count(user_id)
    except BaseException as e:
        percent_of_loan_apps = 0
        print('error in loan app percentage-' + str(e))

    try:
        cr_overdue_days,cr_total_loans,cr_loan_limit,cr_pending_emi = repayment_history(user_id)
    except BaseException as e:
        cr_overdue_days = -1
        cr_total_loans = -1
        cr_loan_limit = -1
        cr_pending_emi = -1
        print('error in repayment history-' + str(e))

    try:
        result , no_ofcontacts = validate(user_id)
    except BaseException as e:
        result = {}
        no_ofcontacts = -1
        print('error in reference-' + str(e))

    try:
        legal_ratio = legal_messages_count_ratio(user_id,no_of_sms)
    except BaseException as e:
        legal_ratio = -1
        print('error in legal-' + str(e))

    try:
        overdue_count, overdue_ratio = overdue_count_ratio(user_id,no_of_sms)
    except BaseException as e:
        overdue_count = -1
        overdue_ratio = -1
        print('error in overdue-' + str(e))

    try:
        legal_count = get_defaulter(user_id)
    except BaseException as e:
        legal_count = -1
        print('error in total rejction msg-' + str(e))

    try:
        normal_app_count, premium_app_count = get_app_rejection_count(user_id)
    except BaseException as e:
        normal_app_count = 0
        premium_app_count = 0
        print('error in app rejection count-' + str(e))

    try:
        res , rel_length = rel_validate(user_id)
    except BaseException as e:
        res = {}
        rel_length = 0
        print('error in relatives -' + str(e))

    try:
        last_salary = last_sal(user_id)
    except BaseException as e:
        last_salary = -1
        print('error in last salary-' + str(e))

    try:
        quarantine_salary = quarantine_sal(user_id)
    except BaseException as e:
        quarantine_salary = -1
        print('error in quarantine salary-' + str(e))

    try:
        secured,unsecured = secure_unsecured_loan(cibil_df)
    except BaseException as e:
        secured = -1
        unsecured = -1
        print('error in secured unsecured-' + str(e))

    try:
        username_msgs = get_name_count(user_id)
    except BaseException as e:
        username_msgs = 0
        print('error in username msgs-' + str(e))

    try:
        loans_info = final_output(user_id)
    except BaseException as e:
        loans_info = {}
        print('error in loan main-' + str(e))

    try:
        last_loan_detail = get_final_loan_details(user_id)
    except BaseException as e:
        last_loan_detail = {}
        print('error in last loan details-' + str(e))

    try:
        overdue_report  = get_overdue_details(user_id)
    except BaseException as e:
        overdue_report = {}
        print('error in overdue report-' + str(e))

    try:
        if cibil_df['data'] is not None:
            payment_r, history, amt_principal, amt_total = cibil_analysis(cibil_df['data'],
                                                                          cibil_df['data']['credit_score'].max(), user_id)
        else:
            history = -1
            amt_principal = -1
            amt_total = -1
            payment_r = -1
    except BaseException as e:
        history = -1
        amt_principal = -1
        amt_total = -1
        payment_r = -1
        print('error in payment history-'+str(e))


    try:
        dict_update = {'Total_msgs':no_of_sms,
                        'overdue_info':overdue_report,
                       'last_loan_details':last_loan_detail,
                       'normal_app_rejection': normal_app_count,
                       'premium_app_rejection': premium_app_count,
                       'loan_info': loans_info,
                        'available_balance':available_bal,
                       'avg_balance':avg_bal,
                       'mean_bal':mean_balance,
                       'last_month_peak':last_peak,
                       'second_last_month_peak':second_last_peak,
                       'third_last_month_peak':third_last_peak,
                       'credit_card':creditcard,
                       'salary':last_salary,
                       'quarantine_salary':quarantine_salary,
                       'ecs_bounce':ecs_count,
                       'chq_bounce':chq_count,
                       'legal_msg_count':legal_count,
                       'legal_msg_ratio':legal_ratio,
                       'overdue_msg_ratio':overdue_ratio,
                       'overdue_msg_count':overdue_count,
                       'account_status':account_status,
                       'active':actives,
                       'closed':close,
                       'age_of_oldest_trade':oldest_trade,
                       'secured_loans':secured,
                       'unsecured_loans':unsecured,
                       'payment_history': history,
                       'written_amt_principal': amt_principal,
                       'written_amt_total': amt_total,
                       'payment_rating':payment_r,
                       'age':age_of_user,
                       'percentage_of_loan_apps':percent_of_loan_apps,
                       'credicxo_loan_limit':cr_loan_limit,
                       'credicxo_overdue_days':cr_overdue_days,
                       'credicxo_pending_emi':cr_pending_emi,
                       'credicxo_total_loans':cr_total_loans,
                       'no_of_contacts':no_ofcontacts,
                       'reference':result,
                       'no_of_relatives':rel_length,
                       'relatives':res,
                       'username_msgs':username_msgs  ,
                       'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}

        db.update_one({'cust_id': user_id}, {"$push": {'parameters': dict_update}}, upsert=True)


        return {'status':True,'message':'success'}
    except BaseException as e:
        return {'status':False,'message':str(e)}


