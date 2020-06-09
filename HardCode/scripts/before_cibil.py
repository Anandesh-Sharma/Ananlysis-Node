import json
import multiprocessing
import warnings
from datetime import datetime

import pytz

from HardCode.scripts.parameters_for_bl0.salary.last_5_salary import latest_salary
from HardCode.scripts.loan_analysis.loan_main import final_output
from HardCode.scripts.loan_analysis.overdue_details import get_overdue_details
from HardCode.scripts.parameters_for_bl0.available_balance.last_month_avbl_bal import \
    average_balance
from HardCode.scripts.analysis_updation import update
from HardCode.scripts.parameters_for_bl0.loan_limit.max_loan_amount import get_loan_max_amount
from HardCode.scripts.Util import conn, logger_1

warnings.filterwarnings("ignore")

def exception_feeder(**kwargs):
    client = kwargs.get('client')
    msg = kwargs.get('msg')
    user_id = kwargs.get('user_id')

    logger = logger_1('exception_feeder',user_id)

    logger.error(msg)
    r = {'status': False, 'message': msg,
         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
    if client:
        client.exception.before_kyc.insert_one(r)
    return r

def result_output_false(msg):
    return {'status': False, 'message': msg,"result":False,"result_type":"cibil"}


def old_to_credit(**kwargs):
    user_id = kwargs.get('user_id')
    sms_count = kwargs.get('sms_count')
    client = conn()
    logger = logger_1('old_to_credit',user_id=user_id)

    salary = latest_salary(user_id)
    balance = average_balance(user_id)
    result_loan_main = get_loan_max_amount(user_id)

    # ==> fetching data
    data = client.dynamic_input.before_cibil({"input": "before_cibil"})
    # >>==>> dynamic inputs
    salaried_variable = data['variable_salaried']  # 0
    loan_amount_variable1 = data['loan_amount_variable1']  # 1500
    loan_amount_variable2 = data['loan_amount_variable2']  # 3000
    last_avbl_balance_variable1 = data['available_balance_variable1']  # 1000
    last_avbl_balance_variable2 = data['available_balance_variable2']  # 3000
    sms_count_variable = data['variable_sms']  #300

    # ===> Updating data
    logger.info("making dict for data")
    dict_update ={"salary":salary['salary']}
    dict_update['last_month_average_balance']=balance['last_avbl_bal']
    dict_update['loan_amount'] = result_loan_main
    client.analysis.parameters.update_one({"cust_id":user_id},{"$push":{'parameter_2': dict_update}},upsert=True)
    logger.info("data updated")
    #==> Data Updation Complete

    # ==> Salary check on old user started
    if salary['salary'] > salaried_variable:
        if result_loan_main > loan_amount_variable1 and balance['last_avbl_bal'] > last_avbl_balance_variable1 :
            return True
        else:
            return False

    else:
        if result_loan_main > loan_amount_variable2 and balance['last_avbl_bal'] > last_avbl_balance_variable2 and sms_count > sms_count_variable :
            return True
        else:
            return False



def before_cibil_function(**kwargs):
    user_id = kwargs.get('user_id')
    sms_json = kwargs.get('sms_json')

    sms_count = len(sms_json)
    client = conn()
    # ==> fetching data
    data = client.dynamic_input.before_cibil({"input": "before_cibil"})
    # >>==>> dynamic inputs
    total_loans_check = data['total_loans_check']
    total_loans_variable =  data['total_loans_variable']  #2

    # ==> creating logger and checking user_id
    logger = logger_1('bl0', user_id)
    if not isinstance(user_id, int):
        try:
            logger.info("user_id not int converting into int")
            user_id = int(user_id)
            logger.info("user_id successfully converted into int")
        except BaseException as e:
            return exception_feeder(user_id=-1, msg='user_id has a issue got id' + str(user_id))
    try:
        logger.info('making connection with db')
        client = conn()
    except BaseException as e:
        logger.critical('error in connection')
        return exception_feeder(user_id=user_id, msg="Exception in making db-"+str(e))
    logger.info('connection success')

    # Analysis Updation
    try:
        logger.info('analysis updation ')
        updation = update(user_id=user_id,sms_json=sms_json)
        if not updation['status']:
            msg = "cheque bounce failed due to some reason"
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                             msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        logger.critical('analysis not updated')
        exception_feeder(user_id=user_id, msg="Exception in updating analysis-"+str(e))
        return result_output_false(msg)
    logger.info('analysis updation success')

    # >>==>> Overdue information
    logger.info("Starting Loan overdue function")
    try:
        result = get_overdue_details(user_id)
        if not result:
            msg = "Loan overdue failed due to some reason"
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "Loan overdue failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)
    logger.info('Loan overdue function complete')

    # ===> Updating data
    logger.info("making dict for data")
    dict_update ={"sms_count":sms_count,'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}
    dict_update['total_loans']=result['total_loans']
    client.analysis.parameters.update_one({"cust_id":user_id},{"$push":{'parameter_2': dict_update}},upsert=True)
    logger.info("data updated")
    #==> Data Updation Complete

    # no of loans check for loan history status
    if total_loans_check:
        if result['total_loans'] > total_loans_variable:
            new_to_credit = False
        else:
            new_to_credit = True


    if not new_to_credit :
        result = old_to_credit(user_id=user_id,sms_count=sms_count)
        if result:
            return {
                "status": True,
                "message": "success",
                "result": False,
                "result_type": "cibil"
            }
        else:
            return {
                "status": True,
                "message": "success",
                "result": True,
                "result_type": "cibil"
            }

    else:
        return {
            "status": True,
            "message": "success",
            "result": False,
            "result_type": "cibil"
        }

