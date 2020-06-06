import warnings
from datetime import datetime

import pytz

from HardCode.scripts.cheque_bounce_analysis.Cheque_Bounce import cheque_user
from HardCode.scripts.parameters_for_bl0.loan_app.loan_app_count_validate import loan_app_percentage
from HardCode.scripts.parameters_for_bl0.rejection_msgs.rejecting_apps_count import get_app_rejection_count
from HardCode.scripts.parameters_for_bl0.relative_verification.relative_validation import rel_validate
from HardCode.scripts.loan_analysis.overdue_details import get_overdue_details
from HardCode.scripts.update_analysis import update
from HardCode.scripts.parameters_for_bl0.salary.last_5_salary import latest_salary
from HardCode.scripts.parameters_for_bl0.available_balance.last_month_avbl_bal import average_balance
from HardCode.scripts.Util import conn, logger_1

warnings.filterwarnings("ignore")

def exception_feeder(**kwargs):
    client = kwargs.get('client')
    msg = kwargs.get('msg')
    user_id = kwargs.get('user_id')

    logger = logger_1('exception_feeder',user_id)

    logger.error(msg)
    r = {'status': False, 'message': msg,
         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id,'result':False,'result_type':'kyc'}
    if client:
        client.exception.before_kyc.insert_one(r)
    return r

def result_output_false(msg):
    return {'status': False, 'message': msg,"result":False,"result_type":"kyc"}

def result_output_block(months,reason):
    return {'status': True, 'message': "success","result":False,"result_type":"kyc",
    "months":months,"reason":reason}

def before_kyc_function(**kwargs):
    user_id = kwargs.get('user_id')
    sms_json = kwargs.get('sms_json')
    app_data = kwargs.get('app_data')
    contacts = kwargs.get('contacts')
    profession = kwargs.get('profession')

    sms_count = len(sms_json)


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
        logger.info('making connection with data')
        client = conn()
    except BaseException as e:
        logger.critical('error in connection')
        return exception_feeder(user_id=user_id, msg="Exception in making data-"+str(e))
    logger.info('connection success')

    # ==> fetching data
    data = client.dynamic_input.before_kyc({"input":"before_kyc"})
    # >>==>> dynamic inputs
    sms_count_check = data['check_sms']
    sms_count_variable = data['variable_sms'] # 300
    sms_count_months = data['month_sms'] # 3
    loan_app_percentage_check = data['check_app_percentage']
    loan_app_percentage_variable = data['variable_app_percentage']# 0.7
    loan_app_percentage_months = data['month_app_percentage'] # -1
    relatives_check = data['check_relatives']
    relatives_variable = data['variable_relatives'] # 3
    relatives_months = data['month_relatives'] # 2
    loan_rejection_check = data['check_rejection']
    # loan_rejection_premium_app_variable = 0
    # loan_rejection_premium_app_months = 3
    loan_rejection_normal_app_variable = data['variable_normal_rejection']# 5
    loan_rejection_normal_app_months = data['month_normal_rejection']# 3
    loan_overdue_check = data['check_overdue']
    loan_overdue_variable = data['variable_overdue']# 15
    loan_overdue_months = data['month_overdue']# 2
    cheque_bounce_check = data['check_cheque']
    cheque_bounce_variable = data['variable_cheque']# 3
    cheque_bounce_months = data['month_cheque'] #2
    salaried_check = data['check_salaried']
    salaried_months = data['month_salaried'] #2
    salaried_variable = data['variable_salaried']  # 0
    last_month_balance1 = data['last_month_balance1'] #1500
    last_month_balance2 = data['last_month_balance2'] #3000
    sms_new_credit_variable = data['sms_new_credit_variable'] #400
    total_loans_check = data['total_loans_check']
    total_loans_variable = data['total_loans_variable'] #2
    # ===> Analysis
    try:
        result = update(user_id=user_id,sms_json=sms_json)
        if not result['status']:
            msg = "sms updation failed due to some reason-" + result['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "sms updation failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)
    # ===> Analysis Complete

    # >>==>> Data Extraction
    logger.info("Starting Relatives Validation")
    try:
        result_relatives = rel_validate(user_id,contacts)
        if not result_relatives['status']:
            msg = "rejection check failed due to some reason-"+result_relatives['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "relatives validation failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)
    logger.info('relatives validation function complete')

    # >>=>> Rejection check
    logger.info('starting rejection check')
    try:
        result_rejection = get_app_rejection_count(user_id)  # returns a dictionary
        if not result_rejection['status']:
            msg = "rejection check failed due to some reason-"+result_rejection['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "rejection check failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)
    logger.info('rejection check complete')


    # >>=>> Overdue_details
    logger.info('starting overdue fetch')
    try:
        result_overdue = get_overdue_details(user_id)
        overdue_days = len (result_overdue['result']['overdue_days_list'])
        if not result_overdue['status']:
            msg = "overdue check failed due to some reason-"+result_overdue['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "overdue check failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)
    logger.info('overdue check complete')


    # >>==>> Loan app percentage
    logger.info("Starting loan app percentage")
    try:
        result_app_percentage = loan_app_percentage(user_id=user_id,app_data=app_data)

        if not result_app_percentage['status']:
            msg = "loan app percentage failed due to some reason-"+result_app_percentage['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "loan app percentage failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)


    # >>==>> Cheque Bounce
    logger.info("Starting cheque bounce")
    try:
        result_cheque = cheque_user(user_id=user_id)

        if not result_cheque['status']:
            msg = "cheque failed due to some reason-"+result_cheque['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "cheque failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)

    # >>==>> Last month Salary
    logger.info("Starting Last month Salary")
    try:
        salary = latest_salary(user_id)

        if not salary['status']:
            msg = "Last month Salary failed due to some reason-"+salary['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "Last month Salary failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)

    # >>==>> Average Available Balance
    logger.info("Starting Average Available Balance")
    try:
        balance = average_balance(user_id)

        if not balance['status']:
            msg = "Average Available Balance failed due to some reason-"+balance['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
            return result_output_false(msg)
    except BaseException as e:
        msg = "Average Available Balance failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
        return result_output_false(msg)

    # ===> Data extraction complete

    # ===> Updating data
    logger.info("making dict for data")
    dict_update ={"sms_count":sms_count,"loan_app_percentage":result_app_percentage['percentage'],'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}
    dict_update['relatives_count']=result_relatives
    dict_update['normal_app_rejection']=result_rejection['normal_app']
    dict_update['overdue_days']=overdue_days
    dict_update['cheque_bounce_count']=result_cheque['count']
    dict_update['average_balance'] = balance['last_avbl_bal']
    dict_update['salary'] = salary['salary']
    client.analysis.parameters.update_one({"cust_id":user_id},{"$push":{'parameter_1': dict_update}},upsert=True)
    logger.info("data updated")
    #==> Data Updation Complete
    logger.info("checking parameters")
    if sms_count_check:
        if sms_count<sms_count_variable:
            return result_output_block(months = sms_count_months,reason="sms_count")

    if loan_app_percentage_check:
        if result_app_percentage['percentage']>loan_app_percentage_variable:
            return result_output_block(months = loan_app_percentage_months,reason="loan_app_percentage")

    if relatives_check:
        if result_relatives>relatives_variable:
            return result_output_block(months = relatives_months,reason="relatives_count")

    if loan_rejection_check:
        # if result_rejection['premium_app']>loan_rejection_premium_app_variable:
        #     return result_output_block(months = loan_rejection_premium_app_months,reason="loan_rejection_premium_app")
        if result_rejection['normal_app']>loan_rejection_normal_app_variable:
            return result_output_block(months = loan_rejection_normal_app_months,reason="loan_rejection_normal_app")

    if loan_overdue_check:
        if overdue_days > loan_overdue_variable:
            return result_output_block(months = loan_overdue_months,reason="loan_overdue")

    if cheque_bounce_check:
        if result_cheque['count'] > cheque_bounce_variable:
            return result_output_block(months=cheque_bounce_months, reason="cheque_bounce")

    logger.info("user passed from kyc")

    if total_loans_check:
        if result['total_loans'] < total_loans_variable:

            if salaried_check :
                if salary['salary'] > salaried_variable:
                    if sms_count < sms_count_variable and balance['last_avbl_bal'] < last_month_balance1:
                        return result_output_block(months=salaried_months, reason="last_month_balance_or_sms_count")

                else:  # profession yet to add
                    if balance['last_avbl_bal'] < last_month_balance2 and sms_count < sms_new_credit_variable :
                        return result_output_block(months=salaried_months, reason="last_month_balance_or_sms_count")

    logger.info("new user passed from cibil")

    return {
        "status":True,
        "message":"success",
        "result": True,
        "result_type": "kyc"
    }
