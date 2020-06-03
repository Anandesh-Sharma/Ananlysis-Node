from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.loan_analysis.preprocessing import preprocessing
from HardCode.scripts.classifiers.Classifier import classifier
from HardCode.scripts.balance_sheet_analysis.transaction_balance_sheet import create_transaction_balanced_sheet
from HardCode.scripts.cheque_bounce_analysis.Cheque_Bounce import cheque_user_outer
from HardCode.scripts.salary_analysis.monthly_salary_analysis import salary_main
from HardCode.scripts.parameters_for_bl0.parameters_updation import parameters_updation
from HardCode.scripts.loan_analysis.current_open_details import get_current_open_details
from HardCode.scripts.loan_analysis.loan_rejection import get_rejection_count
from HardCode.scripts.model_0.scoring.generate_total_score import get_score
from HardCode.scripts.rule_based_model.rule_engine import rule_engine_main
from HardCode.scripts.Util import conn, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def exception_feeder(**kwargs):
    client = kwargs.get('client')
    msg = kwargs.get('msg')
    user_id = kwargs.get('user_id')

    logger = logger_1('exception_feeder', user_id)

    logger.error(msg)
    r = {'status': False, 'message': msg,
         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
    if client:
        client.analysisresult.exception_bl0.insert_one(r)
    return r


def result_output_false(msg):
    return {'status': False, 'message': msg}


def result_output_block():
    return {'status': True, 'message': "success"}


def bl0(**kwargs):
    user_id = kwargs.get('user_id')
    sms_json = kwargs.get('sms_json')
    cibil_df = kwargs.get('cibil_xml')
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
        logger.info('making connection with db')
        client = conn()
    except:
        logger.critical('error in connection')
        return {'status': False, 'message': "Error in making connection.",
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
    logger.info('connection success')

    # >>==>> Classification
    logger.info('starting classification')
    try:
        result_class = classifier(sms_json, str(user_id))
        if not result_class['status']:
            msg = "Classifier failed due to some reason-" + result_class['message']
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Exception in Classifier Analysis-" + str(e)
        exception_feeder(user_id=user_id, msg=msg, client=client)
    logger.info('classification completes')

    # >>=>> LOAN ANALYSIS
    logger.info('starting loan analysis')
    try:
        result_loan = preprocessing(user_id)  # returns a dictionary
        if not result_loan['status']:
            msg = "Loan Analysis failed due to some reason-" + result_loan['message']
            exception_feeder(client=client, user_id=user_id,
                             msg=msg)
    except BaseException as e:
        msg = "Exception in Loan Analysis-" + str(e)
        exception_feeder(user_id=user_id, msg=msg, client=client)
    logger.info('loan analysis successful')

    # >>=>> Rejection check
    logger.info('starting rejection checkrejection check')
    try:
        result_rejection = check_rejection(user_id)  # returns a dictionary
        if not result_rejection['status']:
            msg = "rejection check failed due to some reason-" + result_rejection['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                             msg=msg)
    except BaseException as e:
        msg = "rejection check failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                         msg=msg)
    logger.info('rejection check complete')

    # >>=>> BALANCE SHEET
    logger.info('started making balanced sheet')
    try:
        result_balance_sheet = create_transaction_balanced_sheet(user_id)
        if not result_balance_sheet['status']:
            msg = "Balance Sheet check failed due to some reason-" + result_balance_sheet['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Balance Sheet failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Balance Sheet complete')

    # >>=>> CHEQUE BOUNCE ANALYSIS
    try:
        result_cheque_bounce = cheque_user_outer(user_id)
        if not result_cheque_bounce['status']:
            msg = "Cheque Bounce check failed due to some reason-" + result_cheque_bounce['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Cheque Bounce failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Cheque Bounce complete')

    # >>=>> SALARY ANALYSIS
    logger.info('starting salary analysis')
    try:
        result_salary = salary_main(user_id)  # Returns a dictionary
        if not result_salary['status']:
            msg = "Salary check failed due to some reason-" + result_salary['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Salary failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Salary analysis complete')

    # >>=>> Loan Rejection
    try:
        result_loan_rejection = get_rejection_count(user_id)
        if not result_loan_rejection['status']:
            msg = "Loan Rejection messages check failed due to some reason-" + result_loan_rejection['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Loan Rejection messages failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Loan Rejection messages complete')

    # >>=>> Parameters Updation
    try:
        result_params = parameters_updation(user_id, cibil_df, sms_count)
        if not result_params['status']:
            msg = "Parameters updation check failed due to some reason-" + result_params['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Parameters updation failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Parameters updation complete')

    # >>=>> Current loan details
    try:
        result_current_loan = get_current_open_details(user_id)
        if not result_current_loan['status']:
            msg = "Current open details check failed due to some reason-" + result_current_loan['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Current open details failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Current open details  complete')

    # >>=>> Scoring Model
    try:
        result_score = get_score(user_id, sms_count)
        if not result_score['status']:
            msg = "Scoring Model failed due to some reason"
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Scoring Model failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Scoring Model complete')

    # >>=>> Rule Engine
    try:
        rule_engine = rule_engine_main(user_id)
        if not rule_engine['status']:
            msg = "Rule engine failed due to some reason-" + rule_engine['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
            rule_engine = {"result": False}
    except BaseException as e:
        msg = "Rule engine failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
        rule_engine = {"result": False}
    logger.info('Rule engine complete')

    client.analysis.result_bl0.update_one({'cust_id': user_id}, {"$push": {
        "result": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), "result": rule_engine['result']}}},
                                          upsert=True)
    return rule_engine['result']
