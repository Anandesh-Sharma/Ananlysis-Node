from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.loan_analysis.preprocessing import preprocessing
from HardCode.scripts.classifiers.Classifier import classifier
from HardCode.scripts.balance_sheet_analysis.transaction_balance_sheet import create_transaction_balanced_sheet
from HardCode.scripts.salary_analysis.monthly_salary_analysis import salary_main
from HardCode.scripts.loan_analysis.loan_rejection import get_rejection_count
from HardCode.scripts.loan_analysis.current_open_details import get_current_open_details
from HardCode.scripts.Util import conn, logger_1
import multiprocessing
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
        client.exception.updation.insert_one(r)
    return r


def update(**kwargs):
    user_id = kwargs.get('user_id')
    sms_json = kwargs.get('sms_json')

    # ==> creating logger and checking user_id
    logger = logger_1('update', user_id)
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
    logger.info('connection success')

    # >>==>> Classification
    logger.info('starting classification')
    try:
        result_class = classifier(sms_json, str(user_id))
        if not result_class['status']:
            msg = "Classifier failed due to some reason-" + result_class['message']
            exception_feeder(client=client, user_id=user_id, msg=msg)
            return result_class
    except BaseException as e:
        msg = "Exception in Classifier Analysis-" + str(e)
        exception_feeder(user_id=user_id, msg=msg, client=client)
        return {"status": False, "message": str(msg)}
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

    # >>=>> Current open details
    try:
        result_loan_limit = get_current_open_details(user_id)
        if not result_loan_limit['status']:
            msg = "Loan_limit check failed due to some reason-" + result_loan_limit['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Loan limit failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Loan limit complete')

    # >>=>> loan rejection
    try:
        result_loan_rejection = get_rejection_count(user_id)
        if not result_loan_rejection['status']:
            msg = "Loan rejection check failed due to some reason-" + result_loan_rejection['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Loan rejection failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Loan rejection complete')

    return {'status': True, 'message': 'success'}
