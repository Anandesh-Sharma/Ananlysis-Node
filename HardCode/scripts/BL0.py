from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.loan_analysis.loan_main import final_output
from HardCode.scripts.classifiers.Classifier import classifier
from HardCode.scripts.balance_sheet_analysis.transaction_balance_sheet import create_transaction_balanced_sheet
from HardCode.scripts.cheque_bounce_analysis.Cheque_Bounce import cheque_user_outer
from HardCode.scripts.salary_analysis.monthly_salary_analysis import salary_main
from HardCode.scripts.parameters_for_bl0.loan_limit.loan_info import loan_limit
from HardCode.scripts.parameters_for_bl0.loan_limit.last_loan_details import get_final_loan_details
from HardCode.scripts.parameters_for_bl0.available_balance.available_balance import find_info
from HardCode.scripts.parameters_for_bl0.available_balance.mean_available_balance import mean_available
from HardCode.scripts.parameters_for_bl0.credit_card_limit.cc_limit import get_extracted_data
from HardCode.scripts.parameters_for_bl0.salary.salary_count import last_sal
from HardCode.scripts.parameters_for_bl0.ecs_bounce.ecs_bounce import get_count_ecs
from HardCode.scripts.parameters_for_bl0.ecs_bounce.chq_bounce import get_count_cb
from HardCode.scripts.parameters_for_bl0.rejection_msgs.total_rejection_msg import get_defaulter
from HardCode.scripts.Util import conn,logger_1
import multiprocessing
import warnings
from datetime import datetime
import pytz

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
        client.analysisresuult.exception_bl0.insert_one(r)
    return r

def result_output_false(msg):
    return {'status': False, 'message': msg}

def result_output_block():
    return {'status': True, 'message': "success"}

def bl0(**kwargs):
    user_id = kwargs.get('user_id')
    sms_json = kwargs.get('sms_json')

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
    logger.info('connection success')


    # >>==>> Classification
    logger.info('starting classification')
    p = multiprocessing.Process(target=classifier, args=(sms_json, str(user_id),))
    try:
        p.start()
    except BaseException as e:
        msg="Exception in starting classifier"+str(e)
        exception_feeder(user_id=user_id, msg=msg,client=client)

    try:
        p.join()
    except BaseException as e:
        msg="Exception in joining classification process"+str(e)
        exception_feeder(user_id=user_id, msg=msg,client=client)
    logger.info('classification completes')

        # >>=>> LOAN ANALYSIS
    logger.info('starting loan analysis')
    try:
        result_loan = final_output(user_id)  # returns a dictionary
        if not result_loan['status']:
            msg="Loan Analysis failed due to some reason-"+result_loan['message']
            exception_feeder(client=client, user_id=user_id,
                         msg=msg)
    except BaseException as e:
        msg="Exception in Loan Analysis-"+str(e)
        exception_feeder(user_id=user_id, msg=msg,client=client)
    logger.info('loan analysis successful')


    # >>=>> Rejection check
    logger.info('starting rejection checkrejection check')
    try:
        result_rejection = check_rejection(user_id)  # returns a dictionary
        if not result_rejection['status']:
            msg = "rejection check failed due to some reason-"+result_rejection['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)
    except BaseException as e:
        msg = "rejection check failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)
    logger.info('rejection check complete')

    # >>=>> BALANCE SHEET
    logger.info('started making balanced sheet')
    try:
        result_balance_sheet = create_transaction_balanced_sheet(user_id)
        if not result_balance_sheet['status']:
            msg = "Balance Sheet check failed due to some reason-"+result_balance_sheet['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Balance Sheet failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Balance Sheet complete')


    # >>=>> CHEQUE BOUNCE ANALYSIS
    try:
        result_cheque_bounce = cheque_user_outer(user_id)
        if not result_cheque_bounce['status']:
            msg = "Cheque Bounce check failed due to some reason-"+result_cheque_bounce['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Cheque Bounce failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Cheque Bounce complete')

# >>=>> SALARY ANALYSIS
    logger.info('starting salary analysis')
    try:
        result_salary = salary_main(user_id)  # Returns a dictionary
        if not result_salary['status']:
            msg = "Salary check failed due to some reason-"+result_salary['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Salary failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Salary analysis complete')

    # >>=>> lOAN_iNFO
    try:
        result_loan_limit = loan_limit(user_id)
        if not result_loan_limit['status']:
            msg = "Loan_limit check failed due to some reason-"+result_loan_limit['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Loan limit failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Loan limit complete')

    # >>=>> lOAN_details
    try:
        result_loan_detail_final = get_final_loan_details(user_id)
        if not result_loan_detail_final['status']:
            msg = "Loan detail final check failed due to some reason-"+result_loan_detail_final['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Loan detail final failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Loan detail final complete')


    # >>=>> Available Balance
    try:
        result_available_balance = find_info(user_id)
        if not result_available_balance['status']:
            msg = "Available balance final check failed due to some reason-"+result_available_balance['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Available Balance failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Available Balance complete')


    # >>=>> Available Balance
    try:
        result_mean_available = mean_available(user_id)
        if not result_mean_available['status']:
            msg = "Mean balance final check failed due to some reason-"+result_mean_available['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Mean Balance Available failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Mean Balance Available complete')


    # >>=>> CC Limit
    try:
        result_cc_limit = get_extracted_data(user_id)
        if not result_cc_limit['status']:
            msg = "cc limit check failed due to some reason-"+result_cc_limit['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "cc limit failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('cc limit complete')


    # >>=>> Last Salary Calculate
    try:
        result_last_salary = last_sal(user_id)
        if not result_last_salary['status']:
            msg = "Last salary check failed due to some reason-"+result_last_salary['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Last Salary failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Last Salary complete')


    # >>=>> Ecs Calculate
    try:
        result_ecs_data = get_count_ecs(user_id)
        if not result_ecs_data['status']:
            msg = "Ecs data check failed due to some reason-"+result_ecs_data['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Ecs data failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Ecs data complete')


    # >>=>> Chq Bounce Calculate
    try:
        result_chq_bounce = get_count_cb(user_id)
        if not result_chq_bounce['status']:
            msg = "Cheque Bounce failed due to some reason-"+result_chq_bounce['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Cheque Bounce failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Cheque Bounce complete')


    # >>=>> Total Rejection Message
    try:
        result_rejection_message = get_defaulter(user_id)
        if not result_rejection_message['status']:
            msg = "Rejection Messages failed due to some reason-"+result_rejection_message['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Rejection Messages failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Rejection Messages complete')

    return {"status":True,"messages":"success"}
