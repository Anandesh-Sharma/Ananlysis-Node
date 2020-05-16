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
from HardCode.scripts.parameters_for_bl0.credit_card_limit.cc_limit import get_cc_limit
from HardCode.scripts.parameters_for_bl0.salary.salary_count import last_sal
from HardCode.scripts.parameters_for_bl0.ecs_bounce.ecs_bounce import get_count_ecs
from HardCode.scripts.parameters_for_bl0.ecs_bounce.chq_bounce import get_count_cb
from HardCode.scripts.parameters_for_bl0.rejection_msgs.total_rejection_msg import get_defaulter
from HardCode.scripts.parameters_for_bl0.account_status.status import get_acc_status
from HardCode.scripts.parameters_for_bl0.active_close_status.active_closed_count import get_active_closed
from HardCode.scripts.parameters_for_bl0.age_of_oldest_trade.age import age_oldest_trade
from HardCode.scripts.parameters_for_bl0.age_of_user.user_age import get_age
from HardCode.scripts.parameters_for_bl0.loan_app.loan_app_count_validate import loan_app_count
from HardCode.scripts.parameters_for_bl0.payment_rating.pay_rating import get_payment_rating
from HardCode.scripts.parameters_for_bl0.reapyment_history.repayment_history import repayment_history
from HardCode.scripts.parameters_for_bl0.reference_verification.validation.check_reference import validate
from HardCode.scripts.parameters_for_bl0.relative_verification.relative_validation import rel_validate
from HardCode.scripts.parameters_for_bl0.secured_unsecured_loans.count import secure_unsecured_loan
from HardCode.scripts.parameters_for_bl0.user_name_msg.name_count_ratio import get_name_count
from HardCode.scripts.rule_based_model.rule_engine import rule_engine_main
from HardCode.scripts.model_0.scoring.generate_total_score import get_score
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
    cibil_df = kwargs.get('cibil')

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

    # >>=>> lOAN_INFO
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


    # >>=>> Mean Available Balance
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
        result_cc_limit = get_cc_limit(user_id)
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

    # >>=>> Account status
    try:
        account_status = get_acc_status(user_id,cibil_df)
        if not account_status['status']:
            msg = "Account status failed due to some reason-"+account_status['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Account status failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Account status complete')


    # >>=>> Active Close
    try:
        active_closed = get_active_closed(user_id,cibil_df)
        if not active_closed['status']:
            msg = "Active closed loans failed due to some reason-"+active_closed['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Active closed loans failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Active closed loans complete')


    # >>=>> Age of oldest Trade
    try:
        age_of_oldest_trade = age_oldest_trade(user_id,cibil_df)
        if not age_of_oldest_trade['status']:
            msg = "Age of oldest trade failed due to some reason-"+age_of_oldest_trade['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Age of oldest trade failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Age of oldest trade complete')

    # >>=>> Age of user
    try:
        age_of_user = get_age(user_id)
        if not age_of_user['status']:
            msg = "Age of user failed due to some reason-"+age_of_user['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Age of user failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Age of user complete')

    # >>=>> Loan App Percentage
    try:
        loan_app_percent = loan_app_count(user_id)
        if not loan_app_percent['status']:
            msg = "Loan App Percentage failed due to some reason-"+loan_app_percent['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Loan App Percentage failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Loan App Percentage complete')

    # >>=>> Payment Rating
    try:
        payment_rating = get_payment_rating(user_id,cibil_df)
        if not payment_rating['status']:
            msg = "Payment Rating failed due to some reason-"+payment_rating['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Payment Rating failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Payment Rating complete')

    # >>=>> Repayment History
    try:
        repay_history = repayment_history(user_id)
        if not repay_history['status']:
            msg = "Repayment History failed due to some reason-"+repay_history['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Repayment History failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Repayment History complete')

    # >>=>> Reference
    try:
        reference = validate(user_id)
        if not reference['status']:
            msg = "Reference failed due to some reason-"+reference['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Reference failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Reference verification complete')

    # >>=>> Relatives
    try:
        relatives = rel_validate(user_id)
        if not relatives['status']:
            msg = "Relatives failed due to some reason-"+relatives['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Relatives failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Relatives verification complete')

    # >>=>> Secured Unsecured Loans
    try:
        secured_unsecured = secure_unsecured_loan(user_id,cibil_df)
        if not secured_unsecured['status']:
            msg = "Secured Unsecured Loans failed due to some reason-"+secured_unsecured['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Secured Unsecured Loans failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Secured Unsecured Loans complete')

    # >>=>> Username messages
    try:
        username_msg = get_name_count(user_id)
        if not username_msg['status']:
            msg = "Username messages failed due to some reason-"+username_msg['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,msg=msg)
    except BaseException as e:
        msg = "Username messages failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,msg=msg)
    logger.info('Username messages count complete')

    # >>=>> Scoring Model
    # try:
    #     result_score = get_score(user_id,sms_count)
    #     if not result_score['status']:
    #         msg = "Scoring Model failed due to some reason"
    #         logger.error(msg)
    #         exception_feeder(client=client, user_id=user_id,msg=msg)
    # except BaseException as e:
    #     msg = "Scoring Model failed due to some reason-"+str(e)
    #     logger.error(msg)
    #     exception_feeder(client=client, user_id=user_id,msg=msg)
    # logger.info('Scoring Model complete')

    # >>=>> Rule Engine
    # try:
    #     rule_engine = rule_engine_main(user_id)
    #     if not rule_engine['status']:
    #         msg = "Rule engine failed due to some reason-"+rule_engine['message']
    #         logger.error(msg)
    #         exception_feeder(client=client, user_id=user_id,msg=msg)
    # except BaseException as e:
    #     msg = "Rule engine failed due to some reason-"+str(e)
    #     logger.error(msg)
    #     exception_feeder(client=client, user_id=user_id,msg=msg)
    # logger.info('Rule engine complete')


    return {"status":True,"messages":"success"}
