from HardCode.scripts.loan_analysis.loan_main import final_output
from HardCode.scripts.salary_analysis.monthly_salary_analysis import main
from HardCode.scripts.cheque_bounce_analysis.Cheque_Bounce import cheque_user_outer
from HardCode.scripts.loan_salary_analysis.Loan_Salary_Logic import *
from HardCode.scripts.cibil.Analysis import analyse
from HardCode.scripts.balance_sheet_analysis.transaction_balance_sheet import create_transaction_balanced_sheet
from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.classifiers.Classifier import classifier
# from HardCode.scripts.reference_verification.validation.check_reference import validate
from HardCode.scripts.Util import *
from HardCode.scripts.model_0.scoring.generate_total_score import get_score
import warnings
import multiprocessing
import pandas as pd
import pytz

warnings.filterwarnings("ignore")


def exception_feeder(**kwargs):
    client = kwargs.get('client')
    logger = kwargs.get('logger')
    msg = kwargs.get('msg')
    user_id = kwargs.get('user_id')

    logger.error(msg)
    r = {'status': False, 'message': msg, 'limit': None,
         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
    if client:
        client.analysisresult.exception_bl0.insert_one(r)
    return r


# TODO : return the universal response for middleware
def result_fetcher(**kwargs):
    user_id = kwargs.get('user_id')
    client = kwargs.get('client')
    loan_result = kwargs.get('result_loan')
    salary_result = kwargs.get('result_salary')
    balance_sheet = kwargs.get('balance_sheet_result')
    rejection = kwargs.get('result_rejection')
    score = kwargs.get('result_score')
    output_flag = kwargs.get('output_flag', 'cibil')
    test_final_result = client.analysisresult.bl0.find_one({'cust_id': user_id})
    final_result = test_final_result['result'][-1:][0]

    del final_result['modified_at']
    del test_final_result['result']
    test_final_result['result'] = final_result
    del test_final_result['_id']
    del score['cust_id']
    del loan_result['result']['cust_id']
    del salary_result['cust_id']
    del rejection['cust_id']

    analysis = {
        'salary': salary_result,
        'loan': loan_result,
        'balance_sheet': balance_sheet,
        'rejection_check': rejection

    }
    response = {
        'status': True,
        'output_flag': output_flag,
        'limit': test_final_result['result'][output_flag],
        'user_id': user_id,

    }
    test_final_result['Model_0'] = score['Model_0']
    test_final_result['analysis'] = analysis
    return test_final_result


def set_processing_bool(user_id, status):
    client = conn()
    client.user_process.bl0.update_one({'cust_id': user_id}, {'$set': {'processing': status}}, upsert=True)


def bl0(**kwargs):
    # cibil_score, sms_json, user_id, new_user, list_loans, current_loan
    cibil_score = kwargs.get('cibil_score')
    # sms_json = kwargs.get('sms_json')
    user_id = kwargs.get('user_id')
    new_user = kwargs.get('new_user')
    list_loans = kwargs.get('list_loans')
    current_loan = kwargs.get('current_loan')
    cibil_df = kwargs.get('cibil_xml')
    sms_json = kwargs.get('sms_json')

    logger = logger_1('bl0', user_id)
    if not isinstance(user_id, int):
        return exception_feeder(user_id=user_id, msg='user_id not int type', logger=logger)

    try:
        logger.info('making connection with db')
        client = conn()
    except BaseException as e:
        logger.critical('error in connection')
        return exception_feeder(user_id=user_id, msg=str(e), logger=logger)

    logger.info('connection success')

    # CHECK IF THE USER_ID is already processing or just got a new hit !!!
    user_process_bool = client.user_process.bl0.find_one({'cust_id': user_id})
    if not user_process_bool:
        set_processing_bool(user_id, True)
    elif user_process_bool['processing']:
        return
    else:
        set_processing_bool(user_id, True)

    logger.info("checking started")

    # typechecking current_loan
    if not isinstance(current_loan, int):
        tc_r = exception_feeder(user_id=user_id, msg='current_loan not int type', logger=logger, client=client)
        client.close()
        return tc_r
    # typechecking list_loans
    if not isinstance(list_loans, list):
        tc_r = exception_feeder(user_id=user_id, msg='list_loan not list type', logger=logger, client=client)
        client.close()
        return tc_r
    # typechecking list_loans elements
    for i in list_loans:
        if not isinstance(i, int):
            logger.error('list_loan items not int type')
            tc_r = exception_feeder(user_id=user_id, msg='list_loan items not int type', logger=logger,
                                    client=client)
            client.close()
            return tc_r

    list_loans.sort()
    # typechecking new_user
    if not isinstance(new_user, bool):
        tc_r = exception_feeder(user_id=user_id, msg='new_user not boolean type', logger=logger, client=client)
        client.close()
        return tc_r

    logger.info('checking variables finished')
    # logger.info("starting classification")

    # initialising results in db
    r = {"cust_id": user_id,
         "status": True,
         "result": []
         }
    analysis_result = {}
    # IFF customer's data is not already present in the database
    try:
        # insert again if result needs to be updated!
        if not client.analysisresult.bl0.find_one({"cust_id": user_id}):
            client.analysisresult.bl0.insert_one(r)
    except BaseException as e:
        # exit and return
        return exception_feeder(client=client, logger=logger, msg=f'default result not generated {e}',
                                user_id=user_id)

    # >>==>> Classification
    logger.info('starting classification')
    p = multiprocessing.Process(target=classifier, args=(sms_json, str(user_id),))
    try:
        p.start()
    except Exception as e:
        print(e)
    try:
        p.join()
    except Exception as e:
        print(e)
    # classifier_result = classifier(sms_json, str(user_id))
    # if not classifier_result:   # returns a bool
    #     exception_feeder(client=client, user_id=user_id, logger=logger, msg='error in classification')

    logger.info('classification completes')

    # >>=>> BALANCE SHEET
    logger.info('started making balanced sheet')
    try:
        balance_sheet_result = create_transaction_balanced_sheet(user_id)
        if not balance_sheet_result['status']:
            exception_feeder(client=client, user_id=user_id, logger=logger,
                             msg="Balance sheet error-" + str(balance_sheet_result['message']))
    except BaseException as e:
        exception_feeder(client=client, logger=logger, msg=f'unhandeled error in balanced sheet {e}',
                         user_id=user_id)

    # >>=>> LOAN ANALYSIS
    logger.info('starting loan analysis')
    result_loan = final_output(user_id)  # returns a dictionary

    if result_loan['status']:
        pass
    if not result_loan['status']:
        exception_feeder(client=client, user_id=user_id, logger=logger,
                         msg="Loan Analysis failed due to some reason")
    logger.info('loan analysis successful')

    # >>=>> Rejection check
    logger.info('starting rejection check')
    result_rejection = check_rejection(user_id)  # returns a dictionary
    if result_rejection['status']:
        pass
    if not result_rejection['status']:
        exception_feeder(client=client, user_id=user_id, logger=logger,
                         msg="rejection check failed due to some reason")
    logger.info('rejection check complete')

    # >>=>> reference verification

    # logger.info('starting reference verification')
    # result_verification = validate(user_id)  # returns a dictionary
    # if result_verification['status']:
    #     pass
    # if not result_verification['status']:
    #     exception_feeder(client=client, user_id=user_id, logger=logger,
    #                      msg="reference verification failed due to some reason")
    # logger.info('reference verification complete')

    # >>=>> SALARY ANALYSIS
    logger.info('starting salary analysis')
    try:
        result_salary = main(user_id)  # Returns a dictionary
        if not result_salary['status']:
            exception_feeder(client=client, user_id=user_id, logger=logger,
                             msg="Salary Analysis failed due to some reason")
    except BaseException as e:
        exception_feeder(client=client, user_id=user_id, logger=logger,
                         msg=str(e))
        # -> Run BASE CIBIL logic and handle
        pass

    # >>=>> CHEQUE BOUNCE ANALYSIS
    if new_user:
        file1 = client.messagecluster.extra.find_one({"cust_id": user_id})
        if file1 is None:
            a = 0
        else:
            df = pd.DataFrame(file1['sms'])
            a, msg = cheque_user_outer(df, user_id)  # corrected BAWASEER BUG
        logger.info('successfully checked bounced cheque messages')
        if a > 0:
            logger.info('user has bounced cheques exiting')
            analysis_result['cheque_bounce'] = True
            # TODO : CREATE A NEW FUNCTION THAT FINDS THE RESULT IN DB AND RETURN IT TO MIDDLEWARE

    # >>=>> UTILIZING LOAN ANALYSIS
    # TODO : Change the logic for loan
    logger.info('Checking if a person has done default')
    if not result_loan['result']['PAY_WITHIN_30_DAYS']:
        logger.info('defaulter on the basis of loan')
        loan_limit = -1
        analysis_result['loan'] = loan_limit
        # TODO : CREATE A NEW FUNCTION THAT FINDS THE RESULT IN DB AND RETURN IT TO MIDDLEWARE
    logger.info('Not a defaulter')
    logger.info('Starting Analysis')

    logger.info("Scoring Model starts")
    try:

        result_score = get_score(user_id, cibil_df)  # Returns a dictionary
        if not result_score['status']:
            exception_feeder(client=client, user_id=user_id, logger=logger,
                             msg="scoring model failed due to some reason")
    except BaseException as e:
        print(f"Error : {e}")
        exception_feeder(client=client, user_id=user_id, logger=logger,
                         msg=str(e))

    salary_present = False
    loan_present = False

    if float(result_salary['salary']) > 0:
        salary_present = True

    if not result_loan['result']['empty']:
        loan_present = True

    if salary_present and loan_present:
        result = loan_salary_analysis_function(result_salary['salary'], result_loan['result'], list_loans, current_loan,
                                               user_id, new_user)
        limit = result['limit']
        analysis_result['loan_salary'] = limit
    else:
        analysis_result['loan_salary'] = -9

    if loan_present:
        result = loan_analysis_function(result_loan['result'], list_loans, current_loan, user_id, new_user)
        limit = result['limit']
        analysis_result['loan'] = limit
    else:
        analysis_result['loan'] = -9

    if salary_present:
        result = salary_analysis_function(float(result_salary['salary']), list_loans, current_loan, user_id, new_user)
        limit = result['limit']
        analysis_result['salary'] = limit
    else:
        analysis_result['salary'] = -9

    # BASE CIBIL CASE
    limit = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df, new_user=new_user,
                    cibil_score=cibil_score)
    analysis_result['cibil'] = limit
    analysis_result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))

    # PUSH analysis_result to the mongo
    client.analysisresult.bl0.update({'cust_id': user_id}, {'$push': {'result': analysis_result}})
    end_result = result_fetcher(client=client, user_id=user_id, result_loan=result_loan, result_salary=result_salary,
                                balance_sheet_result=balance_sheet_result, result_rejection=result_rejection,
                                result_score=result_score)
    logger.info("Setting processing bool to false")
    set_processing_bool(user_id, False)
    client.close()
    logger.info("analysis complete")
    return end_result
