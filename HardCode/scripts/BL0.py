from HardCode.scripts.classifiers.Classifier import classifier
from HardCode.scripts.loan_analysis.loan_main import final_output
from HardCode.scripts.salary_analysis.Salary_Analysis import salary_analysis
from HardCode.scripts.cheque_bounce_analysis.Cheque_Bounce import cheque_user_outer
from HardCode.scripts.loan_salary_analysis.Loan_Salary_Logic import *
from HardCode.scripts.cibil.Analysis import analyse
from HardCode.scripts.balance_sheet_analysis.transaction_balance_sheet import create_transaction_balanced_sheet
from HardCode.scripts.rejection.rejected import check_rejection
from HardCode.scripts.Util import *
import warnings
import json
import pandas as pd
from datetime import datetime
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

    output_flag = kwargs.get('output_flag', 'cibil')
    test_final_result = client.analysisresult.bl0.find_one({'cust_id': user_id})
    final_result = test_final_result['result'][-1:][0]

    del final_result['modified_at']
    del test_final_result['result']
    test_final_result['result'] = final_result
    del test_final_result['_id']

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
    test_final_result['analysis'] = analysis
    return test_final_result


def bl0(**kwargs):
    # cibil_score, sms_json, user_id, new_user, list_loans, current_loan
    cibil_score = kwargs.get('cibil_score')
    # sms_json = kwargs.get('sms_json')
    user_id = kwargs.get('user_id')
    new_user = kwargs.get('new_user')
    list_loans = kwargs.get('list_loans')
    current_loan = kwargs.get('current_loan')
    cibil_df = kwargs.get('cibil_xml')
    classification_flag = kwargs.get('classification_flag')

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

    # >>=>> BALANCE SHEET
    logger.info('started making balanced sheet')
    if not classification_flag:
        balance_sheet_result = create_transaction_balanced_sheet(user_id)
        if balance_sheet_result['status']:
            if balance_sheet_result['new']:
                bs_res = json.dumps(balance_sheet_result)
                bs_res = json.loads(bs_res)
                bs_res['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
                bs_res['cust_id'] = user_id
                try:
                    client.analysis.balance_sheet.update({'cust_id': user_id}, {"$set": bs_res}, upsert=True)
                    logger.info('balanced sheet found and saved')
                except BaseException as e:
                    logger.critical(f'error in balanced sheet data upload as {e}')
                    exception_feeder(client=client, user_id=user_id, logger=logger,
                                     msg="Balance sheet error")
            else:
                logger.info("Old User updation balance_sheet")
                if not balance_sheet_result['upto_date']:
                    try:
                        for i in range(len(balance_sheet_result['df']['sheet'])):
                            client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                                "$push": {"df.sheet": balance_sheet_result['df']['sheet'][i]}})

                        credit = balance_sheet_result['old_credit']
                        debit = balance_sheet_result['old_debit']
                        if balance_sheet_result['df']['credit'][0][0] in credit[0]:
                            client.analysis.balance_sheet.update_one({"cust_id": int(user_id)}, {
                                "$set": {
                                    'df.credit.' + str(balance_sheet_result['len_credit'] - 1) + ".1": credit[1] +
                                                                                                       balance_sheet_result[
                                                                                                           'df'][
                                                                                                           'credit'][0][
                                                                                                           1],
                                    'df.debit.' + str(balance_sheet_result['len_credit'] - 1) + ".1": debit[1] +
                                                                                                      balance_sheet_result[
                                                                                                          'df'][
                                                                                                          'debit'][0][1]
                                }}, upsert=True)
                            for i in range(len(balance_sheet_result['df']['credit']) - 1):
                                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                                    "$push": {"df.credit": balance_sheet_result['df']['credit'][i + 1]}})

                            for i in range(len(balance_sheet_result['df']['debit']) - 1):
                                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                                    "$push": {"df.debit": balance_sheet_result['df']['debit'][i + 1]}})

                        else:
                            for i in range(len(balance_sheet_result['df']['credit'])):
                                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                                    "$push": {"df.credit": balance_sheet_result['df']['credit'][i]}})

                            for i in range(len(balance_sheet_result['df']['debit'])):
                                client.analysis.balance_sheet.update({"cust_id": int(user_id)}, {
                                    "$push": {"df.debit": balance_sheet_result['df']['debit'][i]}})

                        logger.info("balanced sheet sms of old user updated successfully")
                        client.analysis.balance_sheet.update_one({"cust_id": int(user_id)}, {
                            "$set": {"timestamp": balance_sheet_result['max_timestamp'],
                                     'df.final_credit': balance_sheet_result['df']['final_credit'],
                                     'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                                                 upsert=True)
                    except BaseException as e:
                        logger.critical(f'error in balanced sheet data upload as {e}')
                        exception_feeder(client=client, user_id=user_id, logger=logger,
                                         msg="Balance sheet error-" + str(e))
                else:
                    logger.info("transaction messages upto date")
        else:
            exception_feeder(client=client, user_id=user_id, logger=logger,
                             msg="Balance sheet error")

    # >>=>> LOAN ANALYSIS
    logger.info('starting loan analysis')
    if not classification_flag:
        result_loan = final_output(user_id)  # returns a dictionary

        if result_loan['status']:
            pass
        if not result_loan['status']:
            exception_feeder(client=client, user_id=user_id, logger=logger,
                             msg="Loan Analysis failed due to some reason")
        logger.info('loan analysis successful')

    # >>=>> Rejection check
    logger.info('starting rejection check')
    if not classification_flag:
        result_rejection = check_rejection(user_id)  # returns a dictionary

        if result_rejection['status']:
            pass
        if not result_rejection['status']:
            exception_feeder(client=client, user_id=user_id, logger=logger,
                             msg="rejection check failed due to some reason")
        logger.info('rejection check successful')

    # >>=>> SALARY ANALYSIS
    logger.info('starting salary analysis')
    if not classification_flag:
        try:
            result_salary = salary_analysis(user_id)  # Returns a dictionary

            if result_salary['status']:
                pass
            if not result_salary['status']:
                exception_feeder(client=client, user_id=user_id, logger=logger,
                                 msg="Salary Analysis failed due to some reason")

        except BaseException as e:

            exception_feeder(client=client, user_id=user_id, logger=logger,
                             msg=str(e))
            # -> Run BASE CIBIL logic and handle
            pass
    else:
        exception_feeder(client=client, user_id=user_id, logger=logger, msg='error in classification')

    # >>=>> CHEQUE BOUNCE ANALYSIS
    if not classification_flag:
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
    analysis_result['modified_at'] = datetime.now(pytz.timezone('Asia/Kolkata'))

    # PUSH analysis_result to the mongo
    client.analysisresult.bl0.update({'cust_id': user_id}, {'$push': {'result': analysis_result}})
    logger.info("analysis complete")
    end_result = result_fetcher(client=client, user_id=user_id, result_loan=result_loan, result_salary=result_salary,
                                balance_sheet_result=balance_sheet_result, result_rejection=result_rejection)
    client.close()
    return end_result
