from .Util import logger_1, conn
from .Classifier import classifier
from .loan_main import final_output
from .Salary_Analysis import salary_analysis
from .Cheque_Bounce import cheque_user_outer
from .Loan_Salary_Logic import *
from .Analysis import analyse
from .transaction_balance_sheet import create_transaction_balanced_sheet
import warnings
import json
import pandas as pd
import datetime

warnings.filterwarnings("ignore")


def bl0(**kwargs):
    # cibil_score, sms_json, user_id, new_user, list_loans, current_loan
    cibil_score = kwargs.get('cibil_score')
    sms_json = kwargs.get('sms_json')
    user_id = kwargs.get('user_id')
    new_user = kwargs.get('new_user')
    list_loans = kwargs.get('list_loans')
    current_loan = kwargs.get('current_loan')
    cibil_df = kwargs.get('cibil_xml')
    '''
    user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                                        new_user=new_user
                                        , cibil_score=cibil_score
    '''
    '''Implements BL0
    
    Amount of the loan is calculated on the basis of cibil score and checks 
    the if there are any bounced check of the user and stores the user sms.

    Parameters:
    df_cibil (Data Frame)    :Containing fields of individual users with column names
        account_type(int)           : type of account
        payment_history(string)     : payment histroy of individual loan of user
        credit_score(int)           : credit score of the user
        written_amt_total(int)      : written amount total of specific loan
        written_amt_principal(int)  : written principle total of specific loan
        payment_rating(int)         : payment rating of a person
    
    sms_json(json object)   :containing the sms of the user
        timestamp(string)       :main dictionary containing keys
            body(string)            :body of message
            sender(string)          :sender's name
            read(bool)              :whether the message is seen
    
    user_id(int)            :user's specific id
    new_user(bool)          :Whether the user is new or not 
    list_loans(list)(int)   :list containing integers of current loan types available
    current_loan(int)       :current loan given to the user

    Returns:
    dict    :containing follwing keys
        status(bool)    :whether the code worked correctly
        message(string) :explains the status
        onhold(bool)    :user is on hold or not
        user_id(int)    :user's specific id
        limit(int)      :limiting amount of user calculated
        logic(string)   :buissness logic of the process
    '''
    logger = logger_1('bl0', -1)
    if not isinstance(user_id, int):
        logger.error('user_id not int type')
        return {'status': False, 'message': 'user_id not int type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}

    logger = logger_1('bl0', user_id)
    try:
        logger.info('making connection with db')
        client = conn()
    except Exception as e:
        logger.critical('error in connection')
        return {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    logger.info('connection success')
    logger.info("checking started")
    if not isinstance(current_loan, int):
        logger.error('current_loan not int type')
        r = {'status': False, 'message': 'current_loan not int type', 'onhold': None, 'user_id': user_id,
             'limit': None,
             'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.bl0.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    if not isinstance(list_loans, list):
        logger.error('list_loan not list type')
        r = {'status': False, 'message': 'list_loan not list type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.bl0.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    for i in list_loans:
        if not isinstance(i, int):
            logger.error('list_loan items not int type')
            r = {'status': False, 'message': 'list_loan items not int type', 'onhold': None, 'user_id': user_id,
                 'limit': None, 'logic': 'BL0'}
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.bl0.update({'cust_id': user_id}, r, upsert=True)
            client.close()
            return r

    list_loans.sort()

    if not isinstance(new_user, bool):
        logger.error('new_user not boolean type')
        r = {'status': False, 'message': 'new_user not boolean type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.bl0.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    logger.info('checking variables finished')
    logger.info("starting classification")

    try:
        result = classifier(sms_json, str(user_id))
        r = result
        if not result['status']:
            logger.debug('classification of messages failed')
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
            r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                        new_user=new_user
                        , cibil_score=cibil_score)
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
            client.close()
            return r

    except Exception as e:
        logger.debug('classification of messages failed')
        r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    logger.info('started making balanced sheet')
    result = create_transaction_balanced_sheet(user_id)
    if not result['status']:
        result['modified_at'] = str(datetime.datetime.now())
        result['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, result, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return result
    res = json.dumps(result)
    res = json.loads(res)
    try:
        res['modified_at'] = str(datetime.datetime.now())
        client.analysis.balance_sheet.update({'cust_id': user_id}, res, upsert=True)
        logger.info('balanced sheet complete')
    except Exception as e:
        logger.critical('error in connection')
        r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None, 'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    logger.info('starting loan analysis')
    try:
        result_loan = final_output(int(user_id))
        if not result_loan['status']:
            logger.caution('Error in loan analysis')
            result_loan['onhold'] = None
            result_loan['user_id'] = user_id
            result_loan['limit'] = None
            result_loan['logic'] = 'BL0'
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.exception_bl0.update({'cust_id': user_id}, result_loan, upsert=True)
            r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                        new_user=new_user
                        , cibil_score=cibil_score)
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
            client.close()
            return result_loan
    except Exception as e:
        logger.debug('error in loan analysis')
        r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    except:
        logger.debug('error in loan analysis')
        r = {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    logger.info('loan analysis successsful')
    logger.info('starting salary analysis')
    try:
        result_salary = salary_analysis(int(user_id))
        if not result_salary['status']:
            logger.error('Error in loan analysis')
            result_salary['onhold'] = None
            result_salary['user_id'] = user_id
            result_salary['limit'] = None
            result_salary['logic'] = 'BL0'
            r = {'status': True, 'message': None, 'onhold': None, 'user_id': user_id, 'limit': None,
                 'logic': 'BL0'}
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
            r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                        new_user=new_user
                        , cibil_score=cibil_score)
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
            client.close()
            return result_salary
    except Exception as e:
        logger.debug('error in salary analysis')
        r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    except:
        logger.debug('error in salary analysis')
        r = {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    if new_user:
        try:
            file1 = client.messagecluster.extra.find_one({"_id": user_id})
            if file1 is None:
                a = 0
            else:
                df = pd.DataFrame(file1['sms'])
                a = cheque_user_outer(df, user_id)
        except Exception as e:
            logger.debug('error occured during checking bounced cheque messages')
            r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                 'logic': 'BL0'}
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
            r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                        new_user=new_user
                        , cibil_score=cibil_score)
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
            client.close()
            return r

        logger.info('successfully checked bounced cheque messages')
        if a > 0:
            logger.info('user has bounced cheques exiting')
            a = {'_id': user_id, 'onhold': True, 'limit': -1, 'logic': 'BL0'}
            r = {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id, 'limit': -1,
                 'logic': 'BL0'}
            r['modified_at'] = str(datetime.datetime.now())
            r['cust_id']=user_id
            client.analysisresult.chequebounce_bl0.update({'cust_id': user_id}, r, upsert=True)
            client.close()
            return r

    logger.info('checking result salary and loan salary output')
    if not isinstance(result_loan['result'], dict):
        logger.caution("loan dict doesn't contain loan result")
        r = {'status': False, 'message': 'result_loan not dict type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    if 'empty' not in result_loan['result'].keys():
        logger.caution("loan dict result doesn't contain empty")
        r = {'status': False, 'message': 'empty key not present in loan dict', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.exception_bl0.update({'cust_id': user_id}, r, upsert=True)
        r = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                    new_user=new_user
                    , cibil_score=cibil_score)
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r
    try:
        result_loan['result']['CURRENT_OPEN'] = int(result_loan['result']['CURRENT_OPEN'])
    except:
        result_loan['result']['CURRENT_OPEN'] = 0
    try:
        result_loan['result']['TOTAL_LOANS'] = int(result_loan['result']['TOTAL_LOANS'])
    except:
        result_loan['result']['TOTAL_LOANS'] = 0
    try:
        result_loan['result']['PAY_WITHIN_30_DAYS'] = bool(result_loan['result']['PAY_WITHIN_30_DAYS'])
    except:
        result_loan['result']['PAY_WITHIN_30_DAYS'] = False
    try:
        result_loan['result']['MAX_AMOUNT'] = float(result_loan['result']['MAX_AMOUNT'])
    except:
        result_loan['result']['MAX_AMOUNT'] = 0
    try:
        result_loan['result']['empty'] = bool(result_loan['result']['empty'])
    except:
        result_loan['result']['empty'] = True
    g = []
    for i in result_loan['result']['CURRENT_OPEN_AMOUNT']:
        if i is None:
            continue
        else:
            try:
                g.append(float(i))
            except:
                continue
    result_loan['result']['CURRENT_OPEN_AMOUNT'] = g

    logger.info('checking result salary and loan salary output complete')

    logger.info('Checking if a person has done default')

    if not result_loan['result']['PAY_WITHIN_30_DAYS']:
        logger.info('defaulter on the basis of loan')
        r = {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id,
             'limit': -1, 'logic': 'BL0-loan'}
        r['modified_at'] = str(datetime.datetime.now())
        r['cust_id']=user_id
        client.analysisresult.loan_bl0.update({'cust_id': user_id}, r, upsert=True)
        client.close()
        return r

    logger.info('Not a defaulter')
    logger.info('Starting Analysis')
    salary_present = False
    if float(result_salary['salary']) > 0:
        salary_present = True
    if result_loan['result']['empty']:
        loan_present = False
    else:
        loan_present = True

    if salary_present and loan_present:
        result = loan_salary_analysis_function(result_salary['salary'], result_loan['result'], list_loans, current_loan,
                                               user_id, new_user)
        result['modified_at'] = str(datetime.datetime.now())
        result['cust_id']=user_id
        client.analysisresult.loan_salary_bl0.update({'cust_id': user_id}, result, upsert=True)

    elif loan_present:
        result = loan_analysis_function(result_loan['result'], list_loans, current_loan, user_id, new_user)
        result['modified_at'] = str(datetime.datetime.now())
        result['cust_id']=user_id
        client.analysisresult.loan_bl0.update({'cust_id': user_id}, result, upsert=True)

    elif salary_present:
        result = salary_analysis_function(float(result_salary['salary']), list_loans, current_loan, user_id, new_user)
        result['modified_at'] = str(datetime.datetime.now())
        result['cust_id']=user_id
        client.analysisresult.salary_bl0.update({'cust_id': user_id}, result, upsert=True)

    else:
        result = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                         new_user=new_user
                         , cibil_score=cibil_score)
        result['modified_at'] = str(datetime.datetime.now())
        result['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, result, upsert=True)

    if int(result['limit']) < 3000:
        result = analyse(user_id=user_id, current_loan=current_loan, cibil_df=cibil_df,
                         new_user=new_user
                         , cibil_score=cibil_score)
        result['modified_at'] = str(datetime.datetime.now())
        result['cust_id']=user_id
        client.analysisresult.cibil.update({'cust_id': user_id}, result, upsert=True)

    logger.info("analysis complete")
    client.close()
    return result
