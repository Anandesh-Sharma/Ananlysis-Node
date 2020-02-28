from .Util import logger_1, conn
from .Classifier import classifier
from .loan_main import final_output
from .Salary_Analysis import salary_analysis
from .Cheque_Bounce import cheque_user_outer
from .Loan_Salary_Logic import *
from .Analysis import analyse,cibil_analysis
from .transaction_balance_sheet import create_transaction_balanced_sheet
import warnings
import json
import pandas as pd
warnings.filterwarnings("ignore")



def bl0(cibil_score, sms_json, user_id, new_user, list_loans, current_loan):
    '''
    Implements BL0
    
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
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    logger.info('connection success')
    logger.info("checking started")
    if not isinstance(current_loan, int):
        logger.error('current_loan not int type')
        r = {'status': False, 'message': 'current_loan not int type', 'onhold': None, 'user_id': user_id,
             'limit': None,
             'logic': 'BL0'}
        client.analysisresult.bl0.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    if not isinstance(list_loans, list):
        logger.error('list_loan not list type')
        r = {'status': False, 'message': 'list_loan not list type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        client.analysisresult.bl0.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    for i in list_loans:
        if not isinstance(i, int):
            logger.error('list_loan items not int type')
            r = {'status': False, 'message': 'list_loan items not int type', 'onhold': None, 'user_id': user_id,
                 'limit': None, 'logic': 'BL0'}
            client.analysisresult.bl0.update({'_id' : user_id}, r, upsert = True)
            client.close()
            return r

    list_loans.sort()

    if not isinstance(new_user, bool):
        logger.error('new_user not boolean type')
        r = {'status': False, 'message': 'new_user not boolean type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        client.analysisresult.bl0.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    logger.info('checking variables finished')
    logger.info("starting classification")

    try:
        result = classifier(sms_json, str(user_id))
        if not result['status']:
            logger.debug('classification of messages failed')
            client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
            r=cibil_analysis(cibil_score,current_loan)
            client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
            client.close()
            return r

    except Exception as e:
        logger.debug('classification of messages failed')
        r = {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    logger.info('started making balanced sheet')
    result = create_transaction_balanced_sheet(user_id)
    if not result['status']:
        client.analysisresult.exception.update({'_id' : user_id}, result, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return result
    res = json.dumps(result)
    res = json.loads(res)
    try:
        client.analysis.balance_sheet.update({'_id' : user_id}, res, upsert = True)
        logger.info('balanced sheet complete')
    except Exception as e:
        logger.critical('error in connection')
        r = {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
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
            r = {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
            client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
            r=cibil_analysis(cibil_score,current_loan)
            client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
            client.close()
            return r

        logger.info('successfully checked bounced cheque messages')
        if a > 0:
            logger.info('user has bounced cheques exiting')
            a = {'_id': user_id, 'onhold': True, 'limit': -1, 'logic': 'BL0'}
            r = {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id, 'limit': -1,
                'logic': 'BL0'}
            client.analysisresult.chequebounce.update({'_id' : user_id}, r, upsert = True)
            client.close()
            return r
    logger.info('starting loan analysis')
    try:
        result_loan = final_output(int(user_id))
        if not result_loan['status']:
            logger.caution('Error in loan analysis')
            result_loan['onhold']=None
            result_loan['user_id']= user_id
            result_loan['limit']= None
            result_loan['logic'] = 'BL0'
            client.analysisresult.exception.update({'_id' : user_id}, result_loan, upsert = True)
            r=cibil_analysis(cibil_score,current_loan)
            client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
            client.close()
            return result_loan
    except Exception as e:
        logger.debug('error in loan analysis')
        r = {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    except:
        logger.debug('error in loan analysis')
        r = {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    logger.info('loan analysis successsful')
    logger.info('starting salary analysis')
    try:
        result_salary = salary_analysis(int(user_id))
        if not result_salary['status']:
            logger.error('Error in loan analysis')
            result_salary['onhold']=None
            result_salary['user_id']= user_id
            result_salary['limit']= None
            result_salary['logic'] = 'BL0'
            r = {'status': True, 'message': None, 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
            client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
            r=cibil_analysis(cibil_score,current_loan)
            client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
            client.close()
            return result_salary
    except Exception as e:
        logger.debug('error in salary analysis')
        r = {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    except:
        logger.debug('error in salary analysis')
        r = {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r
    
    logger.info('checking result salary and loan salary output')
    if not isinstance(result_loan['result'],dict):
        logger.caution("loan dict doesn't contain loan result")
        r={'status': False, 'message': 'result_loan not dict type', 'onhold': None, 'user_id': user_id,
        'limit': None, 'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    if 'empty' not in result_loan['result'].keys():
        logger.caution("loan dict result doesn't contain empty")
        r={'status': False, 'message': 'empty key not present in loan dict', 'onhold': None, 'user_id': user_id,
            'limit': None, 'logic': 'BL0'}
        client.analysisresult.exception.update({'_id' : user_id}, r, upsert = True)
        r=cibil_analysis(cibil_score,current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, r, upsert = True)
        client.close()
        return r

    logger.info('checking result salary and loan salary output complete')
    logger.info('Starting Analysis')
    print(result_salary)
    if float(result_salary['salary'])>0:
        salary_present=True
    else:
        salary_present=False

    if result_loan['result']['empty']:
        loan_present=False
    else:
        loan_present=True

    if salary_present and loan_present:
        result = loan_salary_analysis_function(result_salary['salary'],result_loan['result'],list_loans,current_loan,user_id)
        client.analysisresult.loan_salary.update({'_id' : user_id}, result, upsert = True)

    elif loan_present:
        result = loan_analysis_function(result_loan['result'],list_loans,current_loan,user_id)
        client.analysisresult.loan.update({'_id' : user_id}, result, upsert = True)
    
    elif salary_present:
        result = salary_analysis_function(float(result_salary['salary']),list_loans,current_loan,user_id)
        client.analysisresult.salary.update({'_id' : user_id}, result, upsert = True)
    
    else:
        result = analyse(user_id, cibil_score, new_user, current_loan)
        client.analysisresult.cibil.update({'_id' : user_id}, result, upsert = True)

    logger.info("analysis complete")
    client.analysisresult.bl0.update({'_id' : user_id}, result, upsert = True)
    client.close()
    return result