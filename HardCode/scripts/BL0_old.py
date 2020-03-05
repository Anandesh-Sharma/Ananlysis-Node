# from pymongo import MongoClient
from .Util import logger_1, conn
from .Classifier import classifier
from .Loan_Analysis import loan_analysis
from .Salary_Analysis import salary_analysis
from .Analysis import analyse
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

from time import sleep


def bl0(df_cibil, sms_json, user_id, new_user, list_loans, current_loan):
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
        return {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    logger.info('connection success')
    logger.info("checking started")
    if not isinstance(current_loan, int):
        logger.error('current_loan not int type')
        r = {'status': False, 'message': 'current_loan not int type', 'onhold': None, 'user_id': user_id,
             'limit': None,
             'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    if not isinstance(list_loans, list):
        logger.error('list_loan not list type')
        r = {'status': False, 'message': 'list_loan not list type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    if not isinstance(df_cibil, pd.DataFrame):
        logger.error('df_cibil not dataframe type')
        r = {'status': False, 'message': 'df_cibil not dataframe type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    req_col = ["account_type", "payment_history", "credit_score", "written_amt_total", "written_amt_principal",
               "payment_rating"]
    temp_l = df_cibil.columns

    for i in req_col:
        if i not in temp_l:
            logger.error('df_cibil does not contain required columns')
            r = {'status': False, 'message': "df_cibil doesn't contain required columns", 'onhold': None,
                 'user_id': user_id, 'limit': None, 'logic': 'BL0'}
            a = {"processing": False, "result": r}
            client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
            client.close()
            return r

    del temp_l

    for i in list_loans:
        if not isinstance(i, int):
            logger.error('list_loan items not int type')
            r = {'status': False, 'message': 'list_loan items not int type', 'onhold': None, 'user_id': user_id,
                 'limit': None, 'logic': 'BL0'}
            a = {"processing": False, "result": r}
            client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
            client.close()
            return r

    list_loans.sort()

    if not isinstance(new_user, bool):
        logger.error('new_user not boolean type')
        r = {'status': False, 'message': 'new_user not boolean type', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    logger.info('checking variables finished')

    logger.info('extracting saved results')

    file = client.analysisresult.result.find_one({"_id": user_id})
    if file is not None and not file["processing"]:
        r = analyse(user_id, df_cibil, new_user, current_loan)
        if not r['status']:
            logger.debug('classification of messages failed')
            r['user_id'] = user_id
            a = {"processing": False, "result": r}
            client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
            client.close()
            return r
        else:
            return r

    del file

    file1 = client.analysisresult.result.find_one({"_id": user_id})
    logger.info('extraction of data success')

    logger.info("checking for existing process")
    if file1 is None:
        d = {"_id": user_id, "processing": True}
        client.analysisresult.result.insert_one(d)

    elif file1["processing"]:
        while True:
            sleep(10)
            file1 = client.analysisresult.result.find_one({"_id": user_id})
            if not file1["processing"]:
                client.close()
                return file1["result"]

    logger.info("starting classification")

    try:
        result = classifier(sms_json, str(user_id))
        if not result['status']:
            logger.debug('classification of messages failed')
            result['user_id'] = user_id
            r = result
            a = {"processing": False, "result": r}
            client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
            client.close()
            return r

    except Exception as e:
        logger.debug('classification of messages failed')
        r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    logger.info('starting loan analysis')
    try:
        loan_analysis(int(user_id))
    except Exception as e:
        logger.debug('error in loan analysis')
        r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    except:
        logger.debug('error in loan analysis')
        r = {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    logger.info('loan analysis successsful')
    logger.info('starting salary analysis')
    try:
        salary_analysis(int(user_id))
    except Exception as e:
        logger.debug('error in salary analysis')
        r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
             'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    except:
        logger.debug('error in salary analysis')
        r = {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
             'limit': None, 'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    logger.info('salary analysis successsful')
    r = analyse(user_id, df_cibil, new_user, current_loan)
    if not r['status']:
        logger.debug('classification of messages failed')
        r['user_id'] = user_id
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r
    else:
        return r
