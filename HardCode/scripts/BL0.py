from pymongo import MongoClient
from .Util import logger_1, conn
from .Classifier import classifier
from .cibil_analysis import cibil_analysis
from .Cheque_Bounce import cheque_user_outer
from .Loan_Analysis import loan_analysis
from .Salary_Analysis import salary_analysis
import pandas as pd
from datetime import datetime

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
    if not isinstance(current_loan, int):
        logger.error('current_loan not int type')
        return {'status': False, 'message': 'current_loan not int type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}

    if not isinstance(list_loans, list):
        logger.error('list_loan not list type')
        return {'status': False, 'message': 'list_loan not list type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}

    if not isinstance(df_cibil, pd.DataFrame):
        logger.error('df_cibil not dataframe type')
        return {'status': False, 'message': 'df_cibil not dataframe type', 'onhold': None, 'user_id': user_id,
                'limit': None, 'logic': 'BL0'}

    req_col = ["account_type", "payment_history", "credit_score", "written_amt_total", "written_amt_principal",
               "payment_rating"]
    temp_l = df_cibil.columns

    for i in req_col:
        if i not in temp_l:
            logger.error('df_cibil does not contain required columns')
            return {'status': False, 'message': "df_cibil doesn't contain required columns", 'onhold': None,
                    'user_id': user_id, 'limit': None, 'logic': 'BL0'}

    del temp_l

    for i in list_loans:
        if not isinstance(i, int):
            logger.error('list_loan items not int type')
            return {'status': False, 'message': 'list_loan items not int type', 'onhold': None, 'user_id': user_id,
                    'limit': None, 'logic': 'BL0'}

    list_loans.sort()

    if not isinstance(new_user, bool):
        logger.error('new_user not boolean type')
        return {'status': False, 'message': 'new_user not boolean type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}

    try:  # changes to be added for updating sms
        logger.info('making connection with db')
        client = conn()
        logger.info('connection success')
        db = client.messagecluster
        file1 = db.transaction.find_one({"_id": user_id})
        logger.info('extraction of data success')
        result = classifier(sms_json, str(user_id))
        if not result['status']:
            logger.debug('updation of messages failed')
            client.close()
    except Exception as e:
        logger.critical('error in connection')
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    logger.info('checking if file already exists')

    if not file1:
        logger.info('user id already exists')
        logger.info('classification of messages started')
        result = classifier(sms_json, str(user_id))

        if not result['status']:
            logger.debug('classification of messages failed')
            client.close()
            result['user_id'] = user_id
            return result

    logger.info('starting loan analysis')
    try:
        loan_analysis(str(user_id))
    except Exception as e:
        logger.debug('error in loan analysis')
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    except:
        logger.debug('error in loan analysis')
        return {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}
    logger.info('loan analysis successsful')
    logger.info('starting salary analysis')
    try:
        salary_analysis(str(user_id))
    except Exception as e:
        logger.debug('error in salary analysis')
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    except:
        logger.debug('error in salary analysis')
        return {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}
    logger.info('salary analysis successsful')
    logger.info('Starting checking bounced cheque messages')
    try:
        file1 = db.extra.find_one({"_id": user_id})
        df = pd.DataFrame(file1['sms'])
        a = cheque_user_outer(df, user_id)
        file1 = db.extra.find_one({"_id": user_id})
        df = pd.DataFrame(file1['sms'])
        a += cheque_user_outer(df, user_id)
    except Exception as e:
        logger.debug('error occured during checking bounced cheque messages')
        client.close()
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    logger.info('successfully checked bounced cheque messages')

    if a > 0:
        logger.info('user has bounced cheques exiting')
        a = {'_id': user_id, 'onhold': True, 'limit': -1, 'logic': 'BL0'}
        client.analysisresult.bl0.update_one({'_id': user_id}, {'$set': a}, upsert=True)
        return {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id, 'limit': -1,
                'logic': 'BL0'}

    client.close()

    if df_cibil.empty:
        logger.error('df_cibil is empty')
        return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                'limit': 0, 'logic': 'BL0'}

    logger.info('Stariting cibil analysis')
    if new_user:
        logger.info('new user checked')
        try:
            logger.info('Cibil analysis started')
            result = cibil_analysis(df_cibil, 749, user_id)
            if not result['status']:
                logger.debug('cibil analysis got some error')
                return result
            logger.info('Cibil analysis successful')
            ans = result['ans']
            df_credit_score = int(df_cibil['credit_score'][0])
            if ans != 0:
                logger.info('returning result 3k')
                a = {'_id': user_id, 'onhold': False, 'limit': 3000}
                client.analysisresult.bl0.update_one({'_id': user_id}, {'$set': a}, upsert=True)
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                        'limit': 3000, 'logic': 'BL0'}

            elif df_credit_score > 750:
                logger.info('returning result 2k')
                a = {'_id': user_id, 'onhold': False, 'limit': 2000}
                client.analysisresult.bl0.update_one({'_id': user_id}, {'$set': a}, upsert=True)
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                        'limit': 2000, 'logic': 'BL0'}
            else:
                logger.info('returning result 0')
                a = {'_id': user_id, 'onhold': False, 'limit': 0}
                client.analysisresult.bl0.update_one({'_id': user_id}, {'$set': a}, upsert=True)
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                        'limit': 0, 'logic': 'BL0'}

        except Exception as e:
            logger.debug('Exception in cibil analysis')
            return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                    'logic': 'BL0'}

    else:
        logger.info('existing user checked')
        try:
            logger.info('Cibil analysis started')
            result = cibil_analysis(df_cibil, 649, user_id)
            if not result['status']:
                logger.debug('cibil analysis got some error')
                return result
            logger.info('Cibil analysis successful')

            ans = result['ans']
            if current_loan > ans:
                logger.info('returning result' + str(current_loan))
                a = {'_id': user_id, 'onhold': False, 'limit': current_loan, 'timestamp': str(datetime.now(tz=None))}
                client.analysisresult.bl1.update_one({'_id': user_id}, {'$set': a}, upsert=True)
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                        'limit': current_loan, 'logic': 'BL1'}
            else:
                logger.info('returning result' + str(ans))
                a = {'_id': user_id, 'onhold': False, 'limit': ans, 'timestamp': str(datetime.now(tz=None))}
                client.analysisresult.bl1.update_one({'_id': user_id}, {'$set': a}, upsert=True)
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                        'limit': ans, 'logic': 'BL1'}

        except Exception as e:
            logger.debug('Exception in cibil analysis')
            return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                    'logic': 'BL1'}
