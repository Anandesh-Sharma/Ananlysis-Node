from .Util import conn, logger_1
import pandas as pd
from .Cheque_Bounce import cheque_user_outer
from .cibil_analysis import cibil_analysis
from datetime import datetime


def analyse(user_id, df_cibil, new_user, current_loan):
    logger = logger_1("Analysis", user_id)
    try:
        logger.info('making connection with db')
        client = conn()
    except Exception as e:
        logger.critical('error in connection')
        return {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    logger.info('connection success')
    logger.info('Starting checking bounced cheque messages')

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
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    logger.info('successfully checked bounced cheque messages')
    if a > 0:
        logger.info('user has bounced cheques exiting')
        a = {'_id': user_id, 'onhold': True, 'limit': -1, 'logic': 'BL0'}
        client.analysisresult.bl0.update({'_id': user_id}, a, upsert=True)
        r = {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id, 'limit': -1,
             'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    if df_cibil.empty:
        logger.error('df_cibil is empty')
        r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
             'limit': 0, 'logic': 'BL0'}
        a = {"processing": False, "result": r}
        client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
        client.close()
        return r

    logger.info('Stariting cibil analysis')
    if new_user:
        logger.info('new user checked')
        try:
            logger.info('Cibil analysis started')
            result = cibil_analysis(df_cibil, 749, user_id)
            if not result['status']:
                logger.debug('cibil analysis got some error')
                r = result
                a = {"processing": False, "result": r}
                client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
                client.close()
                return r

            logger.info('Cibil analysis successful')
            ans = result['ans']
            df_credit_score = int(df_cibil['credit_score'][0])
            if ans != 0:
                logger.info('returning result 3k')
                a = {'_id': user_id, 'onhold': False, 'limit': 3000}
                client.analysisresult.bl0.update({'_id': user_id}, a, upsert=True)
                r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': 3000, 'logic': 'BL0'}
                a = {"processing": False, "result": r}
                client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
                client.close()
                return r

            elif df_credit_score > 750:
                logger.info('returning result 2k')
                a = {'_id': user_id, 'onhold': False, 'limit': 2000}
                client.analysisresult.bl0.update({'_id': user_id}, a, upsert=True)
                r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': 2000, 'logic': 'BL0'}
                a = {"processing": False, "result": r}
                client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
                client.close()
                return r

            else:
                logger.info('returning result 0')
                a = {'_id': user_id, 'onhold': False, 'limit': 0}
                client.analysisresult.bl0.update({'_id': user_id}, a, upsert=True)
                r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': 0, 'logic': 'BL0'}
                a = {"processing": False, "result": r}
                client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
                client.close()
                return r

        except Exception as e:
            logger.debug('Exception in cibil analysis')
            r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                 'logic': 'BL0'}
            a = {"processing": False, "result": r}
            client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
            client.close()
            return r

    else:
        logger.info('existing user checked')
        try:
            logger.info('Cibil analysis started')
            result = cibil_analysis(df_cibil, 649, user_id)
            if not result['status']:
                logger.debug('cibil analysis got some error')
                r = result
                a = {"processing": False, "result": r}
                client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
                client.close()
                return r

            logger.info('Cibil analysis successful')

            ans = result['ans']
            if current_loan > ans:
                logger.info('returning result' + str(current_loan))
                a = {'_id': user_id, 'onhold': False, 'limit': current_loan, 'timestamp': str(datetime.now(tz=None))}
                client.analysisresult.bl1.update({'_id': user_id}, a, upsert=True)
                r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': current_loan, 'logic': 'BL1'}
                a = {"processing": False, "result": r}
                client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
                client.close()
                return r

            else:
                logger.info('returning result' + str(ans))
                a = {'_id': user_id, 'onhold': False, 'limit': ans, 'timestamp': str(datetime.now(tz=None))}
                client.analysisresult.bl1.update({'_id': user_id}, a, upsert=True)
                r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': ans, 'logic': 'BL1'}
                a = {"processing": False, "result": r}
                client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
                client.close()
                return r

        except Exception as e:
            logger.debug('Exception in cibil analysis')
            r = {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                 'logic': 'BL1'}
            a = {"processing": False, "result": r}
            client.analysisresult.result.update({"_id": user_id}, a, upsert=True)
            client.close()
            return r
