from .Util import conn, logger_1

def analyse(user_id, df_cibil, new_user, current_loan):
    logger = logger_1("Analysis", user_id)
    
    if df_cibil.empty:
        logger.error('df_cibil is empty')
        r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
             'limit': 0, 'logic': 'BL0'}
        return r

    logger.info('Stariting cibil analysis')
    logger.info('user cibil check')
    
    if df_cibil['credit_score'] > 750:
        logger.info('returning result 2k')
        r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                'limit': 2000, 'logic': 'BL0'}
        return r

    else:
        logger.info('returning result 0')
        r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                'limit': 0, 'logic': 'BL0'}
        return r

    