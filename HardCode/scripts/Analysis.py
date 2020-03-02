from .Util import conn, logger_1


def analyse(**kwargs):
    user_id = kwargs.get('user_id')
    cibil_score = kwargs.get('cibil_score')
    new_user = kwargs.get('new_user')
    current_loan = kwargs.get('current_loan')
    cibil_df = kwargs.get('cibil_df')

    logger = logger_1("Analysis", user_id)
    logger.info('Stariting cibil analysis')
    logger.info('user cibil check')
    if cibil_df:
        r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
             'limit': -1, 'logic': 'BL0'}
    else:
        if int(cibil_score) >= 750:
            logger.info('returning result 3k')
            a = 3000

        else:
            logger.info('cibil score is less than 750')
            if new_user:
                a = -1
            else:
                a = current_loan

        r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
             'limit': a, 'logic': 'BL0'}
    return r
