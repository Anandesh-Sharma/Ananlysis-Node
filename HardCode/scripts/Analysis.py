from .Util import conn, logger_1

def analyse(user_id, cibil_score, new_user, current_loan):
    logger = logger_1("Analysis", user_id)
        
    logger.info('Stariting cibil analysis')
    logger.info('user cibil check')
    
    if int(cibil_score) > 750:
        logger.info('returning result 2k')
        a =3000

    else:
        logger.info('returning result 0')
        a=0
    if current_loan>a:
        a=current_loan
    r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                 'limit': a, 'logic': 'BL0'}
    return r

def cibil_analysis(cibil_score,current_loan):
    if int(cibil_score)>749:
        a=3000
    
    else:
        a=0

    if a<current_loan:
        a=current_loan
    return r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                 'limit': a, 'logic': 'BL0'}