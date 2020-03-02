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

    # if cibil found for a specific customer then run the new cibil analysis
    if cibil_df:
        Account_Status = dict()
        Payment_Ratings = dict()
        review = False
        # account status
        for acc_status in cibil_df['account_status']:
            Account_Status[acc_status] = user_id
        # payment ratings
        for pay_rating in cibil_df['payment_rating']:
            Payment_Ratings[pay_rating] = user_id

        Blocked_Payment_Ratings = [3, 4, 5, 6]
        Blocked_Status = [93, 89, 97, 32, 33, 34, 35, 37, 38, 43, 44, 45, 46, 47, 49, 50, 53, 54, 55, 56, 57, 58, 59,
                          61,
                          62, 63, 64, 65, 66, 67, 68, 69, 70, 72, 73, 74, 75, 76, 77, 79, 81, 85, 86, 87, 88, 94, 90]

        for bpr in Blocked_Payment_Ratings:
            if bpr in Payment_Ratings:
                review = True
                break
        if not review:
            for bs in Blocked_Status:
                if bs in Account_Status:
                    review = True
                    break

        if not review:
            r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                 'limit': 2000, 'logic': 'BL0'}
        else:
            r = {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id,
                 'limit': -1, 'logic': 'BL0'}

    # else the base logic will run for the old customers having equifax score
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
