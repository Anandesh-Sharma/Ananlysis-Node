import numpy as np
from .Util import logger_1


def cibil_analysis(df, cibil_score, user_id):
    """ 
    Based on BL0 analyse the cibil report

    Parameters: 
    df (Data Frame)   : Containing fields of individual users with column names
        account_type          : type of account
        payment_history       : payment histroy of individual loan of user
        credit_score          : credit score of the user
        written_amt_total     : written amount total of specific loan
        written_amt_principal : written principle total of specific loan
        payment_rating(int)   : payment rating of a person
    cibil_score(int)          : minimum cibil score for calculating loan amount
    user_id(int)              :user's specific id

    Returns: 
    bool    : whether we can pass loan"""

    logger = logger_1('cibil analysis', user_id)
    logger.info('cibil analysis function starts')

    history = 'N'
    score = 0
    payment_r = '0'
    amt_principal = 0
    amt_total = 0
    loan = [1, 2, 3, 8, 15]
    N_loans = 0
    unneeded_history = ['S', 'D', 'B', '6', '5', '4', '3', 'L', 'M']
    try:
        logger.info('cibil analysis loop starts')
        for i, row in df.iterrows():
            if row['account_type'] in loan:
                N_loans += 1

            s2, s1, s0, sn, ss, sd, sb, s3, s4, s5, s6, sl, sm = False, False, False, False, False, False, False, False, False, False, False, False, False
            if history not in unneeded_history:
                for i in str(row['payment_history']):
                    if i == 'S':
                        ss = True
                        break
                    elif i == 'B':
                        sb = True
                        break
                    elif i == 'D':
                        sd = True
                        break
                    elif i == 'L':
                        sl = True
                        break
                    elif i == 'M':
                        sm = True
                        break
                    elif i == '6':
                        s6 = True
                        break
                    elif i == '5':
                        s5 = True
                        break
                    elif i == '4':
                        s4 = True
                        break
                    elif i == '3':
                        s3 = True
                        break
                    elif i == '2':
                        s2 = True
                    elif i == '1':
                        s1 = True
                    elif i == '0':
                        s0 = True
                    elif i == 'N':
                        sn = True
                if ss:
                    history = 'S'
                elif sd:
                    history = 'D'
                elif sb:
                    history = 'B'
                elif s6:
                    history = '6'
                elif sl:
                    history = 'L'
                elif sm:
                    history = 'M'
                elif s5:
                    history = '5'
                elif s4:
                    history = '4'
                elif s3:
                    history = '3'
                elif s2:
                    history = '2'
                elif s1:
                    history = '1'
                elif s0:
                    history = '0'
                elif sn:
                    history = 'N'

            score = row['credit_score']
            if row['written_amt_total'] != 0 and type(row['written_amt_total']) == int:
                amt_total = row['written_amt_total']
            if row['written_amt_principal'] != 0 and type(row['written_amt_principal']) == int:
                amt_principal = row['written_amt_principal']
            if row['payment_rating'] != '0' and row['payment_rating'] != '1' and row['payment_rating'] != '2':
                payment_r = str(row['payment_rating'])

        selected = ['N', '0', '1', '2']
        ans = False
        logger.info('cibil analysis loop successfully ends')

        if int(score) > cibil_score:
            if N_loans > 0:
                if history in selected:
                    if amt_total == 0 or np.isnan(amt_total):
                        if amt_principal == 0 or np.isnan(amt_principal):
                            if payment_r in selected:
                                ans = True
        logger.info('cibil analysis function successfull')
        if ans:
            return {'status': True, 'message': 'success', 'ans': 3000}
        else:
            return {'status': True, 'message': 'success', 'ans': 0}

    except Exception as e:
        logger.debug('cibil analysis function failed')
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
