import pandas as pd
import regex as re
from datetime import datetime
from .Util import logger_1


def cheque_user_inner(data, user_id):
    '''
    Checks Bounced messages

    It gives a monthly status that in a specific month how many individual
    service's cheque has been bounced.
    
    Parameters:
    df (Data Frame) : Containing fields of individual users with column names
        body        : containing the whole sms
        SMS_HEADER  : containing the sender's name
        STATUS      : status whether the message is read or not
        TIMESTAMP   : timestamp of the message received

    Returns:
    tuple:containing two parameters
        int:    month number of the message received
        set:    the service whose cheque is bounced'''

    logger = logger_1('cheque user inner', user_id)
    logger.info('cheque user inner function starts')

    pattern_1 = 'bounced'
    pattern_2 = 'bounce ho chuka hai'
    pattern_3 = 'has got bounce'
    pattern_4 = 'overdue for bounce'
    pattern_5 = 'cheque bouncing charges'
    pattern_6 = 'unable to process your ecs request'
    pattern_7 = 'dishonou?r charges of'
    pattern_8 = 'dishonou?red'
    pattern_9 = 'has been returned due to reason - insufficient fund'
    pattern_10 = 'auto-debit attempt failed'
    pattern_11 = 'cheque bounces'
    pattern_12 = 'cheque return charges is still unpaid'
    pattern_13 = 'returned unpaid'
    bounce = []
    for i, row in data.iterrows():
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13,message)

        if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None or matcher_9 is not None or matcher_10 is not None or matcher_11 is not None or matcher_12 is not None or matcher_13 is None:
            bounce.append((datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S').month, row['sender'][3:]))
    logger.info('cheque user inner successfully executed')
    return bounce


def cheque_user_outer(df, user_id):
    '''
    Checks Bounced Messages

    Gives the number of unique service cheque bounce adding every month's
    number of cheque bounce.
    
    Parameters:
    df (Data Frame) : Containing fields of individual users with column names
        body        : containing the whole sms
        SMS_HEADER  : containing the sender's name
        STATUS      : status whether the message is read or not
        TIMESTAMP   : timestamp of the message received

    Returns:
    int : count of total unique service messages per month'''
    logger = logger_1('cheque user outer', user_id)
    logger.info('cheque user outer function starts')
    l = {}
    for i in cheque_user_inner(df, user_id):
        if i[0] in l.keys():
            l[i[0]].add(i[1])
        else:
            l[i[0]] = {i[1]}
    count = 0
    for i in l.keys():
        count += len(l[i])
    logger.info('cheque user outer successfully executed')
    return count
