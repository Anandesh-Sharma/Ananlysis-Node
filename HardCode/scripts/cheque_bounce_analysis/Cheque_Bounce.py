# import pandas as pd
from HardCode.scripts.Util import logger_1
import regex as re
from datetime import datetime


# from ..Util import logger_1


def cheque_user_inner(data, user_id):
    """
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
        set:    the service whose cheque is bounced"""

    logger = logger_1('cheque user inner', user_id)
    logger.info('cheque user inner function starts')

    pattern_1 = r'bounced'
    pattern_2 = r'bounce ho chuka hai'
    pattern_3 = r'has got bounce'
    pattern_4 = r'overdue for bounce'
    pattern_5 = r'cheque bouncing charges'
    pattern_6 = r'unable to process your ecs request'
    pattern_7 = r'dishonou?r charges of'
    pattern_8 = r'dishonou?red'
    pattern_9 = r'has been returned due to reason - insufficient fund'
    pattern_10 = r'auto-debit attempt failed'
    pattern_11 = r'cheque bounces'
    pattern_12 = r'cheque return charges is still unpaid'
    pattern_13 = r'returned unpaid'
    pattern_not_1 = r'please ensure.*sufficient balance'
    pattern_not_2 = r'if.*done payment'
    bounce = []
    msg = []
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
        matcher_13 = re.search(pattern_13, message)

        if matcher_1 or matcher_2 or matcher_3 or matcher_4 or matcher_5 or matcher_6 or matcher_7 or matcher_8 or matcher_9 or matcher_10 or matcher_11 or matcher_12 or matcher_13:
            matcher_not_1 = re.search(pattern_not_1, message)
            matcher_not_2 = re.search(pattern_not_2, message)
            if not matcher_not_1 or not matcher_not_2:
                bounce.append((datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S').month, row['sender'][3:]))
                msg.append(row['body'])
    logger.info('cheque user inner successfully executed')
    return bounce, msg


def cheque_user_outer(df, user_id):
    """
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
    int : count of total unique service messages per month"""
    logger = logger_1('cheque user outer', user_id)
    logger.info('cheque user outer function starts')
    l = {}
    bounce, msg = cheque_user_inner(df, user_id)
    for i in bounce:
        if i[0] in l.keys():
            l[i[0]].add(i[1])
        else:
            l[i[0]] = {i[1]}
    count = 0
    for i in l.keys():
        count += len(l[i])
    logger.info('cheque user outer successfully executed')
    return count, msg
