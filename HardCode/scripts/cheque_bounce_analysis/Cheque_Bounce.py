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

    patterns = [
    r'bounced',
    r'bounce ho chuka hai',
    r'has got bounce',
    r'overdue for bounce',
    r'cheque bouncing charges',
    r'unable to process your ecs request',
    r'dishonou?r charges of',
    r'dishonou?red',
    r'has been returned due to reason - insufficient fund',
    r'auto-debit attempt failed',
    r'cheque bounces',
    r'cheque return charges is still unpaid',
    r'returned unpaid']
    pattern_not_1 = r'please ensure.*sufficient balance',
    pattern_not_2 = r'if.*done payment',
    bounce = []
    msg = []
    for row in data:
        message = str(row['body']).lower()
        for pattern in patterns:
            matcher = re.search(pattern,message)
            if matcher:
                matcher_not_1 = re.search(pattern_not_1, message)
                matcher_not_2 = re.search(pattern_not_2, message)
                if not (matcher_not_1 or matcher_not_2):
                    bounce.append((datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S').month, row['sender'][3:]))
                    msg.append(row['body'])
                    break
                break
    logger.info('cheque user inner successfully executed')
    return bounce, msg


def cheque_user_outer(user_id):
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

    try:
        logger.info('making connection with db')
        client = conn()
    except BaseException as e:
        msg = 'error in connection - '+str(e)
        logger.critical(msg)
        return {"status":False,"message":msg}
    logger.info('connection success')

    file1 = client.messagecluster.extra.find_one({"cust_id": user_id})
    if not file1:
        logger.error("Extra File not found")
        return {"status":True,"message":"success","a":0}
    data = file1['sms']
    l = {}
    bounce, msg = cheque_user_inner(data, user_id)
    for i in bounce:
        if i[0] in l.keys():
            l[i[0]].add(i[1])
        else:
            l[i[0]] = {i[1]}
    count = 0
    for i in l.keys():
        count += len(l[i])
    logger.info('cheque user outer successfully executed')
    return {"status":True,"message":"success","count":count, "msg":msg}
