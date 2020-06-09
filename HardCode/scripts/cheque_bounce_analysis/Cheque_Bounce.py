# import pandas as pd
from HardCode.scripts.Util import logger_1,conn
import regex as re
from datetime import datetime


def cheque_user(user_id):
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

    file1 = client.messagecluster.cheque_bounce_msgs.find_one({"cust_id": user_id})
    if not file1:
        logger.error("Cheque Bounce File not found")
        return {"status":True,"message":"success","a":0}
    data = file1['sms']
    l = {}
    bounce = []
    for i in data:
        bounce.append((datetime.strptime(i['timestamp'], '%Y-%m-%d %H:%M:%S').month, i['sender'][3:]))
    for i in bounce:
        if i[0] in l.keys():
            l[i[0]].add(i[1])
        else:
            l[i[0]] = {i[1]}
    count = 0
    for i in l.keys():
        count += len(l[i])
    logger.info('cheque user outer successfully executed')
    return {"status":True,"message":"success","count":count}
