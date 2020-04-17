from HardCode.scripts.Util import conn, logger_1
from pymongo import MongoClient
import pandas as pd
import numpy as np


def get_data(user_id):
    logger = logger_1('get_data', user_id)
    client = conn()
    db = client.messagecluster
    try:
        rejection_data = db.loanrejection.find_one({'cust_id': user_id})
        approval_data = db.loanapproval.find_one({'cust_id': user_id})

        if len(approval_data['sms']) != 0:
            approval_df = pd.DataFrame(approval_data['sms'])
            logger.info("Found loan approval data")
        else:
            approval_df = pd.DataFrame(None)
            logger.info("loan approval data not found")

        if len(rejection_data['sms']) != 0:
            rejection_df = pd.DataFrame(rejection_data['sms'])
            logger.info("Found loan rejection data")
        else:
            rejection_df = pd.DataFrame(None)
            logger.info("loan rejection data not found")

    except BaseException as e:
        print(f"Error in fetching data: {e} ")
        approval_df = pd.DataFrame(None)
        rejection_df = pd.DataFrame(None)

    finally:
        client.close()
        return approval_df, rejection_df


def sms_header_splitter(data):
    pd.options.mode.chained_assignment = None
    for i in range(len(data)):
        data['sender'][i] = data['sender'][i].replace('-', '')
        data["sender"][i] = data["sender"][i][2:]
        data['sender'][i] = data['sender'][i].lower()
    return data


def remove_unwanted_headers_and_sort(data, list_of_headers):
    for i in range(data.shape[0]):
        if data["sender"][i].lower() not in list_of_headers:
            data.drop(i, inplace=True)
    data.sort_values(by='timestamp', inplace=True)
    data.reset_index(drop=True, inplace=True)

    return data
