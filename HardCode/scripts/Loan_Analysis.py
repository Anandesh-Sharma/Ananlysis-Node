from pymongo import MongoClient
import numpy as np
import pandas as pd
import re
# import os
# from glob import glob
# from tqdm import tqdm
import datetime
import warnings
import json

warnings.filterwarnings('ignore')


def sms_header_splitter(data):
    """
    This function splits the sms header of each message of the user.

    Parameters:
        data(dataframe): dataframe of the user

    Returns:
        data(dataframe): dataframe containing sms headers splitted

    """
    pd.options.mode.chained_assignment = None
    data['Sender-Name'] = np.nan

    for i in range(len(data)):
        x = data['sender'][i].split('-')
        data['Sender-Name'][i] = x[-1].upper()
    data.drop(['sender'], axis=1, inplace=True)
    return data


def grouping(data):
    """
    This function groups the data by sender

    Parameters:
        data(dataframe): dataframe of user
    Returns:
        group_by_sender(dataframe): pandas groupby object
    """
    group_by_sender = data.groupby('Sender-Name')
    return group_by_sender


def is_approval(message):
    """
    This funtion checks if the message is of approval or not.

    Parameters:
        message(string) : message of user
    Returns:
        bool            : True if the message is of approval else False

    """
    # pattern_1 = '[^pre-]approved(.*)?'
    pattern_2 = 'succesfully(.*)?approved'
    # pattern_3 = '(.*)?has(.*)?been(.*)?approved'
    pattern_4 = '(.*)?application\sis\sapproved(.*)?'

    # matcher_1 = re.search(pattern_1,message)
    matcher_2 = re.search(pattern_2, message)
    # matcher_3 = re.search(pattern_3,message)
    # matcher_4 = re.search(pattern_4, message)

    if matcher_2 != None:
        return True
    else:
        return False


def is_disbursed(message):
    """
    This funtion checks if the message is of disbursal or not.

    Parameters:
        message(string) : message of user
    Returns:
        bool            : True if the message is of disbursal else False

    """
    pattern_1 = '(.*)?disbursed(.*)?'
    pattern_2 = '(.*)?disbursement(.*)?'
    pattern_3 = '(.*)?transferred(.*)?account(.*)?'
    pattern_4 = 'Money(.*)?transferred(.*)?account'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    matcher_4 = re.search(pattern_4, message)

    if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None:
        return True
    else:
        return False


def is_closed(message):
    """
    This funtion checks if the message is of closed or not.

    Parameters:
        message(string) : message of user
    Returns:
        bool            : True if the message is of closed else False

    """
    pattern_1 = '(.*)?loan(.*)?closed(.*)?'
    pattern_2 = '(.*)?closed(.*)?successfully(.*)?'
    pattern_3 = '(.*)?paid(.*)?successfully(.*)?'
    pattern_4 = '(.*)?paid\sback(.*)?relax(.*)?'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    matcher_4 = re.search(pattern_4, message)

    if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None:
        return True

    else:
        return False


def is_overdue(message):
    pass


def trans_amount_confirm(message):
    """
    This function checks if the message contains an amount or not. This function is called by trans_amount_extract.

    Parameters:
        message(string): message of the user
    Returns:
        bool           : True if amount is present else false
    """

    pattern1 = '(?:(?:[Rr][sS]|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)?(.*)?successfully credited(.*)?'
    pattern2 = '\spayment\s.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern3 = '\spayment\sof\s([0-9]+\.[0-9]+)'
    pattern4 = 'payment.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern5 = '.*(?:credited|debited)\s*(?:by|for)\s[Rr][Ss]\.\s?([0-9]+\.[0-9]+).*'
    pattern6 = '[Rr][Ss]\.?\s([0-9]+\.[0-9]+).*(?:credit[e]?[d]?|debit[e]?[d]?)\s(?:to|from).*'

    matcher_1 = re.search(pattern1, message)
    matcher_2 = re.search(pattern2, message)
    matcher_3 = re.search(pattern3, message)
    matcher_4 = re.search(pattern4, message)
    matcher_5 = re.search(pattern5, message)
    matcher_6 = re.search(pattern6, message)

    if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None or matcher_5 != None or matcher_6 != None:
        return True
    else:
        return False


def trans_amount_extract(message):
    """
    This function extracts amount from the message.

    Parameters:
        message(string):  message of the user
    Returns:
        amount(int)    : amount present in the message
    """

    pattern1 = '(?:(?:[Rr][sS]|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)?(.*)?successfully credited(.*)?'
    pattern2 = '\spayment\s.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern3 = '\spayment\sof\s([0-9]+\.[0-9]+)'
    pattern4 = 'payment.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern5 = '.*(?:credited|debited)\s*(?:by|for)\s[Rr][Ss]\.\s?([0-9]+\.[0-9]+).*'
    pattern6 = '[Rr][Ss]\.?\s([0-9]+\.[0-9]+).*(?:credit[e]?[d]?|debit[e]?[d]?)\s(?:to|from).*'

    matcher_1 = re.search(pattern1, message)
    matcher_2 = re.search(pattern2, message)
    matcher_3 = re.search(pattern3, message)
    matcher_4 = re.search(pattern4, message)
    matcher_5 = re.search(pattern5, message)
    matcher_6 = re.search(pattern6, message)

    if matcher_1 != None:
        amount = str(matcher_1.group(1))
    elif matcher_2 != None:
        amount = str(matcher_2.group(1))
    elif matcher_3 != None:
        amount = str(matcher_3.group(1))
    elif matcher_4 != None:
        if message[: 7] == 'payment':
            amount = str(matcher_4.group(1))
    elif matcher_5 != None:
        amount = str(matcher_5.group(1))
    elif matcher_6 != None:
        amount = str(matcher_6.group(1))
    else:
        amount = -1
    return amount


def amount_extract(temp_data, disbursed_date):
    INDEX = 0
    amount = 0
    for i in range(temp_data.shape[0]):
        iter_date = datetime.datetime.strptime(str(
            temp_data['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
        INDEX += 1
        if (iter_date > disbursed_date):
            start_date = iter_date
            break
    dates_within_5_mins = []
    for i in range(INDEX, temp_data.shape[0]):
        a = datetime.datetime.strptime(str(
            temp_data['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
        if (a - start_date).seconds / 60 < 5:
            dates_within_5_mins.append(i)
        else:
            break
    for i in dates_within_5_mins:
        message = str(temp_data['body'][i]).lower()
        if trans_amount_confirm(message):
            amount = trans_amount_extract(message)
            break
        else:
            amount = -1
    return amount


def get_report(temp_data, trans):
    """
    This function performs analysis of loans taken by the user from Cashbin and Kreditb.

    Parameters:
        temp_data(dataframe)    : dataframe of the user
        trans(datafrmae)        : dataframe containing only transactional messages of the user

    Returns: Dictionary(cust_id, approved/disbursed,closed, overdue, amount, error, status)
    cust_id(int)            : id of the user
    approved/disbursed(int) : count of loans approved/disbursed
    closed(int)             : count of loans closed
    overdue(int)            : count of overdues
    amount(list)            : amount of each loan identified, returns -1 if amount is not found
    error(string)           : displays an error message
    status(int)             : -1 if error in loading data, -2 if data of desired apps is not found, 1 if the code is executed successfully

    """
    report = {
        'approved/disbursed': 0,
        'closed': 0,
        'overdue': 0,
        'days': [],
        'amount': []
    }
    i = 0

    temp_data['Status'] = [0] * temp_data.shape[0]
    while (i < len(temp_data)):
        j = i + 1
        message = str(temp_data['body'][i]).lower()
        if is_disbursed(message):
            report['approved/disbursed'] += 1

            disbursed_date = datetime.datetime.strptime(str(
                temp_data['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
            report['amount'].append(amount_extract(trans, disbursed_date))
            temp_data['Status'][i] = 'approved/disbursed'
            while (j < len(temp_data)):
                message_new = str(temp_data['body'][j]).lower()
                if is_disbursed(message_new):
                    report['approved/disbursed'] += 1
                    new_disbursed_date = datetime.datetime.strptime(str(
                        temp_data['timestamp'][j]), '%Y-%m-%d %H:%M:%S')
                    report['amount'].append(
                        amount_extract(trans, new_disbursed_date))
                    days = (new_disbursed_date - disbursed_date).days
                    if days < 35:
                        report['closed'] += 1
                    else:
                        temp_data['defaulter?'][j] = 1
                        i = j + 1
                        break
                elif is_closed(message_new):
                    closed_date = datetime.datetime.strptime(str(
                        temp_data['timestamp'][j]), '%Y-%m-%d %H:%M:%S')
                    temp_data['Status'][j] = 'closed'
                    days = (closed_date - disbursed_date).days
                    if days < 35:
                        report['closed'] += 1
                        report['days'].append(days)
                        break
                    else:
                        report['closed'] += 1
                        report['overdue'] += 1
                        temp_data['defaulter?'][j] = 1
                        i = j + 1
                        break
                else:
                    j += 1
        elif is_closed(message):
            report['approved/disbursed'] += 1
            report['closed'] += 1
            closed_date = datetime.datetime.strptime(str(
                temp_data['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
            temp_data['Status'][i] = 'closed'
            while (j < len(temp_data)):
                message_new = str(temp_data['body'][j]).lower()
                if is_closed(message_new):
                    report['approved/disbursed'] += 1
                    report['closed'] += 1
                    temp_data['Status'][j] = 'closed'
                    i = j + 1
                    break
                else:
                    j += 1
        i += 1
    return report


def get_customer_data(cust_id, script_Status):
    """
    This function establishes a connection from the mongo database and fetches data of the user.

    Parameters:
        cust_id(int)                : id of the user
        script_status(dictionary)   : a dictionary for reporting errors occured at various stages
    Returns:
        loan_data(dataframe)        : dataframe containing messages of loan disbursal and loan closed
        trans_data(dataframe)       : dataframe containing only transactional messgaes of the user
        script_status(dictionary)   : a dictionary for reporting errors occured at various stages
    """
    try:

        client = MongoClient(
            "mongodb://god:rock0004@localhost:27017/?authSource=admin&StatusPreference=primary&ssl=false")
        # connect to database
        db = client.messagecluster

        # connect to collection
        approval_data = db.loanapproval
        closed_data = db.loanclosed
        trans_data = db.transaction
        disbursed_data = db.disbursed

        closed = closed_data.find_one({"_id": cust_id})
        trans = trans_data.find_one({"_id": cust_id})
        disbursed = disbursed_data.find_one({"_id": cust_id})

        if closed != None:
            closed_df = pd.DataFrame(closed['sms'])
        else:
            raise Exception

        if trans != None:
            transaction_df = pd.DataFrame(trans['sms'])
        else:
            raise Exception

        if disbursed != None:
            disbursed_df = pd.DataFrame(disbursed['sms'])
        else:
            raise Exception

        loan_data = pd.concat([disbursed_df, closed_df], axis=0)

        loan_data.sort_values(by=["timestamp"])
        transaction_df.sort_values(by=["timestamp"])

        loan_data = loan_data.reset_index(drop=True)
        transaction_df = transaction_df.reset_index(drop=True)
        client.close()
        script_Status['data_fetch'] = 1
        return loan_data, transaction_df, script_Status

    except Exception as e:
        script_Status['data_fetch'] = -1
        client.close()
        return None, None, script_Status


def process_customer(cust_id):
    """
    This function calls the above functions and generates a result.
    Parameters:
        cust_id(int)    : id of the user
    Returns:
        result(dictionary) : loan analysis for the user

    """
    script_Status = {'grouping_cashbin': 0,
                     'grouping_kreditb': 0}
    result = {}
    # result['status'] = 0
    # result['error'] = ''
    # result['_id'] = cust_id

    loan_data, transaction_df, script_Status = get_customer_data(
        cust_id, script_Status)

    if script_Status['data_fetch'] != -1:

        data = sms_header_splitter(loan_data)
        data_grouped = grouping(data)

        try:
            cashbin = data_grouped.get_group('CASHBN').sort_values(
                by='timestamp').reset_index(drop=True)
        except:
            script_Status['grouping_cashbin'] = -1

        try:
            kreditb = data_grouped.get_group('KREDTB').sort_values(
                by='timestamp').reset_index(drop=True)
        except:
            script_Status['grouping_kreditb'] = -1
    else:
        result['status'] = -1
        result['error'] = 'error in fetching data'
        return result

    if (script_Status['grouping_cashbin'] == -1) and (script_Status['grouping_kreditb'] == -1):
        result['status'] = -2
        result['error'] = 'no data of cashbin and kreditb'
        return result

    else:

        if script_Status['grouping_cashbin'] != -1:
            report_cashbin = get_report(cashbin, transaction_df)
            result['CASHBN'] = report_cashbin

        if script_Status['grouping_kreditb'] != -1:
            report_kreditb = get_report(kreditb, transaction_df)
            result['KREDTB'] = report_kreditb
        result['status'] = 1
        return result


def loan_analysis(cust_id):
    """
    The script returns a loan anlaysis for two loan apps CASHBIN and KREDITB.

    Parameters:
        cust_id(int): id of the user

    Returns: Dictionary(cust_id, approved/disbursed,closed, overdue, amount, error, status)

    cust_id(int)            : id of the user
    approved/disbursed(int) : count of loans approved/disbursed
    closed(int)             : count of loans closed
    overdue(int)            : count of overdues
    amount(list)            : amount of each loan identified, returns -1 if amount is not found
    error(string)           : displays an error message
    status(int)             : -1 if error in loading data, -2 if data of desired apps is not found, 1 if the code is executed successfully

    """
    result = process_customer(cust_id)
    res = json.dumps(result)
    res = json.loads(res)
    key = {'_id': cust_id}
    client = MongoClient(
        "mongodb://god:rock0004@localhost:27017/?authSource=admin&StatusPreference=primary&ssl=false")
    db = client.messagecluster
    db.loanapps.update(key, res, upsert=True)
    client.close()
