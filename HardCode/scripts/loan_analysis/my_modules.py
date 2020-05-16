import numpy as np
import pandas as pd
import re
from datetime import datetime
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex
from HardCode.scripts.loan_analysis.head_matcher import head_matcher


def sms_header_matcher(header):
    for i in list(head_matcher.keys()):
        try:
            if header in head_matcher[i]:
                header = i
                break
        except:
            pass
    return header

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
        data['sender'][i] = data['sender'][i].replace('-', '')
        try:
            header = str(data["sender"][i][2:]).upper()
            header = sms_header_matcher(header)
        except:
            header = data["sender"][i][2:]
        data['Sender-Name'][i] = header
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

def is_disbursed(message, app):
    """
    This funtion checks if the message is of disbursal or not.

    Parameters:
        message(string) : message of user
    Returns:
        bool            : True if the message is of disbursal else False

    """
    patterns = loan_apps_regex[app]['disbursal']


    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            return True
    else:
        return False




def disbursed_amount_extract(message, app):
    amount = -1
    patterns = loan_apps_regex[app]['disbursal']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            try:
                amount = float(matcher.group(1))
            except:
                amount = -1
    return float(amount)


def is_closed(message, app):
    """
    This funtion checks if the message is of closed or not.

    Parameters:
        message(string) : message of user
    Returns:
        bool            : True if the message is of closed else False

    """
    
    patterns = loan_apps_regex[app]['closed']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            return True
    else:
        return False 


def closed_amount_extract(message, app):
    amount = -1
    patterns = loan_apps_regex[app]['closed']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            try:
                amount = float(matcher.group(1))
            except:
                amount = -1
    return float(amount)


def is_due(message, app):
    patterns = loan_apps_regex[app]['due']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            return True
    else:
        return False 


def due_date_extract(message):
    date = -1
    pattern_1 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*repayment.*\s([0-9]+)'  # group(1) for date and group(2) for amount
    pattern_2 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'  # group(1) for date and group(2) for amount
    pattern_3 = r'.*rs\.?\s([0-9]+).*due\sby\s([0-9]+-[0-9]+-[0-9]+).*'  # group(1) for amount and group(2) for date
    # pattern_4 = r'due\s(?:on)?.*([0-9]+/[0-9]+).*'  # group(1) for date in cashbn
    pattern_4 = r'.*due\s(?:on)?\s?([0-9]+/[0-9]+).*'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    matcher_4 = re.search(pattern_4, message)

    if matcher_1:
        date = str(matcher_1.group(1))
    elif matcher_2:
        date = str(matcher_2.group(1))
    elif matcher_3:
        date = str(matcher_3.group(2))
    elif matcher_4:
        date = str(matcher_4.group(1))
    else:
        date = -1
    return date


def due_amount_extract(message, app):
    amount = -1
    patterns = loan_apps_regex[app]['due']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            try:
                amount = float(matcher.group(1))
            except:
                amount = -1
    return float(amount)


def is_overdue(message, app):

    patterns = loan_apps_regex[app]['overdue']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            return True
    else:
        return False

def overdue_days_extract(message, app):
    patterns = loan_apps_regex[app]['overdue']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            try:
                days = int(matcher.group(1))
            except:
                days = -1
    return days

def extract_amount_from_overdue_message(message, app):
    amount = -1
    patterns = loan_apps_regex[app]['overdue']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            try:
                amount = float(matcher.group(1))
            except:
                amount = -1
    else:
        return float(amount)


def overdue_amount_extract(data, overdue_first_date, app):
    INDEX = 0
    amount = -1
    for i in range(data.shape[0]):
        iter_date = datetime.strptime(str(data['timestamp'][i]), '%Y-%m-%d %H:%M:%S')

        if (iter_date >= overdue_first_date):
            break
        INDEX += 1
    overdue_amount_list = [-1]
    for i in range(INDEX, data.shape[0]):
        message = str(data['body'][i]).lower()
        if is_overdue(message, app):
            amount = extract_amount_from_overdue_message(message, app)
            overdue_amount_list.append(amount)
        else:
            break
    return max(overdue_amount_list)


def is_rejected(message, app):
    patterns = loan_apps_regex[app]['rejection']
    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            return True
    else:
        return False