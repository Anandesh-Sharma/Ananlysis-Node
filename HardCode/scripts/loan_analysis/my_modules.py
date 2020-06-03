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


def remove_numerical_header(data):
    li = []
    try:
        for i in range(data.shape[0]):
            try:
                if isinstance(int(data['Sender-Name'][i]), int):
                    li.append(i)
            except:
                pass
        data.drop(li, axis=0, inplace=True)
        data.reset_index(drop=True, inplace=True)
    except BaseException as e:
        print(e)
    return data


def sms_header_splitter(data):
    """
    This function splits the sms header of each message of the user.

    Parameters:
        data(dataframe): dataframe of the user

    Returns:
        data(dataframe): dataframe containing sms headers splitted

    """
    li = []
    pd.options.mode.chained_assignment = None
    data['Sender-Name'] = np.nan
    try:
        for i in range(len(data)):
            data['sender'][i] = data['sender'][i].replace('-', '')
            data['sender'][i] = data['sender'][i].replace('$', '')
            try:
                header = str(data["sender"][i][2:]).upper()
                header = sms_header_matcher(header)
            except:
                header = data["sender"][i][2:]
            data['Sender-Name'][i] = header
        data.drop(['sender'], axis=1, inplace=True)
        data = remove_numerical_header(data)
    except Exception as e:
        import traceback
        traceback.print_tb(e.__traceback__)
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


def extract_amount(message):
    amount = 0
    patterns = [
        r'total\srepayment\s?(?:of)?\s(?:rs\.?|inr)([0-9,]+[.]?[0-9]+)',
        r'(?:loan|payment[s]?)\s?(?:of)?\s([0-9,]+[.]?[0-9]+)',
        r'amount\srepayable\sis\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)',
        r'(?:inr\.?|rs\.?)\s?\s?([0-9,]+[.]?[0-9]+)',
        r'\s([0-9,]+[.]?[0-9]+){4-5}\s',
        r'\s([0-9]{4,5})\s',
        r'([0-9,]+[.]?[0-9]+)\s?(?:rupees|inr)',
        r'(?:amount|amt|repay)\s?(?:is)?\s([0-9,]+[.?][0-9]+)',
    ]
    not_pattern_1 = r'free\scoupon\sof\s[0-9,]+[.]?[0-9]+\s?(?:rupees|inr)'
    not_pattern_2 = r'[0-9,]+[.]?[0-9]+\s?(?:rupees|inr)\scoupon'

    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            not_matcher_1 = re.search(not_pattern_1, message)
            not_matcher_2 = re.search(not_pattern_2, message)
            if not (not_matcher_1 or not_matcher_2):
                amount = matcher.group(1)
                amount = amount.replace(',', '')
                break
    return float(amount)


def days_extract(message):
    days = -1
    patterns = [r'([0-9]+)\s?(?:din|day[s]?)']
    pattern_not_1 = r'get\sextension\sof\s[0-9]+\s?day[s]?'

    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            matcher_not_1 = re.search(pattern_not_1, message)
            if not matcher_not_1:
                days = matcher.group(1)
                days = int(days)

    return days


def date_extract(message):
    date = -1
    patterns = [
        r'([0-9]{1,2}\/[0-9]{1,2}\/(?:20|19|18))',
        r'([0-9]{2}\/?[-]?[0-9]{2}\/?[-]?20(?:20|19|18))',
        r'\s([0-9]{1,2}\/[0-9]{1,2})\.',
        r'\s([0-9]{1,2}\/[0-9]{1,2})\s',
        r'([0-9]{1,2}\-[0-9]{1,2}\-20(?:20|19|18))',
        r'\s([0-9]{1,2}(?:th|rd|st|nd)\s(?:jan|feb|mar|apr|may|jun|jul|aug|sep[t]?|oct|nov|dec))\s',
        r'([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{2,4})',
        r'(20(?:20|19|18)\-[0-9]{1,2}\-[0-9]{1,2})',
        r'([0-9]{1,2}(?:th|rd|st|nd)\s[a-z]+\s[0-9]{2,4})',
        r'((?:[0-9]{1,2})?\s?\-?(?:january|february|march|april|may|june|july|august|september|october|november|december)\s?\-?[0-9]{1,2})',
        r'([0-9]{1,2}\s(?:january|february|march|april|may|june|july|august|september|october|november|december))',
        r'([0-9]{1,2}[-]?[,]?\s?(?:jan|feb|mar|apr|may|jun|jul|aug|sep[t]?|oct|nov|dec)[-]?[,]?\s?20(?:20|19|18))',
        r'((?:jan|feb|mar|apr|may|jun|jul|aug|sep[t]?|oct|nov|dec)\s?[-]?[0-9]{1,2}\s?[-]?20(?:20|19|18))',
        r'([0-9]{1,2}[-]?[,]?\s?(?:jan|feb|mar|apr|may|jun|jul|aug|sep[t]?|oct|nov|dec)[-]?[,]?\s?(?:20|19|18))',
        r'([0-9]{1,2}(?:th|st|nd|rd)\s?(?:jan|feb|mar|apr|may|jun|jul|aug|sep[t]?|oct|nov|dec)\s?\'?(?:20|19|18))'
    ]

    for pattern in patterns:
        matcher = re.search(pattern, message)
        if matcher:
            date = matcher.group(1)

    return date


def fetch_info(df):
    df['amount'] = [0] * df.shape[0]
    df['loan_duration'] = [-1] * df.shape[0]
    df['overdue_date'] = [-1] * df.shape[0]
    df['overdue_days'] = [-1] * df.shape[0]
    df['due_date'] = [-1] * df.shape[0]
    df['expected_closing_date'] = [-1] * df.shape[0]
    df['due_days'] = [-1] * df.shape[0]
    # df['overdue_days'] = [-1] * df.shape[0]
    # df['penalty'] = [-1] * df.shape[0]
    # df['expected_closing_date'] = [-1] * df.shape[0]

    for i in range(df.shape[0]):
        message = str(df['body'][i]).lower()
        if df['category'][i] == 'disbursed':
            df['amount'][i] = extract_amount(message)
            df['loan_duration'][i] = days_extract(message)
        elif df['category'][i] == 'due':
            df['amount'][i] = extract_amount(message)
            df['due_date'][i] = date_extract(message)
            df['due days'][i] = days_extract(message)
        elif df['category'][i] == 'overdue':
            df['overdue_days'][i] = days_extract(message)
            df['amount'][i] = extract_amount(message)
            df['expected_closing_date'] = date_extract(message)
        else:
            df['amount'][i] = extract_amount(message)
    return df
