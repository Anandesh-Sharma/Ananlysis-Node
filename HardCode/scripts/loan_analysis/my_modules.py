import numpy as np
import pandas as pd
import re
from datetime import datetime


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
        data["sender"][i] = data["sender"][i][2:]
        data['Sender-Name'][i] = data['sender'][i]
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
    # pattern_1 = r'[^pre-]approved(.*)?'
    pattern_2 = r'succesfully(.*)?approved'
    # pattern_3 = r'(.*)?has(.*)?been(.*)?approved'
    pattern_4 = r'(.*)?application\sis\sapproved(.*)?'

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
    pattern_1 = r'(.*)?disbursed(.*)?'
    pattern_2 = r'(.*)?disbursement(.*)?'
    pattern_3 = r'(.*)?transferred(.*)?account(.*)?'
    pattern_4 = r'Money(.*)?transferred(.*)?account'
    pattern_5 = r'.*loan.*approved.*rs\.?\s([0-9]+).*'
    pattern_6 = r'.*loan.*disbursed.*amounting.*\s([0-9]+)\srupees.*'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    matcher_4 = re.search(pattern_4, message)
    matcher_5 = re.search(pattern_5, message)
    matcher_6 = re.search(pattern_6, message)

    if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None or matcher_5 != None or matcher_6 != None:
        return True
    else:
        return False


def disbursed_amount_extract(message):
    amount = -1
    pattern_1 = r'.*loan.*approved.*rs\.?\s([0-9]+).*'
    pattern_2 = r'.*loan.*disbursed.*amounting.*\s([0-9]+)\srupees.*'
    pattern_3 = r'.*disbursed.*(\brs\b).*?\.?\s([0-9]+).*'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)

    if matcher_1 != None:
        amount = int(matcher_1.group(1))
    elif matcher_2 != None:
        amount = int(matcher_2.group(1))
    elif matcher_3 != None:
        amount = int(matcher_3.group(2))
    else:
        amount = -1

    return amount


def is_closed(message):
    """
    This funtion checks if the message is of closed or not.

    Parameters:
        message(string) : message of user
    Returns:
        bool            : True if the message is of closed else False

    """
    pattern_1 = r'(.*)?loan(.*)?closed(.*)?'
    pattern_2 = r'(.*)?closed(.*)?successfully(.*)?'
    pattern_3 = r'(.*)?paid(.*)?successfully(.*)?'
    pattern_4 = r'(.*)?paid\sback(.*)?relax(.*)?'
    pattern_5 = r'payment.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_6 = r'.*successfully\sreceived\spayment.*rs\.\s([0-9]{2,5}).*'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    matcher_4 = re.search(pattern_4, message)
    matcher_5 = re.search(pattern_5, message)
    matcher_6 = re.search(pattern_6, message)

    if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None or matcher_5 != None or matcher_6 != None:
        return True

    else:
        return False


def closed_amount_extract(message):
    amount = -1
    pattern1 = r'\spayment\s.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern2 = r'\spayment\sof\s([0-9]+\.[0-9]+)'
    pattern3 = r'payment.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern4 = r'.*successfully\sreceived\spayment.*rs\.\s([0-9]{2,5}).*'

    matcher1 = re.search(pattern1, message)
    matcher2 = re.search(pattern2, message)
    matcher3 = re.search(pattern3, message)
    matcher4 = re.search(pattern4, message)

    if matcher1 != None:
        amount = str(matcher1.group(1))
    elif matcher2 != None:
        amount = str(matcher2.group(1))
    elif matcher3 != None:
        amount = str(matcher3.group(1))
    elif matcher4 != None:
        amount = str(matcher4.group(1))
    else:
        amount = -1
    return amount


def is_due(message):
    pattern_1 = r'.*payment.*rs\.?.*?([0-9]+).*due.*'  # group(1) for amount
    pattern_2 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'  # group(1) for date and group(2) for amount
    pattern_3 = r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*'  # group(1) for amount and group(2) for date
    pattern_4 = r'due\s(?:on)?.*([0-9]+/[0-9]+).*'  # group(1) for date in cashbn
    pattern_5 = r'.*loan.*rs\.?.*?([0-9]+).*due.*'  # group(1) for loan amount
    pattern_6 = r'.*payment.*due.*'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    matcher_4 = re.search(pattern_4, message)
    matcher_5 = re.search(pattern_5, message)
    matcher_6 = re.search(pattern_6, message)

    if matcher_1 != None or matcher_2 != None or matcher_3 != None or matcher_4 != None or matcher_5 != None or matcher_6 != None:
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

    if matcher_1 != None:
        date = str(matcher_1.group(1))
    elif matcher_2 != None:
        date = str(matcher_2.group(1))
    elif matcher_3 != None:
        date = str(matcher_3.group(2))
    elif matcher_4 != None:
        date = str(matcher_4.group(1))
    else:
        date = -1
    return date


def due_amount_extract(message):
    amount = -1
    pattern_1 = r'.*payment.*rs\.?.*?([0-9]+).*due.*'  # group(1) for amount
    pattern_2 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'  # group(1) for date and group(2) for amount
    pattern_3 = r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*'  # group(1) for amount and group(2) for date
    pattern_4 = r'.*loan.*rs\.?.*?([0-9]+).*due.*'  # group(1) for loan amount
    pattern_5 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*repayment.*\s([0-9]+)'  # group(1) for date and group(2) for amount

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    matcher_4 = re.search(pattern_4, message)
    matcher_5 = re.search(pattern_5, message)

    if matcher_1 != None:
        amount = str(matcher_1.group(1))
    elif matcher_2 != None:
        amount = str(matcher_2.group(2))
    elif matcher_3 != None:
        amount = str(matcher_3.group(1))
    elif matcher_4 != None:
        amount = str(matcher_4.group(1))
    elif matcher_5 != None:
        amount = str(matcher_5.group(2))
    else:
        amount = -1
    return amount


def is_overdue(message):
    pattern_1 = r'.*loan.*overdue.*repayable\sis\srs.\s?([0-9]+)'
    pattern_2 = r'.*loan.*rs\.\s([0-9]+).*overdue.*'
    pattern_3 = r'.*loan.*overdue.*repayment.*rs\.?\s([0-9]+)'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    if matcher_1 != None or matcher_2 != None or matcher_3 != None:
        return True
    else:
        return False


def extract_amount_from_overdue_message(message):
    amount = -1
    pattern_1 = r'.*loan.*overdue.*repayable\sis\srs.\s?([0-9]+)'
    pattern_2 = r'.*loan.*rs\.\s([0-9]+).*overdue.*'
    pattern_3 = r'.*loan.*overdue.*repayment.*rs\.?\s([0-9]+)'

    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)
    matcher_3 = re.search(pattern_3, message)
    if matcher_1 != None:
        amount = int(matcher_1.group(1))
    elif matcher_2 != None:
        amount = int(matcher_2.group(1))
    elif matcher_3 != None:
        amount = int(matcher_3.group(1))
    else:
        amount = -1
    return amount


def overdue_amount_extract(data, overdue_first_date):
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
        if is_overdue(message):
            amount = extract_amount_from_overdue_message(message)
            overdue_amount_list.append(amount)
        else:
            break
    return max(overdue_amount_list)



