import numpy as np 
import pandas as pd
import re 
from datetime import datetime

def is_approval(message):
    """
    This funtion checks if the message is of approval or not.

    Parameters:
        message(string) : message of user
    Returns:
        bool            : True if the message is of approval else False   

    """
    #pattern_1 = r'[^pre-]approved(.*)?'
    pattern_2 = r'succesfully(.*)?approved'
    #pattern_3 = r'(.*)?has(.*)?been(.*)?approved'
    pattern_4 = r'(.*)?application\sis\sapproved(.*)?'

    #matcher_1 = re.search(pattern_1,message)
    matcher_2 = re.search(pattern_2, message)
    #matcher_3 = re.search(pattern_3,message)
    #matcher_4 = re.search(pattern_4, message)

    if matcher_2 != None:
        return True
    else:
        return False

def trans_amount_confirm(message):
    """
    This function checks if the message contains an amount or not. This function is called by trans_amount_extract.

    Parameters:
        message(string): message of the user
    Returns:
        bool           : True if amount is present else false    
    """

    pattern1 = r'(?:(?:[Rr][sS]|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)?(.*)?successfully credited(.*)?'
    pattern2 = r'\spayment\s.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern3 = r'\spayment\sof\s([0-9]+\.[0-9]+)'
    pattern4 = r'payment.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern5 = r'.*(?:credited|debited)\s*(?:by|for)\s[Rr][Ss]\.\s?([0-9]+\.[0-9]+).*'
    pattern6 = r'[Rr][Ss]\.?\s([0-9]+\.[0-9]+).*(?:credit[e]?[d]?|debit[e]?[d]?)\s(?:to|from).*'

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

    pattern1 = r'(?:(?:[Rr][sS]|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)?(.*)?successfully credited(.*)?'
    pattern2 = r'\spayment\s.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern3 = r'\spayment\sof\s([0-9]+\.[0-9]+)'
    pattern4 = r'payment.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern5 = r'.*(?:credited|debited)\s*(?:by|for)\s[Rr][Ss]\.\s?([0-9]+\.[0-9]+).*'
    pattern6 = r'[Rr][Ss]\.?\s([0-9]+\.[0-9]+).*(?:credit[e]?[d]?|debit[e]?[d]?)\s(?:to|from).*'

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

def amount_extract(trans_data, disbursed_date):
    INDEX = 0
    amount = -1
    for i in range(trans_data.shape[0]):
        iter_date = datetime.strptime(str(trans_data['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
        
        if (iter_date >= disbursed_date):
            start_date = iter_date
            break
        INDEX += 1
    dates_within_5_mins = []
    for i in range(INDEX, trans_data.shape[0]):
        a = datetime.strptime(str(trans_data['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
        if (a - start_date).seconds/60 < 5:
            dates_within_5_mins.append(i)
        else:
            break
    for i in dates_within_5_mins:
        message = str(trans_data['body'][i]).lower()
        if trans_amount_confirm(message):
            amount = trans_amount_extract(message)
            break
        else:
            amount = -1
    return amount        

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
    amount = 0
    pattern_1 = r'.*loan.*approved.*rs\.?\s([0-9]+).*'
    pattern_2 = r'.*loan.*disbursed.*amounting.*\s([0-9]+)\srupees.*'
    
    matcher_1 = re.search(pattern_1, message)
    matcher_2 = re.search(pattern_2, message)

    if matcher_1 != None:
        amount = int(matcher_1.group(1))
    elif matcher_2 != None:
        amount = int(matcher_2.group(1))
    else:
        amount = 0
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
    pattern_6 = r'.*successfully\sreceived\spayment.*rs\.\s([0-9]+).*'

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
        #x = data['sender'][i]
        x = data['sender'][i].split('-')
        #data['Sender-Name'][i] = x[2 : ].upper()
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

def is_due(message):
    pattern_1 = r'.*payment.*rs\.?.*?([0-9]+).*due.*'       # group(1) for amount
    pattern_2 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'    # group(1) for date and group(2) for amount 
    pattern_3 = r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*'         # group(1) for amount and group(2) for date 
    pattern_4 = r'due\s(?:on)?.*([0-9]+/[0-9]+).*'                      # group(1) for date in cashbn
    pattern_5 = r'.*loan.*rs\.?.*?([0-9]+).*due.*'                      # group(1) for loan amount 
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
    date = ''
    pattern_1 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*repayment.*\s([0-9]+)'       # group(1) for date and group(2) for amount
    pattern_2 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'    # group(1) for date and group(2) for amount 
    pattern_3 = r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*'         # group(1) for amount and group(2) for date 
    pattern_4 = r'due\s(?:on)?.*([0-9]+/[0-9]+).*'                          # group(1) for date in cashbn

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
        date = ''
    return date                     



def due_amount_extract(message):
    amount = ''
    pattern_1 = r'.*payment.*rs\.?.*?([0-9]+).*due.*'       # group(1) for amount
    pattern_2 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'    # group(1) for date and group(2) for amount 
    pattern_3 = r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*'       # group(1) for amount and group(2) for date
    pattern_4 = r'.*loan.*rs\.?.*?([0-9]+).*due.*'                     # group(1) for loan amount
    pattern_5 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*repayment.*\s([0-9]+)'    # group(1) for date and group(2) for amount

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
        amount = 0
    return amount 


def overdue_amount_extract(data, overdue_first_date):
    INDEX = 0
    amount = 0
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



def closed_amount_extract(message):
    amount = '0'
    pattern1 = r'\spayment\s.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern2 = r'\spayment\sof\s([0-9]+\.[0-9]+)'
    pattern3 = r'payment.*?(?:(?:[Rr][sS]|INR|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern4 = r'.*successfully\sreceived\spayment.*rs\.\s([0-9]+).*'

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
        amount = '0'
    return amount  