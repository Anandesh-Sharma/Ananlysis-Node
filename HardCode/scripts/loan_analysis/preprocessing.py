from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
import pandas as pd
from datetime import datetime
from HardCode.scripts.Util import logger_1
from HardCode.scripts.loan_analysis.my_modules import *

def preprocessing(cust_id):
    # transaction_data = trans_data
    """
    This function is preprocessed the data and give user's loan details in a dictionary.

    Parameters:
        loan_data              : loan_data of a user consist approval, disbursal, due/overdue and closing messages
    Methos use for preprocessing:
        sms_header_splitter    : split the sender's sms header to remove unwanted text
        grouping               : group the sender names in different dataframes
    Returns:
        user_details(dictionary)         :
            multi dictionary consists user's loan apps details
            disbursed_date(datetime) : date of disbursal
            closed_date(datetime)    : date of closed
            due_date(datetime)       : date of due
            loan_closed_amount(str)  : amount received at the closing time
            loan_disbursed_amount(str) : amount recieved at the disbursal time
            loan_due_amount(str)     : due messages amount info
            overdue_max_amount(str)  : maximum overdue amount
            loan_duration(int)       : duration of loan
    """
    loan_data = fetch_user_data(cust_id)
    logger = logger_1('preprocessing', cust_id)
    loan_data = sms_header_splitter(loan_data)
    logger.info("Data Splitted by headers")
    loan_data_grouped = grouping(loan_data)
    logger.info("Data Grouped by Sender-Name")
    loan_details_of_all_apps = {}
    user_app_list = []

    for app, grp in loan_data_grouped:
        logger.info("iteration in groups starts")
        user_app_list.append(str(app))
        if app == 'CASHBN' or app == 'KREDTB' or app == 'KREDTZ' or app == 'LNFRNT' or app == 'RRLOAN' or app == 'LOANAP' or app == 'KISSHT' or app == 'GTCASH' or app == 'FLASHO' or app == 'CSHMMA' or app == 'ZPLOAN' or app == 'FRLOAN' or app == 'SALARY':

            grp = grp.sort_values(by='timestamp')
            grp = grp.reset_index(drop=True)

            loan_count = 0
            loan_details_individual_app = {}
            i = 0
            FLAG = False

            while i < len(grp):
                logger.info("iteration in messages starts")

                individual_loan_details = {
                    'disbursed_date': -1,
                    'closed_date': -1,
                    'loan_duration': -1,
                    'due_date': -1,
                    'loan_disbursed_amount': -1,
                    'loan_due_amount': -1,
                    'overdue_max_amount': -1,
                    'overdue_days' : -1,
                    'messages': []
                }
                message = str(grp['body'][i].encode('utf-8')).lower()

                if is_disbursed(message):
                    """
                    The function to check disbursal message

                    Parameters:
                        message(str)    : a single user message in lowercase

                    Returns:
                        disbursed_date(dictionary)           : disbursal date of the loan 
                        loan_disbursed_amount(dictionary)    : loan_amount (if present)    
                    """
                    logger.info("disbursed message found")
                    individual_loan_details['disbursed_date'] = str(grp['timestamp'][i])
                    disbursed_date = datetime.strptime(str(grp['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
                    individual_loan_details['loan_disbursed_amount'] = float(disbursed_amount_extract(message))
                    individual_loan_details['messages'].append(str(grp['body'][i]))
                    loan_count += 1
                    j = i + 1
                    while j < len(grp):
                        logger.info("iteration in sub messages starts")

                        message_new = str(grp['body'][j].encode('utf-8')).lower()

                        if is_disbursed(message_new):

                            """
                            The function to check next message is also disbursal or not. if YES, than get back to previous method

                            Parameters:
                                message_new(str)     : next user message after disbursal message in lowercase
                            """
                            logger.info("another disbursed message found before closing previous loan")
                            i = j
                            break

                        elif is_due(message_new):

                            """
                            The function to check if the next message is due 

                            Parameters:
                                message_new(str)     : next user messsage after disbursal message in lowercase

                            Returns:
                                due_date(dictionary)         : due_date of the loan
                                loan_due_amount(dictionary)  : loan_due_amount(if present)   
                            """
                            logger.info("due message found")
                            individual_loan_details['due_date'] = due_date_extract(message_new)
                            if individual_loan_details['due_date'] == -1:
                                individual_loan_details['due_date'] = str(grp['timestamp'][j])
                            k = j + 1
                            individual_loan_details['loan_due_amount'] = due_amount_extract(message_new)
                            individual_loan_details['messages'].append(str(grp['body'][j]))
                            while k < len(grp):
                                logger.info("Looking for overdue message")

                                message_overdue = str(grp['body'][k]).lower()

                                if is_overdue(message_overdue):

                                    """
                                    The function to check if the next message is overdue

                                    Parameters:
                                        message_overdue     : next user message after due message in lowercase

                                    Returns:
                                        overdue_max_amount(dictionary)  : maximum overdue amount (if present)    
                                    """
                                    logger.info("overdue message found")
                                    overdue_first_date = datetime.strptime(str(grp['timestamp'][k]),
                                                                           '%Y-%m-%d %H:%M:%S')
                                    individual_loan_details['overdue_max_amount'] = float(
                                        overdue_amount_extract(grp, overdue_first_date))
                                    individual_loan_details['messages'].append(str(grp['body'][k]))
                                    m = k + 1
                                    while m < len(grp):
                                        logger.info("Looking for closed message")
                                        message_closed = str(grp['body'][m]).lower()
                                        if is_closed(message_closed):
                                            """
                                            The function to check if the next message is closed

                                            Parameters:
                                                message_closed    : next user message after overdue message in lowercase

                                            Returns:
                                                closed_date(dictionary)                : closed_date of the loan
                                                loan_duration(dictionary)              : duration of loan (closed_date - disbursal_date)
                                                loan_closed_amount(dictionary)         : loan_closed_amount (if present)   
                                            """
                                            logger.info("closed message found")
                                            individual_loan_details['closed_date'] = str(grp['timestamp'][m])
                                            closed_date = datetime.strptime(str(grp['timestamp'][m]),
                                                                            '%Y-%m-%d %H:%M:%S')
                                            loan_duration = (closed_date - disbursed_date).days
                                            if loan_duration > 15:
                                                individual_loan_details['overdue_days'] = int(loan_duration - 15)
                                            individual_loan_details['loan_duration'] = loan_duration
                                            individual_loan_details['loan_closed_amount'] = float(
                                                closed_amount_extract(message_closed))
                                            individual_loan_details['messages'].append(str(grp['body'][m]))
                                            k = m + 1
                                            FLAG = True
                                            logger.info("Loan Closed!")
                                            break
                                        elif is_disbursed(message_closed):
                                            k = m
                                            FLAG = True
                                            break
                                        m += 1
                                if FLAG == True:
                                    i = j
                                    break
                                elif is_closed(message_overdue):

                                    """
                                    The function to check if the next message is closed

                                    Parameters:
                                        message_overdue    : next user message after due message in lowercase

                                    Returns:
                                        closed_date(dictionary)                : closed_date of the loan
                                        loan_duration(dictionary)              : duration of loan (closed_date - disbursal_date)
                                        loan_closed_amount(dictionary)         : loan_closed_amount (if present)   
                                    """
                                    logger.info("loan closed messagge found")
                                    individual_loan_details['closed_date'] = str(grp['timestamp'][k])
                                    closed_date = datetime.strptime(str(grp['timestamp'][k]), '%Y-%m-%d %H:%M:%S')
                                    loan_duration = (closed_date - disbursed_date).days
                                    if loan_duration > 15:
                                        individual_loan_details['overdue_days'] = int(loan_duration - 15)
                                    individual_loan_details['loan_duration'] = loan_duration
                                    individual_loan_details['loan_closed_amount'] = float(
                                        closed_amount_extract(message_overdue))
                                    individual_loan_details['messages'].append(str(grp['body'][k]))
                                    break
                                elif is_disbursed(message_overdue):

                                    """
                                    The function to check next message is also disbursal or not. if YES, than get back to previous method

                                    Parameters:
                                        message_new(str)     : next user message after due message in lowercase
                                    """
                                    logger.info("another disbursed message found before closing previous loan")
                                    j = k
                                    break

                                k += 1
                        if FLAG  == True:
                            j = k
                            break
                        elif is_overdue(message_new):

                            """
                            The function to check if the next message is overdue

                            Parameters:
                                message_overdue     : next user message after disbursal message in lowercase

                            Returns:
                                overdue_max_amount(dictionary)  : maximum overdue amount (if present)    
                            """
                            logger.info('overdue message found')
                            overdue_first_date = datetime.strptime(str(grp['timestamp'][j]), '%Y-%m-%d %H:%M:%S')
                            individual_loan_details['overdue_max_amount'] = float(
                                overdue_amount_extract(grp, overdue_first_date))
                            individual_loan_details['messages'].append(str(grp['body'][j]))
                            m = j + 1
                            while m < len(grp):
                                message_closed = str(grp['body'][m]).lower()
                                if is_closed(message_closed):
                                    """
                                    The function to check if the next message is closed

                                    Parameters:
                                        message_overdue    : next user message after overdue message in lowercase

                                    Returns:
                                        closed_date(dictionary)                : closed_date of the loan
                                        loan_duration(dictionary)              : duration of loan (closed_date - disbursal_date)
                                        loan_closed_amount(dictionary)         : loan_closed_amount (if present)   
                                    """
                                    logger.info("loan closed message found")
                                    individual_loan_details['closed_date'] = str(grp['timestamp'][m])
                                    closed_date = datetime.strptime(str(grp['timestamp'][m]), '%Y-%m-%d %H:%M:%S')
                                    loan_duration = (closed_date - disbursed_date).days
                                    if loan_duration > 15:
                                        individual_loan_details['overdue_days'] = int(loan_duration - 15)
                                    individual_loan_details['loan_duration'] = loan_duration
                                    individual_loan_details['loan_closed_amount'] = float(
                                        closed_amount_extract(message_closed))
                                    individual_loan_details['messages'].append(str(grp['body'][m]))
                                    j = m + 1
                                    logger.info("Loan Closed!")
                                    break
                                elif is_disbursed(message_closed):
                                    """
                                    The function to check next message is also disbursal or not. if YES, than get back to previous method

                                    Parameters:
                                        message_new(str)     : next user message after due message in lowercase
                                    """
                                    logger.info("another disbursed message found before closing previous loan")
                                    j = m
                                    break
                                m += 1
                        elif is_closed(message_new):

                            """
                            The function to check if the next message is closed

                            Parameters:
                                message_overdue    : next user message after disbursal message in lowercase

                            Returns:
                                closed_date(dictionary)                : closed_date of the loan
                                loan_duration(dictionary)              : duration of loan (closed_date - disbursal_date)
                                loan_closed_amount(dictionary)         : loan_closed_amount (if present)   
                            """
                            logger.info("loan closed message found")
                            individual_loan_details['closed_date'] = str(grp['timestamp'][j])
                            closed_date = datetime.strptime(str(grp['timestamp'][j]), '%Y-%m-%d %H:%M:%S')
                            loan_duration = (closed_date - disbursed_date).days
                            if loan_duration > 15:
                                individual_loan_details['overdue_days'] = int(loan_duration - 15)
                            individual_loan_details['loan_duration'] = loan_duration
                            individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(message_new))
                            individual_loan_details['messages'].append(str(grp['body'][j]))
                            logger.info("Loan Closed!")
                            break
                        j += 1
                    loan_details_individual_app[str(loan_count)] = individual_loan_details

                i += 1
    
            loan_details_of_all_apps[str(app)] = loan_details_individual_app

    return loan_details_of_all_apps, user_app_list
