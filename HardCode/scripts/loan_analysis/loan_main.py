
from .my_modules import *
from HardCode.scripts.Util import logger_1, conn
from datetime import datetime
import pytz
import warnings

warnings.filterwarnings('ignore')

timezone = pytz.timezone('Asia/Kolkata')

script_status = {}


def get_customer_data(cust_id):
    """
    This function establishes a connection from the mongo database and fetches data of the user.

    Parameters:
        cust_id(int)                : id of the user
        script_status(dictionary)   : a dictionary for reporting errors occured at various stages
    Returns:
        loan_data(dataframe)        : dataframe containing messages of loan disbursal, loan closed and due/overdue
        trans_data(dataframe)       : dataframe containing only transactional messgaes of the user
    """
    logger = logger_1('get_customer_data', cust_id)

    try:

        client = conn()
        # connect to database
        db = client.messagecluster
        logger.info("Successfully established the connection with DataBase")
        # db = client.messagecluster

        # connect to collection
        approval_data = db.loanapproval
        disbursed_data = db.disbursed
        overdue_data = db.loandueoverdue
        closed_data = db.loanclosed
        # trans_data = db.transaction
        closed = closed_data.find_one({"cust_id": cust_id})
        # trans = trans_data.find_one({"cust_id": cust_id})
        disbursed = disbursed_data.find_one({"cust_id": cust_id})
        approval = approval_data.find_one({"cust_id": cust_id})
        overdue = overdue_data.find_one({"cust_id": cust_id})
        loan_data = pd.DataFrame(columns=['sender', 'body', 'timestamp', 'read'])
        if closed is not None:
            closed_df = pd.DataFrame(closed['sms'])
            loan_data = loan_data.append(closed_df)
            logger.info("Found loan closed data")
        else:
            logger.error("loan closed data not found")

        '''if trans != None:
            transaction_df = pd.DataFrame(trans['sms'])
        else:
            raise Exception'''

        if disbursed is not None:
            disbursed_df = pd.DataFrame(disbursed['sms'])
            loan_data = loan_data.append(disbursed_df)
            logger.info("Found loan disbursed data")
        else:
            logger.error("loan disbursed data not found")

        if overdue is not None:
            overdue_df = pd.DataFrame(overdue['sms'])
            loan_data = loan_data.append(overdue_df)
            logger.info("Found loan overdue data")
        else:
            logger.error("loan overdue data not found")

        if approval is not None:
            approval_df = pd.DataFrame(approval['sms'])
            loan_data = loan_data.append(approval_df)
            logger.info("Found loan approval data")
        else:
            logger.error("loan approval data not found")

        loan_data.sort_values(by=["timestamp"])
        # transaction_df.sort_values(by=["timestamp"])

        loan_data = loan_data.reset_index(drop=True)
        # transaction_df = transaction_df.reset_index(drop=True)
        script_status = {'status': True, "result": loan_data}
        client.close()
    except Exception as e:
        # script_Status['data_fetch'] = -1 
        logger.critical(e)
        script_status = {'status': False, 'message': 'unable to fetch data'}
        client.close()
    finally:
        return loan_data


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
    loan_data = get_customer_data(cust_id)
    logger = logger_1('preprocessing', cust_id)
    loan_data = sms_header_splitter(loan_data)
    logger.info("Data Splitted by headers")
    loan_data_grouped = grouping(loan_data)
    logger.info("Data Grouped by Sender-Name")
    loan_details_of_all_apps = {}

    for app, grp in loan_data_grouped:
        logger.info("iteration in groups starts")
        if app == 'CASHBN' or app == 'KREDTB' or app == 'KREDTZ' or app == 'LNFRNT' or app == 'RRLOAN' or app == 'LOANAP' or app == 'KISSHT' or app == 'GTCASH' or app == 'FLASHO' or app == 'CSHMMA' or app == 'ZPLOAN' or app =='FRLOAN':


            grp = grp.sort_values(by='timestamp')
            grp = grp.reset_index(drop=True)

            loan_count = 0
            loan_details_individual_app = {}
            i = 0

            if app == 'KREDTB':
                grp = pd.DataFrame(grp)
                grp.to_csv('temp.csv')
            while i < len(grp):
                logger.info("iteration in messages starts")

                individual_loan_details = {
                    'disbursed_date': -1,
                    'closed_date': -1,
                    'loan_duration': -1,
                    'due_date': -1,
                    'loan_disbursed_amount' : -1,
                    'loan_due_amount' : -1,
                    'overdue_max_amount' : -1
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
                                    individual_loan_details['overdue_max_amount'] = float(overdue_amount_extract(grp,
                                                                                                                 overdue_first_date))
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
                                            individual_loan_details['loan_duration'] = loan_duration
                                            individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(
                                                message_closed))
                                            k = m + 1
                                            logger.info("Loan Closed!")
                                            break
                                        elif is_disbursed(message_closed):
                                            k = m
                                            break
                                        m += 1
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
                                    individual_loan_details['loan_duration'] = loan_duration
                                    individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(
                                        message_overdue))
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
                            individual_loan_details['overdue_max_amount'] = float(overdue_amount_extract(grp, overdue_first_date))
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
                                    individual_loan_details['loan_duration'] = loan_duration
                                    individual_loan_details['loan_closed_amount'] = float(
                                        closed_amount_extract(message_closed))
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
                            individual_loan_details['loan_duration'] = loan_duration
                            individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(message_new))
                            logger.info("Loan Closed!")
                            break
                        j += 1
                    loan_details_individual_app[str(loan_count)] = individual_loan_details

                i += 1

            loan_details_of_all_apps[str(app)] = loan_details_individual_app

    return loan_details_of_all_apps


def final_output(cust_id):
    '''
    Function for final output
    Parameters:
        df(dictionary)         : 
            multi dictionary consists user's loan apps details
            disbursed_date(datetime) : date of disbursal 
            closed_date(datetime)    : date of closed
            due_date(datetime)       : date of due 
            loan_closed_amount(str)  : amount received at the closing time
            loan_disbursed_amount(str) : amount recieved at the disbursal time
            loan_due_amount(str)     : due messages amount info
            overdue_max_amount(str)  : maximum overdue amount
            loan_duration(int)       : duration of loan  
        Returns:
            report(dictionary):
                pay_within_30_days(bool) :    if pay within 30 days
                current_open_amount      :    if loan is open than amount of loan
                total_loan               :    total loans
                current_open             :    current open loans
                max_amount               :    maximum loan amount in all loans
    '''
    a = preprocessing(cust_id)
    logger = logger_1('final_output', cust_id)
    report = {
        'CURRENT_OPEN': 0,
        'TOTAL_LOANS': 0,
        'PAY_WITHIN_30_DAYS': True,
        'CURRENT_OPEN_AMOUNT': [],
        'MAX_AMOUNT': -1,
        'empty': False
    }

    # final output
    li = []
    for i in a.keys():
        report['TOTAL_LOANS'] = report['TOTAL_LOANS'] + len(a[i].keys())
        for j in a[i].keys():
            try:
                li.append(float(a[i][j]['loan_disbursed_amount']))
                li.append(float(a[i][j]['loan_closed_amount']))
                li.append(float(a[i][j]['loan_due_amount']))
            except:
                pass
            if a[i][j]['loan_duration'] > 30:
                report['PAY_WITHIN_30_DAYS'] = False

            now = datetime.now()
            now = timezone.localize(now)
            disbursed_date = timezone.localize(pd.to_datetime(a[i][j]['disbursed_date']))
            days = (now - disbursed_date).days

            if a[i][j]['closed_date'] == -1:
                if days < 30:
                    report['CURRENT_OPEN'] += 1

                    try:
                        disbursed_amount = float(a[i][j]['loan_disbursed_amount'])

                        disbursed_amount_from_due = float(a[i][j]['loan_due_amount'])

                        if int(disbursed_amount)!= -1 and int(disbursed_amount_from_due) == -1:
                            report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount)

                        elif int(disbursed_amount) == -1 and int(disbursed_amount_from_due) != -1:
                            report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount_from_due)

                        elif int(disbursed_amount) ==  -1 and int(disbursed_amount_from_due) == -1:
                            report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount)

                        if int(disbursed_amount) != -1 and int(disbursed_amount_from_due) != -1:
                            report['CURRENT_OPEN_AMOUNT'].append(disbursed_amount)

                        # report['CURRENT_OPEN_AMOUNT'].append(float(a[i][j]['loan_disbursed_amount']))
                        #
                        # report['CURRENT_OPEN_AMOUNT'].append(float(a[i][j]['loan_due_amount']))

                    except BaseException as e:
                        continue
            else:
                continue

    try:
        report['MAX_AMOUNT'] = float(max(li))
    except:
        logger.info('no amount detect')
        report['empty'] = True
        script_status = {'status': True, 'message': 'success', 'result': report}
    try:
        client = conn()
        logger.info('Successfully connect to the database')
        report['modified_at'] = str(timezone.localize(datetime.now()))
        report['cust_id'] = cust_id

        client.analysis.loan.update_one({"cust_id": cust_id}, {"$set": report}, upsert=True)

        logger.info('Successfully upload result to the database')

        script_status = {'status': True, "message": "success", 'result': report}
    except Exception as e:
        logger.critical('Unable to connect to the database')
        return {'status': False, "message": str(e)}
    finally:
        client.close()

    return script_status
