import pandas as pd
import numpy as np
import regex as re
from datetime import datetime
import logging
from HardCode.scripts.Util import logger_1


# from concurrent.futures import ThreadPoolExecutor, wait, as_completed


def initialize(data):
    data['time,message'] = [0] * data.shape[0]
    data['acc_no'] = ['0'] * data.shape[0]
    data['VPA'] = [0] * data.shape[0]
    data['IMPS Ref no'] = [0] * data.shape[0]
    data['UPI Ref no'] = [0] * data.shape[0]
    data['neft'] = [0] * data.shape[0]
    data['neft no'] = [0] * data.shape[0]
    data['credit_amount'] = [0] * data.shape[0]
    data['debit_amount'] = [0] * data.shape[0]
    data['upi'] = [0] * data.shape[0]
    data['date_time'] = [0] * data.shape[0]
    data['date,message'] = [0] * data.shape[0]
    data['imps'] = 0 * data.shape[0]
    data['available balance'] = [0] * data.shape[0]


def get_account_number(data):
    pattern_1 = r'[\*nx]+([0-9]{3,})'
    pattern_2 = r'[a]\/c ([0-9]+)'
    pattern_3 = r'[\.]{3,}([0-9]+)'
    pattern_4 = r'account(.*)?\[([0-9]+)\]'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)

        if matcher_1 is not None:
            data['acc_no'][i] = str(matcher_1.group(1))

        elif matcher_2 is not None:
            data['acc_no'][i] = str(matcher_2.group(1))

        elif matcher_3 is not None:
            data['acc_no'][i] = str(matcher_3.group(1))

        elif matcher_4 is not None:
            data['acc_no'][i] = str(matcher_4.group(2))

        else:
            data['acc_no'][i] = '0'


def get_header_splitter(data):
    for i in range(data.shape[0]):
        x = data['sender'][i].split('-')
        if len(x) == 2:
            data['sender'][i] = x[-1].upper()
        else:
            data['sender'][i] = x[0][2:].upper()


def get_vpa(data):
    pattern_1 = r"([\w.-]+[@][\w]+)"
    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)

        if matcher_1 is not None:
            data['VPA'][i] = matcher_1.group(1)
        else:
            data['VPA'][i] = np.nan


def get_imps_ref_no(data):
    pattern_1 = r'imps.*?(\d{12})'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)

        if matcher_1 is not None:
            data['IMPS Ref no'][i] = int(matcher_1.group(1))

        else:
            data['IMPS Ref no'][i] = 0


def get_upi_ref_no(data):
    pattern_1 = r'upi ref no (\d{12})'
    pattern_2 = r'upi reference number \(rrn\):? ?(\d{12})'
    pattern_3 = r'upi ?[\/\-\:] ?(\d{12})'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)

        if matcher_1 is not None:
            data['UPI Ref no'][i] = int(matcher_1.group(1))

        elif matcher_2 is not None:
            data['UPI Ref no'][i] = int(matcher_2.group(1))

        elif matcher_3 is not None:
            data['UPI Ref no'][i] = int(matcher_3.group(1))
        else:
            data['UPI Ref no'][i] = 0


def get_credit_amount(data, logger):
    pattern_2 = r'(?i)credited.*?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_1 = r'(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?credited'
    pattern_4 = r'(?i)(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?deposited'
    pattern_debit_1 = r'(?i)debited(.*)?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_debit_2 = r'credited to beneficiary'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_4 = re.search(pattern_4, message)
        if matcher_1 is not None:
            matcher_debit_1 = re.search(pattern_debit_1, message)
            matcher_debit_2 = re.search(pattern_debit_2, message)
            if matcher_debit_1 is not None:
                amount = 0
            elif matcher_debit_2 is not None:
                amount = 0
            else:
                amount = matcher_1.group(1)

        elif matcher_2 is not None:
            amount = matcher_2.group(1)

        elif matcher_4 is not None:
            amount = matcher_4.group(1)

        else:
            amount = 0
        try:
            data['credit_amount'][i] = float(str(amount).replace(",", ""))
        except Exception as e:
            logger.exception("msg")
        # print(e)


def get_debit_amount(data):
    pattern_2 = r'(?i)debited.*?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_1 = r'(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?).*?debited'
    pattern_3 = r' inb txn of (?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'
    pattern_4 = r'(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?) ?w/d'
    pattern_5 = r'sbidrcard.*?(?:(?:rs|inr|\u20B9)\.?\s?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        if matcher_1:
            amount = matcher_1.group(1)

        elif matcher_2:
            amount = matcher_2.group(1)
        
        elif matcher_3:
            amount = matcher_3.group(1)
        
        elif matcher_4:
            amount = matcher_4.group(1)

        elif matcher_5:
            amount = matcher_5.group(1)

        else:
            amount = 0
        data['debit_amount'][i] = float(str(amount).replace(",", ""))


def get_upi_keyword(data):
    data['upi'] = 0 * data.shape[0]
    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()

        if 'upi' in message:
            data['upi'][i] = 1

        else:
            data['upi'][i] = 0

def get_imps_keyword(data):
    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()

        if 'imps' in message:
            data['imps'][i] = 1

        else:
            data['imps'][i] = 0


def get_neft_keyword(data):
    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        if 'neft' in message:
            data['neft'][i] = 1

        else:
            data['neft'][i] = 0


def get_neft_no(data):
    pattern_0 = r"neft.*?(\d{12})"
    pattern_1 = r'neft ?(?:no|number|cr)?[\.\: \-]{0,3}(\w{0,5}\d{6,12})'
    pattern_2 = r'utr[- ]?(?:ref)?[ \-\:]{0,3}(\w{10,16})'
    pattern_3 = r'neft ?(?:transaction with)? ?(?:ref|reference) (?:no|number)?[\.\-\:]? ?(\w{0,5}\d{6,12})'
    pattern_4 = r'neft.*?\/?(\w{0,5}\d{6,12}\w{0,2})'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_0 = re.search(pattern_0, message)
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)

        if matcher_0:
            data['neft no'][i] = matcher_0.group(1)

        elif matcher_1 is not None:
            data['neft no'][i] = matcher_1.group(1)

        elif matcher_2 is not None:
            data['neft no'][i] = matcher_2.group(1)

        elif matcher_3 is not None:
            data['neft no'][i] = matcher_3.group(1)

        elif matcher_4 is not None:
            data['neft no'][i] = matcher_4.group(1)

        else:
            data['neft no'][i] = 0


def get_month(month):
    mon = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    return str(mon.index(month) + 1)


def get_date_message(data):
    for i in range(data.shape[0]):
        try:
            data['date,message'][i] = data['timestamp'][i].date()
        except Exception as e:
            logging.exception('transaction_balance_sheet/transaction_analysis/get_date_time:' + str(e))


def get_time_message(data, logger):
    pattern_1 = r'(\d{1,2})\:(\d{2})\:(\d{2}) ?(hrs|am|pm)'
    pattern_2 = r'(\d{1,2})\:(\d{2})\:(\d{2})'
    pattern_3 = r'(\d{1,2})\:(\d{2}) ?(hrs|am|pm)'

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)

        if matcher_1 is not None:
            try:
                if matcher_1.group(4) == 'hrs':
                    time = matcher_1.group(1) + ':' + matcher_1.group(2) + ':' + matcher_1.group(3)
                elif matcher_1.group(4) == 'am':
                    time = matcher_1.group(1) + ':' + matcher_1.group(2) + ':' + matcher_1.group(3)
                else:
                    if matcher_1.group(1) == '12':
                        time = matcher_1.group(1) + ':' + matcher_1.group(2) + ':' + matcher_1.group(3)
                    else:
                        time = str(int(matcher_1.group(1)) + 12) + ':' + matcher_1.group(2) + ':' + matcher_1.group(3)

                time = datetime.strptime(time, '%H:%M:%S').time()
                data['time,message'][i] = time
            except Exception as e:
                logger.exception("msg")

        elif matcher_2 is not None:
            try:
                pattern_error_0 = r'(\d{1,2})\:(\d{1,2})\:(\d{2})\:(\d{3})'
                pattern_error = r'(\d{1,2})\:(\d{1,2})\:(\d{2})\:(\d{2})'
                matcher_error = re.search(pattern_error, message)
                matcher_error_0 = re.search(pattern_error_0, message)
                a = 0
                if matcher_error_0 is not None:
                    a = 1
                    time = matcher_error_0.group(1) + ':' + matcher_error_0.group(2) + ':' + matcher_error.group(3)

                elif matcher_error is not None:
                    a = 2
                    time = matcher_error.group(2) + ':' + matcher_error.group(3) + ':' + matcher_error.group(4)

                else:
                    a = 3
                    time = matcher_2.group()

                time = datetime.strptime(time, '%H:%M:%S').time()
                data['time,message'][i] = time
            except Exception as e:
                logger.exception("msg")
                # logging.exception('transaction_balance_sheet/transaction_analysis/get_time_message/matcher2:' + str(e))

        elif matcher_3 is not None:
            try:
                if matcher_3.group(3) == 'hrs':
                    time = matcher_3.group(1) + ':' + matcher_3.group(2) + ':00'
                elif matcher_3.group(3) == 'am':
                    time = matcher_3.group(1) + ':' + matcher_3.group(2) + ':00'
                else:
                    if matcher_3.group(1) == '12':
                        time = matcher_3.group(1) + ':' + matcher_3.group(2) + ':00'
                    else:
                        time = str(int(matcher_3.group(1)) + 12) + ':' + matcher_3.group(2) + ':00'

                time = datetime.strptime(time, '%H:%M:%S').time()
                data['time,message'][i] = time
            except Exception as e:
                logger.exception("msg")
            # logging.exception('transaction_balance_sheet/transaction_analysis/get_time_message/matcher2:' + str(e))


def get_date_time(data, logger):
    for i in range(data.shape[0]):
        date = data['date,message'][i]
        time = data['time,message'][i]
        if (date == 0 or date == '') and (time == '' or time == 0):
            continue
        elif date == 0 or date == '':
            date = pd.to_datetime(data['timestamp'][i]).date()
            date.strftime('%d/%m/%Y')
        elif time == 0 or time == '':
            continue
        try:
            data['date_time'][i] = datetime.combine(date, time)
        except Exception as e:
            logger.exception("msg")
            # print(e)


def balance_check(data):
    pattern_1 = r'(?i)(?:a\/?c|avbl|avl|available|total|avlbl) (?:bal|balance) ?:?-?\.? ?(?:(?:rs|inr|\u20B9)\.?\s?:?)(\d+(:?\,\d+)?(\,' \
                r'\d+)?(\.\d{1,2})?)'
    pattern_2 = r'(?i)(?:updated|available|avbl|abl)(?: account)? (?:balance|bal)(?: is)?(?: paytm wallet)? ?(?:(' \
                r'?:rs|inr|\u20B9)\.?\s?:?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)'

    pattern_3 = r"(?:bal|balance) is (?:(?:rs|inr|\u20B9)\.?\s?:?)(\d+(:?\,\d+)?(\,\d+)?(\.\d{1,2})?)"

    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        amount = 0
        if matcher_1 is not None:
            amount = matcher_1.group(1)

        elif matcher_2 is not None:
            amount = matcher_2.group(1)
        
        elif matcher_3 is not None:
            amount = matcher_3.group(1)

        else:
            amount = 0
        data['available balance'][i] = float(str(amount).replace(",", ""))


def get_time(data):
    for i in range(data.shape[0]):
        x = datetime.strptime(data['timestamp'][i], '%Y-%m-%d %H:%M:%S')
        data['timestamp'][i] = x


def date_time_thread(data, user_id):
    logger = logger_1('date_time_thread', user_id)
    # logger.info('starting get date func')
    get_date_message(data)
    # logger.info('starting get date func successful')

    # logger.info('starting get time func')
    get_time_message(data, logger)
    #  logger.info('starting get time func successful')

    # logger.info('starting get date time func')
    get_date_time(data, logger)


# logger.info('starting get date time func successful')


def process_data(data, user_id):
    logger = logger_1('process_data', user_id)
    try:
        #   logger.info('initializing')
        initialize(data)
        #   logger.info('initialization done')
        data.sort_values(by='timestamp', inplace=True)
        data.reset_index(drop=True, inplace=True)
        #  logger.info('starting header split')
        get_header_splitter(data)
        #  logger.info('header_split complete')

        #   logger.info('starting account number fetch')
        get_account_number(data)
        #  logger.info('account number fetch complete')

        # logger.info('starting vpa fetch')
        get_vpa(data)
        # logger.info('vpa fetch complete')

        # logger.info('starting upi ref fetch')
        get_upi_ref_no(data)
        # logger.info('upi ref fetch complete')

        # logger.info('starting debit amount fetch')
        get_debit_amount(data)
        # logger.info('debit amount fetch complete')

        # logger.info('starting credit amount fetch ')
        get_credit_amount(data, logger)
        # logger.info('credit amount fetch complete')

        # logger.info('starting neft_no fetch')
        get_neft_no(data)
        # logger.info('neft no fetch complete')

        # logger.info('starting neft keyword fetch')
        get_neft_keyword(data)
        # logger.info('neft keyword fetch complete')

        # logger.info('starting imps keyword fetch')
        get_imps_keyword(data)
        logger.info('neft keyword fetch complete')

        logger.info('starting imps reference fetch')
        get_imps_ref_no(data)
        logger.info('imps reference fetch complete')

        logger.info('starting upi keyword fetch')
        get_upi_keyword(data)
        logger.info('upi keyword fetch complete')

        logger.info('starting balance fetch')
        balance_check(data)
        logger.info('balance fetch complete')

        logger.info('starting get_time fetch')
        get_time(data)
        logger.info('get_time fetch complete')

        logger.info('starting date time thread')
        date_time_thread(data, user_id)
        logger.info('data time thread complete')
    except Exception as e:
        return {'status': False, 'message': e}
    return {'status': True, 'message': 'success', 'df': data}
