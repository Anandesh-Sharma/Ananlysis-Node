from HardCode.scripts.Util import conn, convert_json, logger_1
import warnings
import re
from datetime import datetime
import pytz
import pandas as pd

warnings.filterwarnings("ignore")


def get_loan_closed_messages(data, loan_messages_filtered, result, name):
    logger = logger_1("loan closed messages", name)
    selected_rows = []
    all_patterns = [
        r'.*?loan.*?closed.*?',
        r'.*?closed.*?successfully.*?',
        r'successfully\sreceived\spayment.*rs\.\s[0-9]{3,6}',
        r'loan.*?paid\s(?:back|off)',
        r'making\spayment.*?home\scredit\sloan',
        r'bhugta+n\skarne\ske\sliye\sdhanya?wad',
        r'payment.*?was\ssuccessful',
        r'payment\sof.*?received.*?loan',
        r'payment\sof.*?agreement.*?received',
        r'received.*?payment\s(of|rs).*?loan',
        r'rcvd\spayment\s(of|rs).*?loan',
        r'thank\syou\sfor\spayment.*?towards.*?loan',
        r'acknowledge\sreceipt.*?emi\sof.*?(against|towards).*?loan',
        r'your\sfirst\sloan.*?paid\ssuccessfully',
        r'you\sjust\spaid.*?towards\sloan',
        r'thanks\sfor\spayment.*?for\sloan',
        r'payment\sreceived\sfor.*?loan',
        r'received.*\n\n.*towards\syour\sloan',
        r'(?:repayment|payment).*(?:is|has\sbeen)\s?(?:well)?\sreceived',
        r'received.*payment\sof\s(?:rs\.?|inr)'
    ]

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue

        message = str(data['body'][i]).lower()
        for pattern in all_patterns:
            matcher = re.search(pattern, message)
            if matcher:
                selected_rows.append(i)
                break
    logger.info("Loan closed sms extracted successfully")

    logger.info("Append name in result dictionary for loan closed")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan closed successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan closed")
    x= data.copy()[mask].reset_index(drop=True)
    return x


def replace_parenthesis(message):
    # this was done as in some cases cmount was written in a bracket due to which garbage value was detected by the
    # regex
    message = message.replace('(', '')
    message = message.replace(')', '')
    return message


# def get_loan_messages(data):
#     loan_messages = []
#     pattern_1 = '(.*)?loan(.*)?'
#     pattern_2 = 'kreditbee'
#     pattern_3 = 'cashbean'
#     pattern_4 = 'loanfront'
#     pattern_5 = 'loanapp'
#     pattern_6 = 'kissht'
#     pattern_7 = 'gotocash'
#     pattern_8 = 'cashmama'
#
#     data['body'] = data['body'].apply(lambda m: replace_parenthesis(m) )
#     for i in range(data.shape[0]):
#         message = str(data['body'][i]).lower()
#
#         matcher_1 = re.search(pattern_1, message)
#         matcher_2 = re.search(pattern_2, message)
#         matcher_3 = re.search(pattern_3, message)
#         matcher_4 = re.search(pattern_4, message)
#         matcher_5 = re.search(pattern_5, message)
#         matcher_6 = re.search(pattern_6, message)
#         matcher_7 = re.search(pattern_7, message)
#         matcher_8 = re.search(pattern_8, message)
#
#         if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None:
#             loan_messages.append(i)
#     return loan_messages

def get_loan_messages(data):
    loan_messages = []
    all_patterns = [
        'loan',
        'kreditbee',
        'cashbean',
        'loanfront',
        'loanapp',
        'kissht',
        'gotocash',
        'cashmama',
        'nira'
    ]
    header = ['kredtb', 'cashbn', 'lnfrnt', 'cshmma', 'kredtz', 'rrloan',
              'frloan', 'wfcash', 'bajajf', 'flasho', 'kissht', 'gtcash', 'bajafn', 'monvew', 'mpockt',
              'mpokkt', 'montap', 'mnytap', 'erupee','flasho','qcrdit','qcredt','cashln','paymei','pmifsp',
              'salary','esalry','cashme','moneed','bajajf','dhanii','idhani','dhanip','krbeee','krtbee','nirafn','nlrafn','pdnira','pdnlra',
              'icredt','nanocd','nanocr','zestmn','loanzm','lnfrnt','loanap','cshmma','upward','loanit','lenden','vivifi','shubln','paymin','homecr',
              'branch','sthfin','zestmn','loantp','mcreds','casheb','abcfin','cfloan','capflt','icashe','loanxp','paysns','rapidr',
              'cbtest','rsloan','rupbus','ckcash','llnbro','cashbs','credme','atomec','finmtv','cashtm','roboin','trubal','payltr','cashbk','loante',
              'payuib','iavail','smcoin','ruplnd','ftcash','rupeeh','cashmt','loanbl','cashep','cashem','tatacp','loanco','loanfu','loanpl','haaloo',
              'rsfast','cashbo','cashin','rupmax','cashpd','lendko','loanfx','mudrak','prloan','cmntri','cashmx','rupls','rscash','ezloan','ftloan',
              'abcash','loanhr']

    data['body'] = data['body'].apply(lambda m: replace_parenthesis(m))
    for i in range(data.shape[0]):
        head = str(data['sender'][i]).lower()
        if head[2:] in header or head[3:] in header:
            loan_messages.append(i)
            continue
        else:
            for pattern in all_patterns:
                message = str(data['body'][i]).lower()
                matcher = re.search(pattern, message)
                if matcher:
                    loan_messages.append(i)
                    break
    return loan_messages


# def get_loan_messages_promotional_removed(data, loan_messages):
#     loan_messages_filtered = []
#
#     pattern_1 = r'(.*)?apply(.*)?'
#     pattern_2 = r'(.*)?offer(.*)?'
#     pattern_3 = r'(.*)?offers(.*)?'
#     pattern_4 = r'(.*)?avail(.*)?'
#     pattern_5 = r'(.*)?instant(.*)?'
#     pattern_6 = r'(.*)?instantly(.*)?'
#     pattern_7 = r'(.*)?cashback(.*)?'
#     pattern_8 = r'(.*)?voucher(.*)?'
#     pattern_9 = r'(.*)?discount(.*)?'
#     pattern_10 = r'(.*)?hurry(.*)?'
#     pattern_11 = r'(.*)?get(.*)loan(.*)?'
#
#     for i in range(data.shape[0]):
#         if i not in loan_messages:
#             continue
#         message = str(data['body'][i]).lower()
#         matcher_1 = re.search(pattern_1, message)
#         matcher_2 = re.search(pattern_2, message)
#         matcher_3 = re.search(pattern_3, message)
#         matcher_4 = re.search(pattern_4, message)
#         matcher_5 = re.search(pattern_5, message)
#         matcher_6 = re.search(pattern_6, message)
#         matcher_7 = re.search(pattern_7, message)
#         matcher_8 = re.search(pattern_8, message)
#         matcher_9 = re.search(pattern_9, message)
#         matcher_10 = re.search(pattern_10, message)
#         matcher_11 = re.search(pattern_11, message)
#
#         if matcher_1 is not None or matcher_2 is not None or matcher_3 is not None or matcher_4 is not None or matcher_4 is not None or matcher_5 is not None or matcher_6 is not None or matcher_7 is not None or matcher_8 is not None or matcher_9 is not None or matcher_10 is not None or matcher_11 is not None:
#             pass
#         else:
#             loan_messages_filtered.append(i)
#
#     return loan_messages_filtered

def get_loan_messages_promotional_removed(data, loan_messages):
    loan_messages_filtered = []
    not_needed = []
    all_patterns = [
        r'kyc',
        r'sbi\scard',
        r'credited\sto\syour\swallet',
        r'spice\smoney',
        r'sign\sthe\selectronic\scontract',
        r'golden\slightning',
        r'gold\scash',
        r'after\syour\sconfirmation',
        r'your\spersonalised\sloan\soffer',
        r'emi\scard|credit\scard',
        r'\se-?sign\s',
        r'claim\sbonus',
        # r'icredit|rupeeplus',
        r'good\snews',
        r'confirm\snow',
        r'use\sdebit\scard.*netbanking.*wallets.*upi',
        r'are\syou\snot\sgetting\sloan'
    ]
    for i in range(data.shape[0]):
        if i not in loan_messages:
            continue
        message = str(data['body'][i]).lower()
        for pattern in all_patterns:
            matcher = re.search(pattern, message)
            if matcher:
                not_needed.append(i)
                break
    loan_messages_filtered = list(set(loan_messages) - set(not_needed))
    return loan_messages_filtered


def get_approval(data, loan_messages_filtered, result, name):
    logger = logger_1("loan approval", name)
    selected_rows = []
    all_patterns = [
        r'successfully\sapproved',
        r'has\sbeen\sapproved',
        r'documents\shas\sbeen\ssuccessfully\sverified'
    ]

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        for pattern in all_patterns:
            matcher = re.search(pattern, message)
            if matcher:
                selected_rows.append(i)
                break
    logger.info("Loan approval sms extracted successfully")

    logger.info("Append name in result dictionary for loan approval")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan approval successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan approval")
    approve = data.copy()[mask].reset_index(drop=True)
    return approve


def get_disbursed(data, loan_messages_filtered, result, name):
    logger = logger_1("loan disbursed", name)
    selected_rows = []
    all_patterns = [
        r'(?:is|has\sbeen)\sdisburse[d]?',
        r'disbursement\shas\sbeen\scredited',
        r'has\sbeen\stransferred.*account',
        r'disburse?ment.*has\sbeen\sinitiated',
        r'is\stransferred.*account',
        r'loan\sapplication.*?successfully\ssubmitted.*?bank',
        r'you.*?received.*?loan\samount',
        r'your.*?loan.*?made\ssuccessfully.*?loan\samount',
        r'loan\srs.*?disbursed',
        r'loan\shas\sbeen\sreleased\sto.*?bank',
        r'rs.*?credited\sto\sloan\sa\/c',
        r'disbursed.*?loan.*?to\syour\sbank',
        r'amount\sfinanced\sfor\sloan.*?rs',
        r'personal\sloan.*?transferred\ssuccessfully',
        r'your\sloan\sdisbursement\swas\ssuccess',
        r'loan.*?approved.*?cash.*?issued\sto.*?bank\saccount',
        r'loan.*?approved.*?will\stransfer.*?funds.*bank\saccount',
        r'loan.*approved.*fund[s]?\swill\sbe\stransferred.*bank\saccount',
        r'loan.*approved.*disbursed\sinto\syour\saccount',
        r'loan.*approved.*will\srelease.*loan\sto.*account',
        r'loan.*approved.*sent\srs.*to\syour\saccount',
        r'loan.*approved.*will\sbe\sdisbursed',
        r'loan.*approved.*credit\sto.*?bank\saccount'
    ]

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()
        for pattern in all_patterns:
            matcher = re.search(pattern, message)

            if matcher:
                selected_rows.append(i)
                break
    logger.info("Loan disbursed sms extracted successfully")

    logger.info("Append name in result dictionary for loan disbursed")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan disbursed successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan disbursed")
    z = data.copy()[mask].reset_index(drop=True)
    return z


def get_loan_rejected_messages(data, loan_messages_filtered, result, name):
    logger = logger_1("loan rejection", name)
    selected_rows = []
    all_patterns_1 = [
        r'loan\sapplication.*?is\srejected',
        r'loan\sapplication.*?got\srejected',
        r'loan.*paysense\sis\srejected',
        r'has\sbeen.*?rejected',
        r'is\sdeclined',
        r'has\sbeen\sdeclined',
        r'has\snot\sbeen\sapproved',
        r'was\snot\sapproved',
        r'was\sbeen\srejected',
        r'could\snot\sget\sapproved',
        r"sorry.*?couldn't.*?eligible.*?loan",
        r'credit.*rejected.*loan.*application'
        r'loan\s(?:is|was|has\sbeen)\srejected',
        r'sorry.*not.*suitable\sloan\soffer',
        r'unfortunately.*application\scould\snot\smatch.*eligibility\scriteria',
        r'sorry.*can\s?not\sprocess\syour\sapplication',
        r'loan\sprocess\scan\s?not\sbe\scompleted',
        r'loan\sapplication\shas\snot\spassed\sthe\sreview',
        r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
        r'loan\sapplication\sfailed\sto\spass',
        r'unable\sto\sserve\syou.*at\sthe\smoment',
        r'unfortunately.*can\s?not\sapprove\syou\sfor\s?[a]?\sloan',
        r'discrepancy\sin\sthe\sdocuments',
        r'unable\sto\sprocess\syour\sapplication',
        r'cannot\sprovide\syou\s?[a]?\sloan',
        r'loan\sapplication.*cancelled',
        r'loan(.)*not(.)*eligib',
        r'loan\sdid\snot\spass'
    ]
    all_patterns_2 = [
        r'low\scibil\sscore',
        r'low\scredit\sscore',
        r'is\sdue',
        r'network\scard'
    ]

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()
        for pattern in all_patterns_1:
            matcher = re.search(pattern, message)
            if matcher:
                match = False
                for pattern_2 in all_patterns_2:
                    matcher = re.search(pattern_2, message)
                    if matcher is not None:
                        match = True
                        break
                if match:
                    break
                selected_rows.append(i)
                break
    logger.info("Loan rejection sms extracted successfully")

    logger.info("Append name in result dictionary for loan rejction")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan rejection successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan rejection")
    reject = data.copy()[mask].reset_index(drop=True)
    return reject


def get_over_due(data, loan_messages_filtered, result, name):
    logger = logger_1("loan due overdue", name)
    selected_rows = []
    # pattern_1 = r'(.*)?immediate(.*)payment(.*)'
    # pattern_2 = r'(.*)?delinquent(.*)?'
    # # pattern_3 = r'(.*)?has(.*)cd?bounced(.*)?'
    # pattern_4 = r'missed(.*)?payments'
    # pattern_5 = r'(.*)?due(.*)?'
    # pattern_6 = r'\sover-?due\
    all_patterns = [
        r'missed.*installment.*pay\sback',
        r'immediate\spayment',
        r'delinquent',
        r'missed\spayments',
        r'is\sdue',
        r'is\soverdue',
        r'loan\sis\sdue',
        r'due\sdate',
        r'due\shai',
        r'overdue\shai',
        r'\sover-?due',
        r'loan\sis\son\sdue',
        r'emi\sof.*?was\sdue\son',
        r'loan.*?emi\srs.*?due\son',
        r'payment\sof.*?against.*?loan.*?bounced',
        r"you\sstill\shaven['o]t\spaid.*?loan",
        r'you\shave\smissed.*?payment.*?loan',
        # r'loan\sof\srs.*?disbursed\sfrom.*?a\/c',
        r'repay\syour\semi\samount\sdue\son',
        r'loan.*?borrowed.*?will\sbe\sdue\son',
        r'k[io]\sdey\shai',
        r'pay.*?loan\sof\srs.*?by',
        r'charges\sof\srs.*?in\syour\sloan',
        r'your\srepayment\sdate.*?repayment\samount',
        r'loan.*?bakaya\shai',
        r'earlysalary.*?requested\spayment',
        r'missed\syour\spayment\son.*?loan',
        r'not\spaid\semi.*?against\syour\sloan',
        r'emi.*?dhani\sloan\sis\sstill\sdue',
        r'last\sday\sfor.*?loan\srepayment',
        r'payment\sis\sdelayed\sby\sweeks',
        r'do\snot\sforget\sto\spay.*?loan',
        r'your\sloan\sis\sstill\sunpaid',
        r'promised\sto\spay\srs.*?loan',
        r'emi.*?will\sbe\s(auto-)?debited',
        r'instalment\sis\sunpaid',
        r'dues\sof\srs.*?outstanding.*?for\sloan',
        r'.*loan.*overdue.*repayable\sis\srs.\s?([0-9]+)',
        r'.*loan.*rs\.\s([0-9]+).*overdue.*',
        r'.*loan.*overdue.*repayment.*rs\.?\s([0-9]+)',
        r'despite\sseveral\sreminders.*over[-]?\s?due.*legal\saction',
        r'.*payment.*rs\.?.*?([0-9]+).*due.*',  
        r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)',  
        r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*',  
        r'due\s(?:on)?.*([0-9]+/[0-9]+).*',  
        r'.*loan.*rs\.?.*?([0-9]+).*due.*',
        r'.*payment.*due.*',
        r'(?:emi|loan|repayment|payment)\sis\sdue',
        r'pay\s?(?:your)?\soverdue\samount',
        r'is\syour\sdue\s(?:day|date)',
        r'(?:a\/c|account)\sis\sdue',
        r'repayment\sdue\s(?:day|date)',
        r'not\sreceived\s(?:outstanding|o\/s)\s(?:amount|amt)',
        r'loan\srepayment\sis\slate',
        r'not\s(?:make|made).*payment\sof.*loan\swithin.*due\sdate',
        r'overdue\sbills\shave\snot\sbeen\sprocessed',
        r'loan.*passed\sthe\sdue\sdate',
        r'repayment.*is\spending',
        r'settle\syour\sdues.*legal\saction',
        r'pay\surgently',
        r'will\sbe\sauto\s?[-]?debited.*against\syour\sdues'
    ]

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()
        for pattern in all_patterns:
            matcher = re.search(pattern, message)

            if matcher:
                selected_rows.append(i)
                break

    logger.info("Loan due overdue sms extracted successfully")

    logger.info("Append name in result dictionary for loan due overdue")
    if name in result.keys():
        a = result[name]
        a.extend(list(selected_rows))
        result[name] = a
    else:
        result[name] = list(selected_rows)
    logger.info("Appended name in result dictionary for loan due overdue successfully")

    mask = []
    for i in range(data.shape[0]):
        if i in selected_rows:
            mask.append(True)
        else:
            mask.append(False)
    logger.info("Dropped sms other than loan due overdue")
    overdue = data.copy()[mask].reset_index(drop=True)
    return overdue


def loan(df, result, user_id, max_timestamp, new):
    # logger = logger_1("loan_classifier", user_id)
    # logger.info("get all loan messages")
    # loan_messages = get_loan_messages(df)
    # logger.info("remove all loan promotional messages")
    # loan_messages_filtered = get_loan_messages_promotional_removed(df, loan_messages)
    #
    # logger.info("get all loan due overdue messages")
    # data = get_over_due(df, loan_messages_filtered, result, user_id)
    # logger.info("Converting loan due overdue dataframe into json")
    # data_over_due = convert_json(data, user_id, max_timestamp)
    #
    # logger.info("get all loan approval messages")
    # data = get_approval(df, loan_messages_filtered, result, user_id)
    # logger.info("Converting loan approval dataframe into json")
    # data_approve = convert_json(data, user_id, max_timestamp)
    #
    # logger.info("get all loan rejection messages")
    # data = get_loan_rejected_messages(df, loan_messages_filtered, result, user_id)
    # logger.info("Converting loan rejection dataframe into json")
    # data_reject = convert_json(data, user_id, max_timestamp)
    #
    # logger.info("get all loan disbursed messages")
    # data = get_disbursed(df, loan_messages_filtered, result, user_id)
    # logger.info("Converting loan disbursed dataframe into json")
    # data_disburse = convert_json(data, user_id, max_timestamp)
    #
    # logger.info("get all loan closed messages")
    # data = get_loan_closed_messages(df, loan_messages_filtered, result, user_id)
    # logger.info("Converting loan closed dataframe into json")
    # data_closed = convert_json(data, user_id, max_timestamp)

    logger = logger_1("loan_classifier", user_id)
    logger.info("get all loan messages")
    loan_messages = get_loan_messages(df)

    logger.info("remove all loan promotional messages")
    loan_messages_filtered = get_loan_messages_promotional_removed(df, loan_messages)
    logger.info("get all loan disbursed messages")

    data = get_disbursed(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan disbursed dataframe into json")
    data_disburse = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan due overdue messages")
    data = get_over_due(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan due overdue dataframe into json")
    data_over_due = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan closed messages")
    data = get_loan_closed_messages(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan closed dataframe into json")
    data_closed = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan rejection messages")
    data = get_loan_rejected_messages(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan rejection dataframe into json")
    data_reject = convert_json(data, user_id, max_timestamp)

    logger.info("get all loan approval messages")
    data = get_approval(df, loan_messages_filtered, result, user_id)
    logger.info("Converting loan approval dataframe into json")
    data_approve = convert_json(data, user_id, max_timestamp)

    try:
        logger.info('making connection with db')
        client = conn()
        db = client.messagecluster
    except Exception as e:
        logger.critical('error in connection')
        return {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    logger.info('connection success')

    if new:
        logger.info("New user checked")
        db.loanclosed.update({"cust_id": int(user_id)}, {"cust_id": int(user_id), 'timestamp': data_closed['timestamp'],
                                                         'modified_at': str(
                                                             datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                         "sms": data_closed['sms']}, upsert=True)
        db.loanapproval.update({"cust_id": int(user_id)},
                               {"cust_id": int(user_id), 'timestamp': data_approve['timestamp'],
                                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                "sms": data_approve['sms']}, upsert=True)
        db.loanrejection.update({"cust_id": int(user_id)},
                                {"cust_id": int(user_id), 'timestamp': data_reject['timestamp'],
                                 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                 "sms": data_reject['sms']}, upsert=True)
        db.disbursed.update({"cust_id": int(user_id)},
                            {"cust_id": int(user_id), 'timestamp': data_disburse['timestamp'],
                             'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                             "sms": data_disburse['sms']}, upsert=True)
        db.loandueoverdue.update({"cust_id": int(user_id)},
                                 {"cust_id": int(user_id), 'timestamp': data_over_due['timestamp'],
                                  'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                  "sms": data_over_due['sms']}, upsert=True)
        logger.info("All loan messages of new user inserted successfully")
    else:

        for i in range(len(data_approve['sms'])):
            logger.info("Old User checked")
            db.loanapproval.update({"cust_id": int(user_id)}, {"$push": {"sms": data_approve['sms'][i]}})
            logger.info("loan approval sms of old user updated successfully")
        db.loanapproval.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                   upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_reject['sms'])):
            logger.info("Old User checked")
            db.loanrejection.update({"cust_id": int(user_id)}, {"$push": {"sms": data_reject['sms'][i]}})
            logger.info("loan rejection sms of old user updated successfully")
        db.loanrejection.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                    upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_disburse['sms'])):
            logger.info("Old User checked")
            db.disbursed.update({"cust_id": int(user_id)}, {"$push": {"sms": data_disburse['sms'][i]}})
            logger.info("loan disbursed sms of old user updated successfully")
        db.disbursed.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_over_due['sms'])):
            logger.info("Old User checked")
            db.loandueoverdue.update({"cust_id": int(user_id)}, {"$push": {"sms": data_over_due['sms'][i]}})
            logger.info("loan due overdue sms of old user updated successfully")
        db.loandueoverdue.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                     upsert=True)
        logger.info("Timestamp of User updated")
        for i in range(len(data_closed['sms'])):
            logger.info("Old User checked")
            db.loanclosed.update({"cust_id": int(user_id)}, {"$push": {"sms": data_closed['sms'][i]}})
            logger.info("loan closed sms of old user updated successfully")
        db.loanclosed.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                 upsert=True)
        logger.info("Timestamp of User updated")
    client.close()