import re
from HardCode.scripts.Util import conn, convert_json, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def get_loan_closed_messages(data, loan_messages_filtered, result, name):
    logger = logger_1("loan closed messages", name)
    selected_rows = []

    pattern_1 = '.*?loan.*?closed.*?'
    pattern_2 = '.*?closed.*?successfully.*?'
    pattern_3 = 'successfully\sreceived\spayment.*rs\.\s[0-9]{3,6}'
    pattern_4 = 'loan.*?paid\sback'
    pattern_5 = 'making\spayment.*?home\scredit\sloan'
    pattern_6 = 'bhugta+n\skarne\ske\sliye\sdhanya?wad'
    pattern_7 = 'payment.*?was\ssuccessful'
    pattern_8 = 'payment\sof.*?received.*?loan'
    pattern_9 = 'payment\sof.*?agreement.*?received'
    pattern_10 = 'received.*?payment\s(of|rs).*?loan'
    pattern_11 = 'rcvd\spayment\s(of|rs).*?loan'
    pattern_12 = 'thank\syou\sfor\spayment.*?towards.*?loan'
    pattern_13 = 'acknowledge\sreceipt.*?emi\sof.*?(against|towards).*?loan'
    pattern_14 = 'your\sfirst\sloan.*?paid\ssuccessfully'
    pattern_15 = 'you\sjust\spaid.*?towards\sloan'
    pattern_16 = 'thanks\sfor\spayment.*?for\sloan'
    pattern_17 = 'payment\sreceived\sfor.*?loan'
    pattern_18 = 'received.*\n\n.*towards\syour\sloan'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue

        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13, message)
        matcher_14 = re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)
        matcher_16 = re.search(pattern_16, message)
        matcher_17 = re.search(pattern_17, message)
        matcher_18 = re.search(pattern_18, message)

        if matcher_1 or matcher_2 or matcher_3 or matcher_4 or matcher_5 or matcher_6 or matcher_7 or matcher_8 or matcher_9 or matcher_10 or matcher_11 or matcher_12 or matcher_13 or matcher_14 or matcher_15 or matcher_16 or matcher_17 or matcher_18:
            selected_rows.append(i)
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
    return data.copy()[mask].reset_index(drop=True)


def replace_parenthesis(message):
    # this was done as in some cases cmount was written in a bracket due to which garbage value was detected by the regex
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
    pattern_1 = 'loan'
    pattern_2 = 'kreditbee'
    pattern_3 = 'cashbean'
    pattern_4 = 'loanfront'
    pattern_5 = 'loanapp'
    pattern_6 = 'kissht'
    pattern_7 = 'gotocash'
    pattern_8 = 'cashmama'

    header = ['kredtb', 'idfcfb', 'cashbn', 'lnfrnt', 'cshmma', 'kredtz', 'rrloan',
              'frloan', 'wfcash', 'bajajf', 'flasho', 'kissht', 'gtcash', 'bajafn', 'monvew', 'mpockt',
              'mpokkt', 'montap', 'mnytap']

    data['body'] = data['body'].apply(lambda m: replace_parenthesis(m))
    for i in range(data.shape[0]):
        message = str(data['body'][i]).lower()
        head = str(data['sender'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)

        if matcher_1 or matcher_2 or matcher_3 or matcher_4 or matcher_5 or matcher_6 or matcher_7 \
                or matcher_8 or head[2:] in header or head[3:] in header:
            loan_messages.append(i)
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

    pattern_1 = 'kyc'
    pattern_2 = r'sbi\scard'
    pattern_3 = r'credited\sto\syour\swallet'
    pattern_4 = r'spice\smoney'
    pattern_5 = r'sign\sthe\selectronic\scontract'
    pattern_6 = r'golden\slightning'
    pattern_7 = r'gold\scash'
    pattern_8 = r'after\syour\sconfirmation'
    pattern_9 = r'your\spersonalised\sloan\soffer'
    pattern_10 = r'emi\scard|credit\scard'
    pattern_11 = r'\se-?sign\s'
    pattern_12 = r'claim\sbonus'
    pattern_13 = 'icredit|rupeeplus'
    pattern_14 = r'good\snews'
    pattern_15 = r'confirm\snow'

    for i in range(data.shape[0]):
        if i not in loan_messages:
            continue
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13, message)
        matcher_14 = re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)

        if matcher_1 or matcher_2 or matcher_3 or matcher_4 or matcher_4 or matcher_5 or matcher_6 or matcher_7\
                or matcher_8 or matcher_9 or matcher_10 or matcher_11 or matcher_12 or matcher_13 or matcher_14\
                or matcher_15:
            pass
        else:
            loan_messages_filtered.append(i)

    return loan_messages_filtered


def get_approval(data, loan_messages_filtered, result, name):
    logger = logger_1("loan approval", name)
    selected_rows = []
    pattern_1 = r'successfully\sapproved'
    pattern_2 = r'has\sbeen\sapproved'
    pattern_3 = r'documents\shas\sbeen\ssuccessfully\sverified'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)

        if matcher_1 or matcher_2 or matcher_3:
            selected_rows.append(i)
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
    return data.copy()[mask].reset_index(drop=True)


def get_disbursed(data, loan_messages_filtered, result, name):
    logger = logger_1("loan disbursed", name)
    selected_rows = []
    pattern_1 = r'has\sbeen\sdisburse[d]?'
    pattern_2 = r'disbursement\shas\sbeen\scredited'
    pattern_3 = r'has\sbeen\stransferred.*account'
    pattern_4 = r'disburse?ment.*has\sbeen\sinitiated'
    pattern_5 = r'is\stransferred.*account'
    pattern_6 = r'loan\sapplication.*?successfully\ssubmitted.*?bank'
    pattern_7 = r'you.*?received.*?loan\samount'
    pattern_8 = r'your.*?loan.*?made\ssuccessfully.*?loan\samount'
    pattern_9 = r'loan\srs.*?disbursed'
    pattern_10 = r'loan\shas\sbeen\sreleased\sto.*?bank'
    pattern_11 = r'rs.*?credited\sto\sloan\sa\/c'
    pattern_12 = r'disbursed.*?loan.*?to\syour\sbank'
    pattern_13 = r'amount\sfinanced\sfor\sloan.*?rs'
    pattern_14 = r'personal\sloan.*?transferred\ssuccessfully'
    pattern_15 = r'your\sloan\sdisbursement\swas\ssuccess'
    pattern_16 = r'loan.*?approved.*?cash.*?issued\sto.*?bank\saccount'
    pattern_17 = r'loan.*?approved.*?will\stransfer.*?funds.*bank\saccount'
    pattern_18 = r'loan.*approved.*fund[s]?\swill\sbe\stransferred.*bank\saccount'
    pattern_19 = r'loan.*approved.*disbursed\sinto\syour\saccount'
    pattern_20 = r'loan.*approved.*will\srelease.*loan\sto.*account'
    pattern_21 = r'loan.*approved.*sent\srs.*to\syour\saccount'
    pattern_22 = r'loan.*approved.*will\sbe\sdisbursed'
    pattern_23 = r'loan.*approved.*credit\sto.*?bank\saccount'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13, message)
        matcher_14 = re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)
        matcher_16 = re.search(pattern_16, message)
        matcher_17 = re.search(pattern_17, message)
        matcher_18 = re.search(pattern_18, message)
        matcher_19 = re.search(pattern_19, message)
        matcher_20 = re.search(pattern_20, message)
        matcher_21 = re.search(pattern_21, message)
        matcher_22 = re.search(pattern_22, message)
        matcher_23 = re.search(pattern_23, message)

        if matcher_1 or matcher_2 or matcher_3 or matcher_4 or matcher_5 \
                or matcher_6 or matcher_7 or matcher_8 or matcher_9 or matcher_10 \
                or matcher_11 or matcher_12 or matcher_13 or matcher_14 or matcher_15 \
                or matcher_16 or matcher_17 or matcher_18 or matcher_19 or matcher_20 \
                or matcher_21 or matcher_22 or matcher_23:
            selected_rows.append(i)
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
    return data.copy()[mask].reset_index(drop=True)


def get_loan_rejected_messages(data, loan_messages_filtered, result, name):
    logger = logger_1("loan rejection", name)
    selected_rows = []

    pattern_1 = r'loan\sapplication.*?is\srejected'
    pattern_2 = r'loan\sapplication.*?got\srejected'
    pattern_3 = r'loan.*paysense\sis\srejected'
    pattern_4 = r'has\sbeen.*?rejected'
    pattern_5 = r'is\sdeclined'
    pattern_6 = r'has\sbeen\sdeclined'
    pattern_7 = r'has\snot\sbeen\sapproved'
    pattern_8 = r'was\snot\sapproved'
    pattern_9 = r'was\sbeen\srejected'
    pattern_10 = r'could\snot\sget\sapproved'
    pattern_11 = r"sorry.*?couldn't.*?eligible.*?loan"
    pattern_12 = r'credit.*rejected.*loan.*application'
    pattern_13 = r'low\scibil\sscore'
    pattern_14 = r'low\scredit\sscore'
    pattern_15 = r'is\sdue'
    pattern_16 = r'network\scard'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()

        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13, message)
        matcher_14 = re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)
        matcher_16 = re.search(pattern_16, message)

        if matcher_1 or matcher_2 or matcher_3 or matcher_4 or matcher_5 or matcher_6 or matcher_7 or matcher_8 or matcher_9 or matcher_10 or matcher_11 or matcher_12 :
            if matcher_13 is None or matcher_14 is None or matcher_15 is None or matcher_16 is None:
                selected_rows.append(i)
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
    return data.copy()[mask].reset_index(drop=True)


def get_over_due(data, loan_messages_filtered, result, name):
    logger = logger_1("loan due overdue", name)
    selected_rows = []
    # pattern_1 = r'(.*)?immediate(.*)payment(.*)'
    # pattern_2 = r'(.*)?delinquent(.*)?'
    # # pattern_3 = r'(.*)?has(.*)?bounced(.*)?'
    # pattern_4 = r'missed(.*)?payments'
    # pattern_5 = r'(.*)?due(.*)?'
    # pattern_6 = r'\sover-?due\

    pattern_1 = r'immediate\spayment'
    pattern_2 = r'delinquent'
    pattern_3 = r'missed\spayments'
    pattern_4 = r'is\sdue'
    pattern_5 = r'is\soverdue'
    pattern_6 = r'loan\sis\sdue'
    pattern_7 = r'due\sdate'
    pattern_8 = r'due\shai'
    pattern_9 = r'overdue\shai'
    pattern_10 = r'\sover-?due'
    pattern_11 = r'loan\sis\son\sdue'
    pattern_12 = r'emi\sof.*?was\sdue\son'
    pattern_13 = r'loan.*?emi\srs.*?due\son'
    pattern_14 = r'payment\sof.*?against.*?loan.*?bounced'
    pattern_15 = r"you\sstill\shaven['o]t\spaid.*?loan"
    pattern_16 = r'you\shave\smissed.*?payment.*?loan'
    pattern_17 = r'loan\sof\srs.*?disbursed\sfrom.*?a\/c'
    pattern_18 = r'repay\syour\semi\samount\sdue\son'
    pattern_19 = r'loan.*?borrowed.*?will\sbe\sdue\son'
    pattern_20 = r'k[io]\sdey\shai'
    pattern_21 = r'pay.*?loan\sof\srs.*?by'
    pattern_22 = r'charges\sof\srs.*?in\syour\sloan'
    pattern_23 = r'your\srepayment\sdate.*?repayment\samount'
    pattern_24 = r'loan.*?bakaya\shai'
    pattern_25 = r'earlysalary.*?requested\spayment'
    pattern_26 = r'missed\syour\spayment\son.*?loan'
    pattern_27 = r'not\spaid\semi.*?against\syour\sloan'
    pattern_28 = r'emi.*?dhani\sloan\sis\sstill\sdue'
    pattern_29 = r'last\sday\sfor.*?loan\srepayment'
    pattern_30 = r'payment\sis\sdelayed\sby\sweeks'
    pattern_31 = r'do\snot\sforget\sto\spay.*?loan'
    pattern_32 = r'your\sloan\sis\sstill\sunpaid'
    pattern_33 = r'promised\sto\spay\srs.*?loan'
    pattern_34 = r'emi.*?will\sbe\s(auto-)?debited'
    pattern_35 = r'instalment\sis\sunpaid'
    pattern_36 = r'dues\sof\srs.*?outstanding.*?for\sloan'
    # overdue
    pattern_37 = r'.*loan.*overdue.*repayable\sis\srs.\s?([0-9]+)'
    pattern_38 = r'.*loan.*rs\.\s([0-9]+).*overdue.*'
    pattern_39 = r'.*loan.*overdue.*repayment.*rs\.?\s([0-9]+)'

    # due
    pattern_40 = r'.*payment.*rs\.?.*?([0-9]+).*due.*'  # group(1) for amount
    pattern_41 = r'.*due.*on\s([0-9]+-[0-9]+?-[0-9]+).*payment.*rs\.?\s?([0-9]+)'  # group(1) for date and group(2) for amount
    pattern_42 = r'.*rs\.?\s([0-9]+).*due.*([0-9]+-[0-9]+-[0-9]+).*'  # group(1) for amount and group(2) for date
    pattern_43 = r'due\s(?:on)?.*([0-9]+/[0-9]+).*'  # group(1) for date in cashbn
    pattern_44 = r'.*loan.*rs\.?.*?([0-9]+).*due.*'  # group(1) for loan amount
    pattern_45 = r'.*payment.*due.*'

    for i in range(data.shape[0]):
        if i not in loan_messages_filtered:
            continue
        message = str(data['body'][i]).lower()
        matcher_1 = re.search(pattern_1, message)
        matcher_2 = re.search(pattern_2, message)
        matcher_3 = re.search(pattern_3, message)
        matcher_4 = re.search(pattern_4, message)
        matcher_5 = re.search(pattern_5, message)
        matcher_6 = re.search(pattern_6, message)
        matcher_7 = re.search(pattern_7, message)
        matcher_8 = re.search(pattern_8, message)
        matcher_9 = re.search(pattern_9, message)
        matcher_10 = re.search(pattern_10, message)
        matcher_11 = re.search(pattern_11, message)
        matcher_12 = re.search(pattern_12, message)
        matcher_13 = re.search(pattern_13, message)
        matcher_14 = re.search(pattern_14, message)
        matcher_15 = re.search(pattern_15, message)
        matcher_16 = re.search(pattern_16, message)
        matcher_17 = re.search(pattern_17, message)
        matcher_18 = re.search(pattern_18, message)
        matcher_19 = re.search(pattern_19, message)
        matcher_20 = re.search(pattern_20, message)
        matcher_21 = re.search(pattern_21, message)
        matcher_22 = re.search(pattern_22, message)
        matcher_23 = re.search(pattern_23, message)
        matcher_24 = re.search(pattern_24, message)
        matcher_25 = re.search(pattern_25, message)
        matcher_26 = re.search(pattern_26, message)
        matcher_27 = re.search(pattern_27, message)
        matcher_28 = re.search(pattern_28, message)
        matcher_29 = re.search(pattern_29, message)
        matcher_30 = re.search(pattern_30, message)
        matcher_31 = re.search(pattern_31, message)
        matcher_32 = re.search(pattern_32, message)
        matcher_33 = re.search(pattern_33, message)
        matcher_34 = re.search(pattern_34, message)
        matcher_35 = re.search(pattern_35, message)
        matcher_36 = re.search(pattern_36, message)
        matcher_37 = re.search(pattern_37, message)
        matcher_38 = re.search(pattern_38, message)
        matcher_39 = re.search(pattern_39, message)
        matcher_40 = re.search(pattern_40, message)
        matcher_41 = re.search(pattern_41, message)
        matcher_42 = re.search(pattern_42, message)
        matcher_43 = re.search(pattern_43, message)
        matcher_44 = re.search(pattern_44, message)
        matcher_45 = re.search(pattern_45, message)

        if matcher_1 or matcher_2 or matcher_3 or matcher_4 or matcher_5 or matcher_6 or matcher_7 or matcher_8 or matcher_9 or matcher_10 \
                or matcher_11 or matcher_12 or matcher_13 or matcher_14 or matcher_15 or matcher_16 or matcher_17 or matcher_18\
                or matcher_19 or matcher_20 or matcher_21 or matcher_22 or matcher_23 or matcher_24 or matcher_25 or matcher_26\
                or matcher_27 or matcher_28 or matcher_29 or matcher_30 or matcher_31 or matcher_32 or matcher_33 or matcher_34\
                or matcher_35 or matcher_36 or matcher_37 or matcher_38 or matcher_39 or matcher_40 or matcher_41 or matcher_42\
                or matcher_43 or matcher_44 or matcher_45:
            selected_rows.append(i)

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
    return data.copy()[mask].reset_index(drop=True)


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
