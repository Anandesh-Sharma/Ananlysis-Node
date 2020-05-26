import re
import pandas as pd
from HardCode.scripts.Util import conn
from datetime import datetime
import pytz


def get_defaulter(user_id):
    legal_message_count = 0
    connect = conn()
    legal_messages = []
    loan_approval = connect.messagecluster.loanapproval.find_one(
        {'cust_id': user_id})
    loan_reject = connect.messagecluster.loanrejection.find_one(
        {'cust_id': user_id})
    loan_overdue = connect.messagecluster.loandueoverdue.find_one(
        {'cust_id': user_id})
    extra = connect.messagecluster.extra.find_one({'cust_id': user_id})
    if loan_approval:
        loan_approval = loan_approval['sms']
    if loan_reject:
        loan_reject = loan_reject['sms']
    if loan_overdue:
        loan_overdue = loan_overdue['sms']
    if extra:
        extra = extra['sms']
    if loan_overdue or loan_reject or loan_approval or extra:
        total = loan_approval + loan_overdue + loan_reject + extra
        total = pd.DataFrame(total)
    else:
        return legal_message_count

    patterns = [
        r'you\shave\sdefaulted.*legal', 
        r'forced.*to\sseek\slegal\scounsel', 
        r'receive\slegal\s(?:notice|notifications)\s(?:at|in)\syour\sresidence', 
        r'serious\snon\s?[-]?payment\sissues.*legal\saction', 
        r'legal\simplications.*seriously\sin\sarrears', 
        r'initiating\slegal\saction:\n\n.*served\swith.*legal\sdemand\snotice', 
        r'sending\srecovery\steam\sto\syour\splace', 
        r'legal\snotice.*going\sto\sbe\sdispatched', 
        r'(?:taking|take|initiate|initiating)\slegal\saction\sagainst\syou', 
        r'on\sthe\sverge\sof\staking\slegal\saction\sagainst\syou', 
        r'loan.*is\s(?:seriously|severely)\soverdue', 
        r'loan.*is\soverdue\sseriously', 
        r'recovery\sprocess.*started.*overdue\sloan', 
        r'legal\sintimation.*sent\son\syour\smail ', 
        r'taking.*legal\saction\saccording\sby\slaw', 
        r'you\sare\sintentionally\sdefaulting', 
        r'case\sdetails\scopy.*couriered\sto\syou', 
        r'we\sare\sabout\sto\sarrange.*field\srecovery\sagent', 
        r'account.*listed\sas\spart\sof\sdefault[e]?[d]?\sbucket', 
        r'arranged.*field\srecovery\s(?:agent|agency)', 
        r'started.*fraud\sinvestigation\s(?:and|&)\slegal\sproceeding[s]?', 
        r'legal\snotice\s(?:is|has\sbeen)\sprepared', 
        r'legal\snotice\sis\sgoing\sto\sbe\sdispatched', 
        r'collection\s(?:agencies|agency)\s(?:have|has)\s(?:received|recd)\s(?:your|ur)\scase', 
        r'your\scase.*(?:forwarded|escalated).*for\scollection\sof.*loan', 
        r'you\shave\sbeen\sin\sarrears\swith.*loan\sfor\sa\slong\stime', 
        r'started\slegal\sproceedings.*f\.?i\.?r\sis\sfiled', 
        r'referred.*to\san\sagent\sto\scontinue\sproceedings', 
        r'violated.*loan\scontract\sagreement.*malicious\sdefault', 
        r'asset\smanagement\sdepartment\swill\sask.*contacts.*default\sreasons', 
        r'no\sother\schoice.*undertake\slegal\saction', 
        r'initiated\slegal\saction.*report.*all\scredit\sbureaus', 
        r'situation.*critical.*requires\sserious\sattention', 
        r'filing\slegal\scase\sagainst\syou.*cheating.*dishonesty', 
        r'intimate\syour\sparents.*dispatch.*legal\snotice', 
        r'legal\snotice.*already\sregistered\son\syour\sname', 
        r'address.*shared\swith.*legal\sdepartment.*legal\snotice\salong\swith.*case\snumber', 
        r'your\saccount\sis\sseverely\sdelinquent', 
        r'loan\sis\snow\ssuspected\sfraud', 
        r'filing.*police\scomplaint\sagainst\syou'
    ]
    try:
        for _, row in total.iterrows():
            message = row['body'].lower()
            for pattern in patterns:
                matcher = re.search(pattern, message)
                if matcher is not None:
                    legal_messages.append(dict(row))
                    break
        legal_message_count = len(legal_messages)
        connect.analysis.legal_msg.update_one({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                                                'legal_msg': legal_messages}}, upsert = True)
        connect.close()
        return legal_message_count
    except BaseException as e:
        connect.analysisresult.exception_bl0.insert_one({'status': False, 'message': "error in legal-"+str(e), 
                                                         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id})
        connect.close()
        return legal_message_count
