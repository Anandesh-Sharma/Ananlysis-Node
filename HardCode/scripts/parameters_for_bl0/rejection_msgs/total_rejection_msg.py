import re
import pandas as pd
from HardCode.scripts.Util import conn
from datetime import  datetime
import  pytz


def get_defaulter(user_id):
    FLAG = False
    legal_message_count = 0
    connect = conn()
    db = connect.analysis.parameters
    parameters = {}
    loan_approval = connect.messagecluster.loanapproval.find_one({'cust_id': user_id})
    loan_reject = connect.messagecluster.loanrejection.find_one({'cust_id': user_id})
    loan_overdue = connect.messagecluster.loandueoverdue.find_one({'cust_id': user_id})
    if loan_approval:
        loan_approval = loan_approval['sms']
    if loan_reject:
        loan_reject = loan_reject['sms']
    if loan_overdue:
        loan_overdue = loan_overdue['sms']
    if loan_overdue or loan_reject or loan_approval:
        total = loan_approval + loan_overdue + loan_reject
        total = pd.DataFrame(total)
    else:
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.legal_message_count': legal_message_count,
                                                  'parameters.legal_message_status': FLAG}}, upsert=True)
        return {'status':True, 'message':'no loan data found'}

    patterns = [
        r'legal\snotice\salert.*(?:loan|emi)\samount.*overdue.*since\s([0-9]{1,2})\sday[s]?', #days [0]
        r'action\srequired.*pending.*\s([0-9]+)\s?day[s]?', #days [1]
        r'urgent\sattention.*(?:overdue|delinquent).*\s([0-9]+)\sday[s]?', #days [2]
        r'due\ssince\s([0-9]+)\sday[s]?.*immediately', #days [3]
        r'your\s(?:loan|emi).*is\soverdue\sseriously', #[4]
        r'(?:loan|emi|repayment|bill|amount|payment|amt\.?|paymt\.?).*overdue\s(?:for|by|since)\s([0-9]+)\s?days', #days [5]
        r'your\sloan\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\sis\soverdue\sby\s([0-9]+)\s?days', #days [6]
        r'legal\snotice\salert', #[7]
        r'recovery\s(?:process|proceedings).*(?:started|begun)', #[8]
        r'legal\snotice.*going\sto\sbe\sdispatched', #[9]
        r'address.*shared.*legal\sdepartment.*legal\snotice.*', #[10]
        r'take.*serious\saction.*profile.*cibil.*already\simpacted.*', #[11]
        r'dispatch.*legal\snotice', #[12]
        r'sent.*legal\snotice', #[13]
        r'(?:despite|after)\s(?:several|multiple|many|repeated)\sreminders.*over[-]?\s?due\s(?:for|by|since)\s([0-9]+)\s?days', #days [14]
        r'(?:despite|after)\s(?:several|multiple|many|repeated)\sreminders', #[15]
        r'could\snot\sapprove[d]?.*please\sre[-]?apply', #[16]
        r'final\sintimation.*account.*seriously.*due.*still.*not\sreceived[d]?.*payment', #[17]
        r'final\s(?:warning|reminder)', #[18]
        r'legal\scourt\scase.*legal\scase\sagainst\syou', #[19]
        r'final\sintimation.*final\snotice', #[20]
        r'legal\snotice.*contemplated.*avoid.*legal\saction', #[21]
        r'overdue.*([0-9]+)\sday[s]?.*legal.*notice', #days [22]
        r'legal\saction.*attention.*required', #[23]
        r'legal\sdepartment.*legal\snotice', #[24]
        r'legal\scounsel.*legal\sproceedings', #[25]
        # r'legal\simplications.*(\savoid)?legal\saction',
        # r'loan.*due\ssince\s(\d{4}-\d{2}-\d{2}).*legal\s(?:actions|notification)',
        r'legal\scourt\scase.*legal\saction[s]?', #[26]
        r'(?:loan|emi|bill|payment|amount).*pending\s(?:for|since|by)\s([0-9]+)\s?days.*recovery\sprocess.*demand\snotice.*dispatched', #days [27]
        r'visit\sinitiated.*(?:loan|emi|bill).*pending', #[28]
        r'last\schance.*overdue.*urgent[l]?[y]?', #[29]
        r'(?:initiating|initiated)\slegal\saction', #[30]
        r'legal\scase\sfiled\sagainst\syou', #[31]
        r'unpaid\sdues.*loan\sfacility.*cancelled', #[32]
        r'your\saccount\sis\sseverely\sdelinquent', #[33]
        r'your\sloan\s(?:has\sbeen|is)\s(?:moved|transferred)\sto\slegal\sbucket', #[34]
        r'filing.*police\scomplaint', #[35]
        r'overdue.*(?:paid|pay).*immediately',#[36]
        r'(?:exceeded|overdued)\s(?:your)?.*(?:emi|installment|loan)\s(?:payment|repayment).*([0-9]+)\s?days', #days [37]
        r'pay.*before.*tagged\sas\s(?:defaulter|defaulted)' #[38]
    ]
    try:
        for i in range(total.shape[0]):
            message = str(total['body'][i].encode('utf-8')).lower()

            for pattern in patterns:
                matcher = re.search(pattern, message)
                if pattern is patterns[0] or pattern is patterns[1] or pattern is patterns[2] or pattern is patterns[3] or pattern is patterns[5] or \
                    pattern is patterns[6] or pattern is patterns[14] or pattern is patterns[22] or pattern is patterns[27] or pattern is patterns[37]:
                    if matcher is not None:
                        if int(matcher.group(1)) > 15:
                            FLAG = True
                            # legal_message_count += 1
                            break
                        break
                if matcher is not None:
                    FLAG = True
                    legal_message_count += 1
                    break
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.legal_message_count': legal_message_count,
                                                  'parameters.legal_message_status': FLAG}}, upsert=True)
        return {'status':True, 'message':'success'}
    except BaseException as e:
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.legal_message_count': legal_message_count,
                                                  'parameters.legal_message_status': FLAG}}, upsert=True)
        return {'status': False, 'message': str(e)}
