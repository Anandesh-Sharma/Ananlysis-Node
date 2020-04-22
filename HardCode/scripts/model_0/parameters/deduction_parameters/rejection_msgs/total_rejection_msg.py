import re
import pandas as pd
from tqdm import tqdm
from HardCode.scripts.Util import conn

def get_defaulter(user_id):
    FLAG = False
    connect = conn()
    loan_approval = connect.messagecluster.loanapproval.find_one({'cust_id':user_id})
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
        return FLAG

    legal_message_count = 0
    patterns = [
        r'legal\snotice\salert.*loan\samount.*overdue.*since\s([0-9]{1,2})\sday[s]?',
        r'action\srequired.*pending.*\s([0-9]+)\s?day[s]?',
        r'urgent\sattention.*overdue.*\s([0-9]+)\sday[s]?',
        r'due\ssince\s([0-9]+)\sday[s]?.*immediately',
        r'your\s(?:loan|emi).*is\soverdue\sseriously',
        r'your\sloan\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\soverdue\sby\s([0-9]+)\s?days'
        r'legal\snotice\salert',
        r'legal\snotice.*going\sto\sbe\sdispatched',
        r'address.*shared.*legal\sdepartment.*legal\snotice.*',
        r'take.*serious\saction.*profile.*cibil.*already\simpacted.*',
        r'dispatch.*legal\snotice',
        r'sent.*legal\snotice',
        r'despite\sseveral\sreminders.*over[-]?\s?due.*legal\saction',
        r'could\snot\sapprove[d]?.*please\sre[-]?apply',
        r'final\sintimation.*account.*seriously.*due.*still.*not\sreceived[d]?.*payment',
        r'final\swarning',
        r'legal\scourt\scase.*legal\scase\sagainst\syou',
        r'final\sintimation.*final\snotice',
        r'legal\snotice.*contemplated.*avoid.*legal\saction',
        r'overdue.*([0-9]+)\sday[s]?.*legal.*notice',
        r'legal\saction.*attention.*required',
        r'legal\sdepartment.*legal\snotice',
        r'legal\scounsel.*legal\sproceedings',
        #r'legal\simplications.*(\savoid)?legal\saction',
        #r'loan.*due\ssince\s(\d{4}-\d{2}-\d{2}).*legal\s(?:actions|notification)',
        r'legal\scourt\scase.*legal\saction[s]?',
        #r'legal\scourt\scase.*legal\sactions'
    ]

    for i in range(total.shape[0]):
        message = str(total['body'][i].encode('utf-8')).lower()
        
        for pattern in patterns:
            matcher = re.search(pattern, message)
            if pattern is patterns[0] or pattern is patterns[1] or pattern is patterns[2] or pattern is pattern[3] or pattern is pattern[4] or pattern is pattern[5]:
                if matcher is not None:
                    if int(matcher.group(1)) > 15:
                        FLAG = True
                        #legal_message_count += 1
                        break
                    break
                break
            if matcher is not None:
                FLAG = True
                legal_message_count += 1
                break
    return FLAG , legal_message_count 
            