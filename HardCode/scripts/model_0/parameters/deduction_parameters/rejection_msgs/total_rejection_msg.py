import re
import pandas as pd
from tqdm import tqdm
from HardCode.scripts.Util import conn

def get_defaulter(user_id):

    connect = conn()
    loan_approval = connect.messagecluster.loanapproval.find_one({'cust_id':user_id})
    loan_reject = connect.messagecluster.loanrejection.find_one({'cust_id': user_id})
    loan_overdue = connect.messagecluster.loandueoverdue.find_one({'cust_id': user_id})
    loan_approval = loan_approval['sms']
    loan_reject = loan_reject['sms']
    loan_overdue = loan_overdue['sms']
    total = loan_approval + loan_overdue + loan_reject
    total = pd.DataFrame(total)



    FLAG = False
    patterns = [
        r'legal\snotice\salert.*loan\samount.*overdue.*since\s([0-9]{1,2})\sday[s]?',
        r'legal\snotice\salert',
        r'legal\snotice.*going\sto\sbe\sdispatched',
        r'address.*shared.*legal\sdepartment.*legal\snotice.*',
        r'take.*serious\saction.*profile.*cibil.*already\simpacted.*',
        r'dispatch.*legal\snotice',
        r'sent.*legal\snotice',
        r'despite\sseveral\sreminders.*over[-]?\s?due.*legal\saction',
        r'could\snot\sapprove[d]?.*please\sre[-]?apply'
    ]

    for i in range(total.shape[0]):
        message = str(total['body'][i].encode('utf-8')).lower()
        
        for pattern in patterns:
            matcher = re.search(pattern, message)
            if pattern is patterns[0]:
                if matcher is not None:
                    if int(matcher.group(1)) > 15:
                        FLAG = True
                        break
                    break
                break
            if matcher is not None:
                FLAG = True
                break
    return FLAG 
            