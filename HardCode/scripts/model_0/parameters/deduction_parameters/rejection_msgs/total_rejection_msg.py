import re
import pandas as pd
from HardCode.scripts.Util import conn


def get_defaulter(user_id):
    FLAG = False
    legal_message_count = 0
    connect = conn()
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
        return FLAG, legal_message_count

    patterns = [
        r'legal\snotice\salert.*(?:loan|emi)\samount.*overdue.*since\s([0-9]{1,2})\sday[s]?',
        r'action\srequired.*pending.*\s([0-9]+)\s?day[s]?',
        r'urgent\sattention.*(?:overdue|delinquent).*\s([0-9]+)\sday[s]?',
        r'due\ssince\s([0-9]+)\sday[s]?.*immediately',
        r'your\s(?:loan|emi).*is\soverdue\sseriously',
        r'(?:loan|emi|repayment|bill|amount|payment|amt\.?|paymt\.?).*overdue\s(?:for|by|since)\s([0-9]+)\s?days',
        r'your\sloan\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\sis\soverdue\sby\s([0-9]+)\s?days',
        r'legal\snotice\salert',
        r'recovery\s(?:process|proceedings).*(?:started|begun)',
        r'legal\snotice.*going\sto\sbe\sdispatched',
        r'address.*shared.*legal\sdepartment.*legal\snotice.*',
        r'take.*serious\saction.*profile.*cibil.*already\simpacted.*',
        r'dispatch.*legal\snotice',
        r'sent.*legal\snotice',
        r'(?:despite|after)\s(?:several|multiple|many|repeated)\sreminders.*over[-]?\s?due\s(?:for|by|since)\s([0-9]+)\s?days',
        r'(?:despite|after)\s(?:several|multiple|many|repeated)\sreminders',
        r'could\snot\sapprove[d]?.*please\sre[-]?apply',
        r'final\sintimation.*account.*seriously.*due.*still.*not\sreceived[d]?.*payment',
        r'final\s(?:warning|reminder)',
        r'legal\scourt\scase.*legal\scase\sagainst\syou',
        r'final\sintimation.*final\snotice',
        r'legal\snotice.*contemplated.*avoid.*legal\saction',
        r'overdue.*([0-9]+)\sday[s]?.*legal.*notice',
        r'legal\saction.*attention.*required',
        r'legal\sdepartment.*legal\snotice',
        r'legal\scounsel.*legal\sproceedings',
        # r'legal\simplications.*(\savoid)?legal\saction',
        # r'loan.*due\ssince\s(\d{4}-\d{2}-\d{2}).*legal\s(?:actions|notification)',
        r'legal\scourt\scase.*legal\saction[s]?',
        r'(?:loan|emi|bill|payment|amount).*pending\s(?:for|since|by)\s([0-9]+)\s?days.*recovery\sprocess.*demand\snotice.*dispatched',
        r'visit\sinitiated.*(?:loan|emi|bill).*pending',
        r'last\schance.*overdue.*urgent[l]?[y]?',
        r'(?:initiating|initiated)\slegal\saction',
        r'legal\scase\sfiled\sagainst\syou',
        r'unpaid\sdues.*loan\sfacility.*cancelled',
        r'your\saccount\sis\sseverely\sdelinquent',
        r'your\sloan\s(?:has\sbeen|is)\s(?:moved|transferred)\sto\slegal\sbucket',
        r'filing.*police\scomplaint',
        r'overdue.*(?:paid|pay).*immediately',
        r'(?:exceeded|overdued)\s(?:your)?.*(?:emi|installment|loan)\s(?:payment|repayment).*([0-9]+)\s?days',
        r'pay.*before.*tagged\sas\s(?:defaulter|defaulted)'
    ]
    for i in range(total.shape[0]):
        message = str(total['body'][i].encode('utf-8')).lower()

        for pattern in patterns:
            matcher = re.search(pattern, message)
            if pattern is patterns[0] or pattern is patterns[1] or pattern is patterns[2] or pattern is patterns[3] or pattern is patterns[4] or pattern is patterns[5] or \
                pattern is patterns[6] or pattern is patterns[14] or pattern is patterns[37]:
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
    return FLAG, legal_message_count
