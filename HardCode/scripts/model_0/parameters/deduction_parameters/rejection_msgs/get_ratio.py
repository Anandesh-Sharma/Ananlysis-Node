import numpy as np
import pandas as pd
from total_rejection_msg import get_defaulter
from HardCode.scripts.Util import conn
from HardCode.scripts.loan_analysis import is_overdue


def legal_messages_ratio(user_id):
    ratio = -1
    try:
        connection = conn()
        #user_sms_count = fetch from mongo
    except:
        return ratio

    defaulter, legal_messages_count = get_defaulter(user_id)
    ratio = np.round(legal_messages_count/user_sms_count, 2)

    return ratio

def overdue_ratio(user_id):
    ratio = -1
    try:
        connection = conn()
        #user_sms_count = fetch from mongo
        due_overdue_messages = connection.messagecluster.loandueoverdue.find_one({'cust_id': user_id})
        due_overdue_messages = pd.DataFrame(due_overdue_messages['sms'])
    return ratio

    overdue_count = 0
    for i in range(due_overdue_messages.shape[0]):
        message = str(due_overdue_messages['body'].encode('utf-8')).lower()
        if is_overdue(message):
            overdue_count +=1
    
    ratio = np.round(overdue_count/user_sms_count, 2)
    return ratio


    
    
