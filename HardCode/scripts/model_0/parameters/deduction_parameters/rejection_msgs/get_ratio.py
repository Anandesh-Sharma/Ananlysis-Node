import numpy as np
import pandas as pd
from HardCode.scripts.model_0.parameters.deduction_parameters.rejection_msgs.total_rejection_msg import get_defaulter
from HardCode.scripts.Util import conn
from HardCode.scripts.loan_analysis.my_modules import is_overdue


def legal_messages_count_ratio(user_id,sms_count):
    ratio = -1
    legal_messages_count = -1
    try:
        connection = conn()
        # user_sms_count = connection.analysisresult.bl0.find_one({'cust_id':user_id})
        # user_sms_count = user_sms_count['result'][-1]
        # user_sms_count = user_sms_count['sms_count']
        user_sms_count = sms_count


        defaulter, legal_messages_count = get_defaulter(user_id)
        ratio = np.round(legal_messages_count/user_sms_count, 2)

        return ratio,legal_messages_count
    except Exception as e:
        return ratio,legal_messages_count


def overdue_count_ratio(user_id,sms_count):
    ratio = -1
    overdue_count = 0
    try:
        connection = conn()
        # user_sms_count = connection.analysisresult.bl0.find_one({'cust_id': user_id})
        # user_sms_count = user_sms_count['result'][-1]
        # user_sms_count = user_sms_count['sms_count']
        user_sms_count = sms_count
        due_overdue_messages = connection.messagecluster.loandueoverdue.find_one({'cust_id': user_id})
        if due_overdue_messages:
            due_overdue_messages = pd.DataFrame(due_overdue_messages['sms'])
        else:
            return ratio,overdue_count
        if not due_overdue_messages.empty:
            for i in range(due_overdue_messages.shape[0]):
                message = str(due_overdue_messages['body'][i]).lower()
                if is_overdue(message):
                    overdue_count +=1

        ratio = np.round(overdue_count/user_sms_count, 2)
        return ratio,overdue_count
    except Exception as e:

        return ratio,overdue_count


    
    
