import re
import pandas as pd
from HardCode.scripts.Util import conn
from datetime import datetime
import pytz


def get_defaulter(user_id):
    legal_message_count = 0
    connect = conn()
    try:
        legal = connect.messagecluster.legal_msgs.find_one({'cust_id':user_id})
        legal_messages = legal['sms']
        legal_message_count = len(legal_messages)
        connect.close()
        return legal_message_count
    except BaseException as e:
        connect.analysisresult.exception_bl0.insert_one({'status': False, 'message': "error in legal-"+str(e), 
                                                         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id})
        connect.close()
        return legal_message_count
