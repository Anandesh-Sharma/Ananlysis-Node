from HardCode.scripts.Util import logger_1, conn
from HardCode.scripts.loan_analysis.my_modules import sms_header_splitter, grouping
from HardCode.scripts.loan_analysis.last_loan_details import get_final_loan_details
from datetime import datetime
import pandas as pd
import pytz

def get_current_open_details(cust_id):
    count = 0
    report = {"current_open_details": -1}
    client = conn()
    try:
        status = True
        last_loan_details = get_final_loan_details(cust_id)
        for i in last_loan_details.keys():
            if last_loan_details[i]["category"]:
                count += 1
            #report["message"] = last_loan_details["message"][i]
            #report["date"] = last_loan_details["date"][i]
            #report["sender"] = last_loan_details["app"][i]
        report["current_open_details"] = count
        try:
            db = client.analysis.loan
            db.update({"cust_id" : cust_id}, {"$set" : report}, upsert = True)
            msg = 'success'
        except BaseException as e:
            msg = str(e)
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
            client.analysisresult.exception_bl0.insert_one(r)
            print(e)
    except BaseException as e:
        print("error in current open")
        msg = str(e)
        status = False
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
        client.analysisresult.exception_bl0.insert_one(r)

    finally:
        client.close()
        return {'status':status,'message':msg}
