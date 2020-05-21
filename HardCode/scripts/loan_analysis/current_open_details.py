from HardCode.scripts.Util import logger_1, conn
from HardCode.scripts.loan_analysis.my_modules import sms_header_splitter, grouping
from datetime import datetime
import pandas as pd
import pytz

def get_current_open_details(cust_id):
    count = 0
    report = {"current_open_details": -1}
    try:
        client = conn()
        parameters = client.analysis.parameters.find_one({"cust_id" : cust_id})
        last_loan_details = parameters["parameters"]["loan_details"]["last_loan_details"]
        for i in range(len(last_loan_details["app"])):
            if last_loan_details["category"][i] == 1:
                count += 1
            #report["message"] = last_loan_details["message"][i]
            #report["date"] = last_loan_details["date"][i]
            #report["sender"] = last_loan_details["app"][i]
        report["current_open_details"] = count
        try:
            db = client.analysis.loan
            db.update({"cust_id" : cust_id}, {"$set" : report}, upsert = True)
        except BaseException as e:
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
            client.analysisresult.exception_bl0.insert_one(r)
            print(e)
    except BaseException as e:
        print("error in current open")
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
        client.analysisresult.exception_bl0.insert_one(r)
        import traceback
        traceback.print_tb(e.__traceback__)
        print(e)
    finally:
        client.close()
        return count
