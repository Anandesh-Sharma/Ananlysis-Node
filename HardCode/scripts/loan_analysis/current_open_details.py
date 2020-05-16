from HardCode.scripts.Util import logger_1, conn
from HardCode.scripts.loan_analysis.my_modules import sms_header_splitter, grouping
from datetime import datetime
import pandas as pd

def get_current_open_details(cust_id):
    closed_dates = {}
    due_overdue_dates = {}
    count = 0
    report = {"message":"","date":-1,"sender":""}
    try:
        client = conn()
        closed = client.messagecluster.loanclosed.find_one({"cust_id" : cust_id})
        due_overdue = client.messagecluster.loandueoverdue.find_one({"cust_id" : cust_id})

        closed_data = pd.DataFrame(closed['sms'])
        due_overdue_data = pd.DataFrame(due_overdue['sms'])
        #print(closed_data)
        #print(due_overdue_data)
        start_date = datetime.strptime("2020-03-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        try:
            temp1 = False
            closed_data = sms_header_splitter(closed_data)
            closed_data_grouped = grouping(closed_data)

            for app, data in closed_data_grouped:
                date = datetime.strptime(str(data['timestamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
                report["date"] = data['timestamp'].iloc[-1]
                report["message"] = data['body'].iloc[-1]
                report["sender"] = data['Sender-Name'].iloc[-1]
                if date > start_date:
                    closed_dates[app] = date
                    break
                else:
                    closed_dates[app] = 0
        except:
            temp1 = True
        try:
            temp2 = False
            due_overdue_data = sms_header_splitter(due_overdue_data)
            due_overdue_data_grouped = grouping(due_overdue_data)
            for app, data in due_overdue_data_grouped:
                date = datetime.strptime(str(data['timestamp'].iloc[-1]), "%Y-%m-%d %H:%M:%S")
                report["date"] = data['timestamp'].iloc[-1]
                report["message"] = data['body'].iloc[-1]
                report["sender"] = data['Sender-Name'].iloc[-1]
                if date > start_date:
                    due_overdue_dates[app] = date
                else:
                    due_overdue_dates[app] = 0
        except:
            temp2 = True
        for i in list(closed_dates.keys()):
            try:
                if i in due_overdue_dates.keys():
                    if closed_dates[i] < due_overdue_dates[i]:
                        count += 1
                    else:
                        pass
                    overdue_apps = list(due_overdue_dates.keys())
                    closed_apps = list(closed_dates.keys())
                    for i in overdue_apps:
                        if not i in closed_apps:
                            count += 1
                #print(len(closed_dates.keys()))
                #print(len(due_overdue_dates.keys()))

            except BaseException as e:
                r = {'status': False, 'message': str(e),
                    'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
                client.analysisresult.exception_bl0.insert_one(r)
                print(e)
        if temp1 and temp2:
            pass
        try:
            db = client.analysis.testing
            db.update({"cust_id" : cust_id}, {"$set" : report}, upsert = True)
        except BaseException as e:
            r = {'status': False, 'message': str(e),
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
            client.analysisresult.exception_bl0.insert_one(r)
            print(e)
    except BaseException as e:
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
        client.analysisresult.exception_bl0.insert_one(r)
        print(e)
    finally:
        client.close()
        return count