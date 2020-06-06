from HardCode.scripts.loan_analysis.get_loan_data import fetch_user_data
from HardCode.scripts.Util import logger_1,conn
from HardCode.scripts.loan_analysis.my_modules import *
from HardCode.scripts.loan_analysis.loan_app_regex_superset import loan_apps_regex, bank_headers
from datetime import  datetime
import pytz
# from HardCode.scripts.loan_analysis.current_open_details import get_current_open_details
# from HardCode.scripts.loan_analysis.loan_rejection import get_rejection_count


def preprocessing(cust_id):
    loan_data = fetch_user_data(cust_id)
    logger = logger_1('preprocessing', cust_id)
    loan_data = sms_header_splitter(loan_data)
    logger.info("Data Splitted by headers")
    loan_data_grouped = grouping(loan_data)
    #print(loan_data)
    logger.info("Data Grouped by Sender-Name")
    loan_details_of_all_apps = {}
    user_app_list = []
    report = {}
    script_status = {}
    #print(loan_data['Sender-Name'].unique())
    try:
        for app, data in loan_data_grouped:
            logger.info("iteration in groups starts")
            user_app_list.append(app)
            app_name = app
            if app not in list(loan_apps_regex.keys()) and app not in bank_headers:
                app = 'OTHER'
            if app in list(loan_apps_regex.keys()):
                logger.info("app found in app list")
                data = data.sort_values(by='timestamp')
                data = data.reset_index(drop=True)
                loan_count = 0
                loan_details_individual_app = {}
                i = 0
                FLAG = False
                #check = False

                while i < len(data):
                    individual_loan_details = {
                        'disbursed_date': -1,
                        'closed_date': -1,
                        'loan_duration': -1,
                        'loan_disbursed_amount': -1,
                        'due_date' : -1,
                        'loan_due_amount': -1,
                        'loan_closed_amount' : -1,
                        'overdue_days' : -1,
                        'overdue_check' : 0,
                        'messages': []
                    }
                    msg1 = str(data['body'][i].encode('utf-8')).lower()
                    if data["category"][i] == "disbursed":
                        logger.info("disbursal message found")
                        disbursal_date = datetime.strptime(str(data['timestamp'][i]), "%Y-%m-%d %H:%M:%S")
                        individual_loan_details['disbursed_date'] = str(data['timestamp'][i])
                        individual_loan_details['loan_disbursed_amount'] = extract_amount(msg1)
                        individual_loan_details["messages"].append({"date" : str(data['timestamp'][i]), "message" : str(data['body'][i])})
                        loan_count += 1
                        j = i + 1    # iterate through next message after disbursal message
                        while j < len(data):
                            msg2 = str(data['body'][j].encode('utf-8')).lower()
                            if data["category"][j] == "disbursed":
                                logger.info("another disbursed message found before closing last loan")
                                break
                            if data["category"][j] == "due":
                                logger.info("due message found")
                                due_date = datetime.strptime(str(data['timestamp'][j]), "%Y-%m-%d %H:%M:%S")
                                if (due_date - disbursal_date).days < 16:
                                    # due message belongs to above disbursal message
                                    individual_loan_details['loan_due_amount'] = extract_amount(msg2)
                                    individual_loan_details["messages"].append({"date" : str(data['timestamp'][j]), "message" : str(data['body'][j])})
                                    k = j + 1
                                    while k < len(data):
                                        msg3 = str(data['body'][k].encode('utf-8')).lower()
                                        if data["category"][k] == "overdue":
                                            logger.info("overdue message found")
                                            try:
                                                individual_loan_details['overdue_days'] = overdue_days_extract(msg3, app)
                                            except:
                                                pass
                                            individual_loan_details['overdue_check'] += 1
                                            individual_loan_details["messages"].append({"date" : str(data['timestamp'][k]), "message" : str(data['body'][k])})
                                            m = k + 1
                                            while m < len(data):
                                                msg4= str(data['body'][m].encode('utf-8')).lower()
                                                if data["category"][m] == "closed":
                                                    logger.info("closed message found")
                                                    closed_date = datetime.strptime(str(data['timestamp'][m]), "%Y-%m-%d %H:%M:%S")
                                                    loan_duration = (closed_date - disbursal_date).days
                                                    if individual_loan_details['overdue_days'] == -1:
                                                        individual_loan_details['overdue_days'] = loan_duration - 15
                                                    individual_loan_details['closed_date'] = str(data['timestamp'][m])
                                                    individual_loan_details['loan_duration'] = loan_duration
                                                    individual_loan_details['loan_closed_amount'] = extract_amount(msg4)
                                                    individual_loan_details["messages"].append({"date" : str(data['timestamp'][m]), "message" : str(data['body'][m])})
                                                    k = m + 1
                                                    FLAG = True
                                                    logger.info("loan closed!")
                                                    break
                                                elif data["category"][m] == "disbursed" or data["category"][m] == "due":
                                                    logger.info("loan closed because before closing previous loan another disbursal message found")
                                                    k = m
                                                    FLAG = True
                                                    break
                                                elif data["category"][m] == "overdue":
                                                    k = m
                                                    try:
                                                        individual_loan_details['overdue_days'] = overdue_days_extract(msg4, app)
                                                    except:
                                                        pass
                                                    individual_loan_details['overdue_check'] += 1
                                                    individual_loan_details["messages"].append({"date" : str(data['timestamp'][m]), "message" : str(data['body'][m])})
                                                else:
                                                    pass
                                                m += 1
                                        elif data["category"][k] == "closed":
                                            logger.info("closed message found")
                                            closed_date = datetime.strptime(str(data['timestamp'][k]), "%Y-%m-%d %H:%M:%S")
                                            loan_duration = (closed_date - disbursal_date).days
                                            if loan_duration > 15:
                                                individual_loan_details['overdue_days'] = loan_duration - 15
                                            individual_loan_details['closed_date'] = str(data['timestamp'][k])
                                            individual_loan_details['loan_duration'] = loan_duration
                                            individual_loan_details['loan_closed_amount'] = extract_amount(msg3)
                                            individual_loan_details["messages"].append({"date" : str(data['timestamp'][k]), "message" : str(data['body'][k])})
                                            FLAG = True
                                            j = k
                                            logger.info("loan closed")
                                            break
                                        elif data["category"][k] == "disbursed":
                                            logger.info("loan closed because before closing previous loan another disbursal message found")
                                            j = k
                                            FLAG = True
                                            break
                                        elif data["category"][k] == "due":
                                            due_date = datetime.strptime(str(data['timestamp'][k]), "%Y-%m-%d %H:%M:%S")
                                            if (due_date - disbursal_date).days <= 16:
                                                j = k
                                                #individual_loan_details['messages'].append(msg_after_due)
                                                individual_loan_details["messages"].append({"date" : str(data['timestamp'][k]), "message" : str(data['body'][k])})
                                            else:
                                                logger.info("loan closed because a due message found which is not belong to current loan")
                                                FLAG = True
                                                j = k
                                                break
                                        else:
                                            pass
                                        if FLAG == True:
                                            j = k
                                        k += 1     # 'k' loop increment
                                else:
                                    # due message doesn't belong to above disbursal message
                                    i = j - 1
                                    break
                                if FLAG == True:
                                    i = j
                                    break
                            # ***********************************************************************************************
                            # ***********************************************************************************************
                            elif data["category"][j] == "overdue":
                                logger.info("overdue message found")
                                try:
                                    individual_loan_details['overdue_days'] = overdue_days_extract(msg2, app)
                                except:
                                    pass
                                individual_loan_details['overdue_check'] += 1
                                individual_loan_details["messages"].append({"date" : str(data['timestamp'][j]), "message" : str(data['body'][j])})
                                k = j + 1
                                while k < len(data):
                                    msg5 = str(data['body'][k].encode('utf-8')).lower()
                                    if data["category"][k] == "closed":
                                        logger.info("closed message found")
                                        closed_date = datetime.strptime(str(data['timestamp'][k]), "%Y-%m-%d %H:%M:%S")
                                        loan_duration = (closed_date - disbursal_date).days
                                        if individual_loan_details['overdue_days'] == -1:
                                            individual_loan_details['overdue_days'] = loan_duration - 15
                                        individual_loan_details['closed_date'] = str(data['timestamp'][k])
                                        individual_loan_details['loan_duration'] = loan_duration
                                        individual_loan_details['loan_closed_amount'] = extract_amount(msg5)
                                        individual_loan_details["messages"].append({"date" : str(data['timestamp'][k]), "message" : str(data['body'][k])})
                                        FLAG = True
                                        j = k + 1
                                        logger.info("loan closed!")
                                        break
                                    elif data["category"][k] == "disbursed" or data["category"][k] == "due":
                                        logger.info("loan closed because before closing previous loan another disbursal/due message found")
                                        FLAG = True
                                        j = k
                                        break
                                    elif data["category"][k] == "overdue":
                                        j = k
                                        try:
                                            individual_loan_details['overdue_days'] = overdue_days_extract(msg5, app)
                                            individual_loan_details["messages"].append({"date" : str(data['timestamp'][k]), "message" : str(data['body'][k])})
                                        except:
                                            pass
                                        individual_loan_details['overdue_check'] += 1
                                    else:
                                        pass
                                    k += 1
                                if FLAG == True:
                                    i = j
                                    break   # comes out from 'j' loop
                            # ***********************************************************************************************
                            # ***********************************************************************************************
                            elif data["category"][j] == "closed":
                                logger.info("closed message found")
                                closed_date = datetime.strptime(str(data['timestamp'][j]), "%Y-%m-%d %H:%M:%S")
                                loan_duration = (closed_date - disbursal_date).days
                                if loan_duration > 15:
                                    individual_loan_details['overdue_days'] = loan_duration - 15
                                individual_loan_details['closed_date'] = str(data['timestamp'][j])
                                individual_loan_details['loan_duration'] = loan_duration
                                individual_loan_details['loan_closed_amount'] = extract_amount(msg2)
                                #individual_loan_details['messages'].append(str(data['body'][j]))
                                individual_loan_details["messages"].append({"date" : str(data['timestamp'][j]), "message" : str(data['body'][j])})
                                i = j
                                logger.info("loan closed!")
                                FLAG = True
                                break
                            j += 1     # 'j' loop increment
                        loan_details_individual_app[str(loan_count)] = individual_loan_details

                    elif data["category"][i] == "due":
                        due_date = datetime.strptime(str(data['timestamp'][i]), "%Y-%m-%d %H:%M:%S")
                        individual_loan_details['loan_due_amount'] = extract_amount(msg1)
                        individual_loan_details['expected_closed_date'] = date_extract(msg1)
                        individual_loan_details['due_date'] = data['timestamp'][i]
                        individual_loan_details["messages"].append({"date" : str(data['timestamp'][i]), "message" : str(data['body'][i])})
                        loan_count += 1
                        j = i + 1
                        while j < len(data):
                            msg6 = str(data["body"][j]).lower()
                            if data["category"][j] == "due":
                                nxt_due_date = datetime.strptime(str(data['timestamp'][j]), "%Y-%m-%d %H:%M:%S")
                                if (nxt_due_date - due_date).days <= 16:
                                    i = j
                                    if individual_loan_details['expected_closed_date'] == -1:
                                        individual_loan_details['expected_closed_date'] = date_extract(msg6)
                                    individual_loan_details["messages"].append({"date" : str(data['timestamp'][j]), "message" : str(data['body'][j])})
                                else:
                                    logger.info("loan closed because a due message found which is not belong to current loan")
                                    FLAG = True
                                    i = j - 1
                                    break
                            elif data["category"][j] == "overdue":
                                individual_loan_details['overdue_days'] = days_extract(msg6)
                                individual_loan_details['overdue_check'] += 1
                                individual_loan_details["messages"].append({"date" : str(data['timestamp'][j]), "message" : str(data['body'][j])})
                                k = j + 1
                                while k < len(data):
                                    msg7 = str(data["body"][k]).lower()
                                    if data["category"][k] == "overdue":
                                        individual_loan_details['overdue_days'] = days_extract(msg7)
                                        individual_loan_details['overdue_check'] += 1
                                        individual_loan_details["messages"].append({"date" : str(data['timestamp'][k]), "message" : str(data['body'][k])})
                                        j = k
                                    elif data["category"][k] == "disbursed" or data["category"][k] == "due":
                                        j = k - 1
                                        FLAG = True
                                        break
                                    elif data["category"][k] == "closed":
                                        logger.info("closed message found")
                                        individual_loan_details['closed_date'] = str(data['timestamp'][k])
                                        #individual_loan_details['loan_duration'] = loan_duration
                                        individual_loan_details['loan_closed_amount'] = extract_amount(msg7)
                                        individual_loan_details["messages"].append({"date" : str(data['timestamp'][k]), "message" : str(data['body'][k])})
                                        j = k
                                        logger.info("loan closed!")
                                        FLAG = True
                                        break
                                    else:
                                        pass
                                    k += 1
                                if FLAG == True:
                                    i = j
                                    break
                            elif data["category"][j] == "closed":
                                logger.info("closed message found")
                                individual_loan_details['closed_date'] = str(data['timestamp'][j])
                                #individual_loan_details['loan_duration'] = loan_duration
                                individual_loan_details['loan_closed_amount'] = extract_amount(msg6)
                                individual_loan_details["messages"].append({"date" : str(data['timestamp'][j]), "message" : str(data['body'][j])})
                                i = j
                                logger.info("loan closed!")
                                break
                            elif data["category"][j] == "disbursed":
                                i = j - 1
                                break
                            else:
                                pass
                            j += 1
                        loan_details_individual_app[str(loan_count)] = individual_loan_details
                    else:
                        pass
                    i += 1   # 'i' loop increment
                loan_details_of_all_apps[app_name] = loan_details_individual_app
                logger.info("all information fetch from current loan app")
        rem_list = []
        for i in loan_details_of_all_apps.keys():
            if not loan_details_of_all_apps[i]:
                rem_list.append(i)
        [loan_details_of_all_apps.pop(key) for key in rem_list]
        # premium_rejection, normal_rejection, rejection_messages = get_rejection_count(cust_id)
        report['cust_id'] = cust_id
        report['complete_info'] = loan_details_of_all_apps
        report['user_app_list'] = user_app_list
        #report['current_open_details'] = get_current_open_details(cust_id)
        # report['premium_app_rejection'] = premium_rejection
        # report['normal_app_rejection'] = normal_rejection
        # report['rejection_messages'] = rejection_messages
        report['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))

        logger.info("successfully make connection with database")
        client = conn()
        client.analysis.loan.update_one({"cust_id" : cust_id}, {"$set" : report}, upsert = True)
        script_status = {"status" : True, "message" : "success"}
    except BaseException as e:
        r = {'status': False, 'message': str(e),
            'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': cust_id}
        client.analysisresult.exception_bl0.insert_one(r)
        script_status = {"status" : False, "message" : str(e)}
    finally:
        return script_status