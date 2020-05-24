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
    logger.info("Data Grouped by Sender-Name")
    loan_details_of_all_apps = {}
    user_app_list = []
    report = {}
    script_status = {}
    #print(loan_data['Sender-Name'].unique())
    try:
        for app, data in loan_data_grouped:
            logger.info("iteration in groups starts")
            '''try:
                if isinstance(int(app), int):
                    pass
            except:
                user_app_list.append(app)'''
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

                while i < len(data):
                    individual_loan_details = {
                        'disbursed_date': -1,
                        'closed_date': -1,
                        'loan_duration': -1,
                        'loan_disbursed_amount': -1,
                        'due_message_date' : -1,
                        'loan_due_amount': -1,
                        'loan_closed_amount' : -1,
                        'overdue_days' : -1,
                        'overdue_check' : 0,
                        'messages': []
                    }
                    message = str(data['body'][i].encode('utf-8')).lower()
                    if is_disbursed(message, app):
                        logger.info("disbursal message found")
                        disbursal_date = datetime.strptime(str(data['timestamp'][i]), "%Y-%m-%d %H:%M:%S")
                        individual_loan_details['disbursed_date'] = str(data['timestamp'][i])
                        individual_loan_details['loan_disbursed_amount'] = float(disbursed_amount_extract(message, app))
                        individual_loan_details['messages'].append(str(data['body'][i]))
                        loan_count += 1
                        j = i + 1    # iterate through next message after disbursal message
                        while j < len(data):
                            msg_after_disbursal = str(data['body'][j].encode('utf-8')).lower()
                            if is_disbursed(msg_after_disbursal, app):
                                i = j
                                break
                            if is_due(msg_after_disbursal, app):
                                logger.info("due message found")
                                due_date = datetime.strptime(str(data['timestamp'][j]), "%Y-%m-%d %H:%M:%S")
                                if (due_date - disbursal_date).days < 16:
                                    # due message belongs to above disbursal message
                                    individual_loan_details['loan_due_amount'] = float(due_amount_extract(msg_after_disbursal, app))
                                    individual_loan_details['messages'].append(msg_after_disbursal)
                                    k = j + 1
                                    while k < len(data):
                                        msg_after_due = str(data['body'][k].encode('utf-8')).lower()
                                        if is_overdue(msg_after_due, app):
                                            logger.info("overdue message found")
                                            try:
                                                individual_loan_details['overdue_days'] = overdue_days_extract(msg_after_due, app)
                                            except:
                                                pass
                                            individual_loan_details['overdue_check'] += 1
                                            individual_loan_details['messages'].append(str(data['body'][k]))
                                            m = k + 1
                                            while m < len(data):
                                                msg_after_overdue = str(data['body'][m].encode('utf-8')).lower()
                                                if is_closed(msg_after_overdue, app):
                                                    logger.info("closed message found")
                                                    closed_date = datetime.strptime(str(data['timestamp'][m]), "%Y-%m-%d %H:%M:%S")
                                                    loan_duration = (closed_date - disbursal_date).days
                                                    if individual_loan_details['overdue_days'] == -1:
                                                        individual_loan_details['overdue_days'] = loan_duration - 15
                                                    individual_loan_details['closed_date'] = str(data['timestamp'][m])
                                                    individual_loan_details['loan_duration'] = loan_duration
                                                    individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(msg_after_overdue, app))
                                                    individual_loan_details['messages'].append(str(data['body'][m]))
                                                    k = m + 1
                                                    FLAG = True
                                                    logger.info("loan closed!")
                                                    break
                                                elif is_disbursed(msg_after_overdue, app) or is_due(msg_after_overdue, app):
                                                    logger.info("loan closed because before closing previous loan another disbursal message found")
                                                    k = m
                                                    FLAG = True
                                                    break
                                                elif is_overdue(msg_after_overdue, app):
                                                    k = m
                                                    try:
                                                        individual_loan_details['overdue_days'] = overdue_days_extract(msg_after_overdue, app)
                                                    except:
                                                        pass
                                                    individual_loan_details['overdue_check'] += 1
                                                    individual_loan_details['messages'].append(str(data['body'][m]))
                                                else:
                                                    pass
                                                m += 1
                                        elif is_closed(msg_after_due, app):
                                            logger.info("closed message found")
                                            closed_date = datetime.strptime(str(data['timestamp'][k]), "%Y-%m-%d %H:%M:%S")
                                            loan_duration = (closed_date - disbursal_date).days
                                            if loan_duration > 15:
                                                individual_loan_details['overdue_days'] = loan_duration - 15
                                            individual_loan_details['closed_date'] = str(data['timestamp'][k])
                                            individual_loan_details['loan_duration'] = loan_duration
                                            individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(msg_after_due, app))
                                            individual_loan_details['messages'].append(str(data['body'][k]))
                                            FLAG = True
                                            j = k + 1
                                            logger.info("loan closed")
                                            break
                                        elif is_disbursed(msg_after_due, app):
                                            logger.info("loan closed because before closing previous loan another disbursal message found")
                                            j = k
                                            FLAG = True
                                            break
                                        elif is_due(msg_after_due, app):
                                            due_date = datetime.strptime(str(data['timestamp'][k]), "%Y-%m-%d %H:%M:%S")
                                            if (due_date - disbursal_date).days <= 16:
                                                j = k
                                                individual_loan_details['messages'].append(msg_after_due)
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
                                    i = j
                                    break
                                if FLAG == True:
                                    i = j
                                    break
                            # ***********************************************************************************************
                            # ***********************************************************************************************
                            elif is_overdue(msg_after_disbursal, app):
                                logger.info("overdue message found")
                                try:
                                    individual_loan_details['overdue_days'] = overdue_days_extract(msg_after_disbursal, app)
                                except:
                                    pass
                                individual_loan_details['overdue_check'] += 1
                                individual_loan_details['messages'].append(str(data['body'][j]))
                                k = j + 1
                                while k < len(data):
                                    msg_after_overdue = str(data['body'][k].encode('utf-8')).lower()
                                    if is_closed(msg_after_overdue, app):
                                        logger.info("closed message found")
                                        closed_date = datetime.strptime(str(data['timestamp'][k]), "%Y-%m-%d %H:%M:%S")
                                        loan_duration = (closed_date - disbursal_date).days
                                        if individual_loan_details['overdue_days'] == -1:
                                            individual_loan_details['overdue_days'] = loan_duration - 15
                                        individual_loan_details['closed_date'] = str(data['timestamp'][k])
                                        individual_loan_details['loan_duration'] = loan_duration
                                        individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(msg_after_overdue, app))
                                        individual_loan_details['messages'].append(str(data['body'][k]))
                                        FLAG = True
                                        j = k + 1
                                        logger.info("loan closed!")
                                        break
                                    elif is_disbursed(msg_after_overdue, app) or is_due(msg_after_overdue, app):
                                        logger.info("loan closed because before closing previous loan another disbursal/due message found")
                                        FLAG = True
                                        j = k
                                        break
                                    elif is_overdue(msg_after_overdue, app):
                                        j = k
                                        try:
                                            individual_loan_details['overdue_days'] = overdue_days_extract(msg_after_overdue, app)
                                            individual_loan_details['messages'].append(msg_after_overdue)
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
                            elif is_closed(msg_after_disbursal, app):
                                logger.info("closed message found")
                                closed_date = datetime.strptime(str(data['timestamp'][j]), "%Y-%m-%d %H:%M:%S")
                                loan_duration = (closed_date - disbursal_date).days
                                if loan_duration > 15:
                                    individual_loan_details['overdue_days'] = loan_duration - 15
                                individual_loan_details['closed_date'] = str(data['timestamp'][j])
                                individual_loan_details['loan_duration'] = loan_duration
                                individual_loan_details['loan_closed_amount'] = float(closed_amount_extract(msg_after_disbursal, app))
                                individual_loan_details['messages'].append(str(data['body'][j]))
                                i = j + 1
                                logger.info("loan closed!")
                                break
                            j += 1     # 'j' loop increment
                        loan_details_individual_app[str(loan_count)] = individual_loan_details
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