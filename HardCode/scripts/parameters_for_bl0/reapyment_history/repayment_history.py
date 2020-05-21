from HardCode.scripts.parameters_for_bl0.profile_info import get_profile_info
from datetime import datetime
from HardCode.scripts.Util import conn,logger_1
import pytz


def repayment_history(user_id):
    connect=  conn()
    db = connect.analysis.parameters
    parameters = {}
    age,app_data,total_loans,allowed_limit,expected_date,repayment_date,reference_number,reference_relation,no_of_contacts = get_profile_info(user_id)
    if allowed_limit:
        loan_limit = allowed_limit[-1]
    else:
        loan_limit = 0

    try:
        date1 = datetime.strptime('2020-03-21', '%Y-%m-%d')
        for i in range(len(expected_date)):
            expected_date[i] = datetime.strptime(str(expected_date[i]), '%Y-%m-%d')
        for i in range(len(repayment_date)):
            repayment = repayment_date[i].split("T")[0]
            repayment_date[i] = datetime.strptime(str(repayment), '%Y-%m-%d')
        pending_emi = 0
        overdue_report = {
            '0-3_days': 0,
            '3-7_days': 0,
            '7-12_days': 0,
            '12-15_days': 0,
            'more_than_15': 0
        }

        for i in range(len(expected_date)):
            if expected_date[i] < date1:
                try:
                    if repayment_date[i] > expected_date[i]:
                        overdue = repayment_date[i] - expected_date[i]      # EMI wise overdue days
                        if overdue.days <= 3:
                            overdue_report['0-3_days'] += 1
                        elif (overdue.days > 3 and overdue.days <= 7):
                            overdue_report['3-7_days'] += 1
                        elif (overdue.days > 7 and overdue.days <= 12):
                            overdue_report['7-12_days'] += 1
                        elif (overdue.days > 12 and overdue.days <= 15):
                            overdue_report['12-15_days'] += 1
                        else:
                            overdue_report['more_than_15'] += 1
                except:
                    overdue = datetime.now() - expected_date[i]
                    if overdue.days <= 3:
                        overdue_report['0-3_days'] += 1
                    elif (overdue.days > 3 and overdue.days <= 7):
                        overdue_report['3-7_days'] += 1
                    elif (overdue.days > 7 and overdue.days <= 12):
                        overdue_report['7-12_days'] += 1
                    elif (overdue.days > 12 and overdue.days <= 15):
                        overdue_report['12-15_days'] += 1
                    else:
                        overdue_report['more_than_15'] += 1



            elif expected_date[i] > date1:
                try:
                    if repayment_date[i] > expected_date[i]:
                        overdue = repayment_date[i] - expected_date[i]      # EMI wise overdue days
                        if overdue.days <= 3:
                            overdue_report['0-3_days'] += 1
                        elif (overdue.days > 3 and overdue.days <= 7):
                            overdue_report['3-7_days'] += 1
                        elif (overdue.days > 7 and overdue.days <= 12):
                            overdue_report['7-12_days'] += 1
                        elif (overdue.days > 12 and overdue.days <= 15):
                            overdue_report['12-15_days'] += 1
                        else:
                            overdue_report['more_than_15'] += 1
                except:
                    overdue = datetime.now() - expected_date[i]
                    if overdue.days <= 3:
                        overdue_report['0-3_days'] += 1
                    elif (overdue.days > 3 and overdue.days <= 7):
                        overdue_report['3-7_days'] += 1
                    elif (overdue.days > 7 and overdue.days <= 12):
                        overdue_report['7-12_days'] += 1
                    elif (overdue.days > 12 and overdue.days <= 15):
                        overdue_report['12-15_days'] += 1
                    else:
                        overdue_report['more_than_15'] += 1
                    pending_emi += 1

        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.credicxo_overdue_days': overdue_report,
                                                  'parameters.credicxo_total_loans':total_loans,
                                                  'parameters.credicxo_loan_limit':loan_limit,
                                                  'parameters.credicxo_pending_emi':pending_emi}}, upsert=True)
        return {'status':True,'message':'success'}
    except BaseException as e:
        overdue_report = {
            '0-3_days': -1,
            '3-7_days': -1,
            '7-12_days': -1,
            '12-15_days': -1,
            'more_than_15': -1}
        pending_emi = -1
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.credicxo_overdue_days': overdue_report,
                                                  'parameters.credicxo_total_loans': total_loans,
                                                  'parameters.credicxo_loan_limit': loan_limit,
                                                  'parameters.credicxo_pending_emi': pending_emi}}, upsert=True)
        return {'status': False, 'message': str(e)}


