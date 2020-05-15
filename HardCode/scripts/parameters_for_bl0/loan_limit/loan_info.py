from HardCode.scripts.Util import conn
from datetime import datetime
import pytz
from HardCode.scripts.parameters_for_bl0.loan_limit.overdue_details import get_overdue_details


def loan_limit(user_id):
    """
    parameter: user_id(int)
    :returns max loan sanctioned by other loan apps, -1 implies amount is not detected
    :rtype: float
    """
    max_limit = -1
    due_days = -1
    no_of_loan_apps = 0
    loan_apps = []
    loan_dates = []
    overdue_ratio = 0
    total_loans = 0
    connect = conn()
    db = connect.analysis.parameters
    parameters = {}
    output = {}
    loan_analysis_result = connect.analysis.loan.find_one({'cust_id': user_id})
    overdue_ratio_3_months, overdue_report, total_loans_within_3_months = get_overdue_details(user_id)

    try:

        max_limit = loan_analysis_result['MAX_AMOUNT']
        no_of_loan_apps = loan_analysis_result['TOTAL_LOAN_APPS']
        loan_apps = loan_analysis_result['LOAN_APP_LIST']
        loan_dates = loan_analysis_result['LOAN_DATES']

        due_days = overdue_report
        overdue_ratio = overdue_ratio_3_months
        total_loans = total_loans_within_3_months
        parameters['loan_limit'] = max_limit
        parameters['no_of_loan_apps'] = no_of_loan_apps
        parameters['loan_apps_list'] = loan_apps
        parameters['loan_dates'] = loan_dates
        parameters['overdue_days'] = due_days
        parameters['overdue_ratio'] = overdue_ratio
        parameters['total_loans'] = total_loans
        status = True
        msg = 'success'


    except BaseException as e:
        status = False
        msg = str(e)
        print(f"Error in loan limit check : {e}")

    finally:
        parameters['cust_id'] = user_id
        db.update({'cust_id': user_id}, {"$set": {'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                  'parameters.loan_limit': max_limit,
                                                  'parameters.no_of_loan_apps': no_of_loan_apps,
                                                  'parameters.loan_apps_list': loan_apps,
                                                  'parameters.loan_dates': loan_dates,
                                                  'parameters.overdue_days': due_days,
                                                  'parameters.overdue_ratio': overdue_ratio,
                                                  'parameters.total_loans': total_loans}}, upsert=True)
        return {'status':status,'message':msg}
