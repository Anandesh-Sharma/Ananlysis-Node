from HardCode.scripts.Util import conn
from HardCode.scripts.model_0.parameters.deduction_parameters.loan_limit.overdue_details import get_overdue_details


def loan_limit(user_id):
    """
    :returns max loan sanctioned by other loan apps, -1 implies amount is not detected
    :rtype: float
    """

    max_limit = -1
    due_days = -1
    no_of_loan_apps = 0
    loan_apps = []
    loan_dates = []
    overdue_ratio = 0
    connect = conn()
    loan_analysis_result = connect.analysis.loan.find_one({'cust_id': user_id})
    overdue_ratio_3_months, overdue_report = get_overdue_details(user_id)

    try:
        max_limit = loan_analysis_result['MAX_AMOUNT']
        no_of_loan_apps = loan_analysis_result['TOTAL_LOAN_APPS']
        loan_apps = loan_analysis_result['LOAN_APP_LIST']
        loan_dates = loan_analysis_result['LOAN_DATES']

        due_days = overdue_report
        overdue_ratio = overdue_ratio_3_months

    except BaseException as e:
        print(f"Error in loan limit check : {e}")

    finally:
        return max_limit,due_days,no_of_loan_apps,loan_apps, overdue_ratio, loan_dates
