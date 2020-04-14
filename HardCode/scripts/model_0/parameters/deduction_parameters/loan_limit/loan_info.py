from HardCode.scripts.Util import conn


def loan_limit(user_id):
    """
    :returns max loan sanctioned by other loan apps, -1 implies amount is not detected
    :rtype: float
    """
    max_limit = -1
    due_days = -1
    no_of_loan_apps = 0
    premium_apps = []
    connect = conn()
    loan_analysis_result = connect.analysis.loan.find_one({'cust_id': user_id})

    try:
        max_limit = loan_analysis_result['MAX_AMOUNT']
        due_days = loan_analysis_result['OVERDUE_DAYS']
        no_of_loan_apps = loan_analysis_result['TOTAL_LOAN_APPS']
        premium_apps = loan_analysis_result['LOAN_APP_LIST']

    except BaseException as e:
        print(f"Error in loan limit check : {e}")

    finally:
        return max_limit,due_days,no_of_loan_apps,premium_apps
