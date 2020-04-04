from HardCode.scripts.Util import conn


def loan_limit(user_id):
    """
    :returns max loan sanctioned by other loan apps, -1 implies amount is not detected
    :rtype: float
    """
    max_limit = -1
    connect = conn()
    loan_analysis_result = connect.analysis.loan.find_one({'cust_id': user_id})

    try:
        max_limit = loan_analysis_result['MAX_AMOUNT']

    except BaseException as e:
        print(f"Error in loan limit check : {e}")

    finally:
        return max_limit
