from HardCode.scripts.Util import conn


def loan_limit(user_id) -> bool:

    connect = conn()
    loan_analysis_result = connect.analysis.loan.find_one({'cust_id': user_id})

    try:
        max_limit = loan_analysis_result['MAX_AMOUNT']

    except BaseException as e:
        print(f"Error in loan limit check : {e}")

    finally:
        return max_limit
