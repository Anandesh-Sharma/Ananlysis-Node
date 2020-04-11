from HardCode.scripts.Util import conn


def average_monthly_balance(user_id):
    """
    :return average monthly balance maintained by the user
    :rtype: float
    """
    connect = conn()
    bal = connect.analysis.balance_sheet.find_one({'cust_id': user_id})
    avg = []
    status = False
    avg_balance = 0
    try:

        if bal:
            credit = bal['credit']
            for i in range(len(credit)):
                a = credit[i][1] - bal['debit'][i][1]
                avg.append(a)
            avg_balance = sum(avg) / len(avg)
            status = True
            
    except BaseException as e:
        print(f"Error in balance check : {e}")

    finally:
        return round(avg_balance, 2) , status
