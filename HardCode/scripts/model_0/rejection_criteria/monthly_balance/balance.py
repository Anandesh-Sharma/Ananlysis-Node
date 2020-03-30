from HardCode.scripts.Util import conn


def average_monthly_balance(user_id) -> bool:
    connect = conn()
    bal = connect.analysis.balance_sheet.find_one({'cust_id': user_id})
    avg = []

    avg_balance = None
    try:

        if bal:
            credit = bal['credit']
            for i in range(len(credit)):
                a = credit[i][1] - bal['debit'][i][1]
                avg.append(a)
            avg_balance = sum(avg) / len(avg)
            # if avg_balance < 2000:
            #     balance_check = False
    except BaseException as e:
        print(f"Error in balance check : {e}")

    finally:
        return round(avg_balance, 2)
