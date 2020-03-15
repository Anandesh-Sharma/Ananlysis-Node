def analyse(**kwargs):
    user_id = kwargs.get('user_id')
    cibil_score = int(kwargs.get('cibil_score'))
    new_user = kwargs.get('new_user')
    current_loan = kwargs.get('current_loan')
    cibil_df = kwargs.get('cibil_df')
    # if cibil found for a specific customer then run the new cibil analysis
    # IMPORTANT #
    # new cibil logic is implemented on new customers only
    # Dheeraj told to run it on 700-900 but we are giving loans to 750 < !!
    # dataframe throws error ## manually parse xml
    if cibil_df['status'] == False and new_user:
        try:
            with open(f'logs/{user_id}.txt', 'w') as f:
                f.write(f"DF failed for userid {user_id}")
        except Exception as e:
            print(f"XML df creation failed with error {e}")
        print(f"DF failed for userid {user_id}")
    if cibil_df['status'] and new_user and 700 < cibil_score < 750:
        cibil_df = cibil_df['data']
        Account_Status = dict()
        Payment_Ratings = dict()
        review = False
        # account status
        for acc_status in cibil_df['account_status']:
            Account_Status[acc_status] = user_id
        # payment ratings
        for pay_rating in cibil_df['payment_rating']:
            Payment_Ratings[pay_rating] = user_id

        Blocked_Payment_Ratings = [3, 4, 5, 6]
        Blocked_Status = [93, 89, 97, 32, 33, 34, 35, 37, 38, 43, 44, 45, 46, 47, 49, 50, 53, 54, 55, 56, 57, 58, 59,
                          60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 72, 73, 74, 75, 76, 77, 79, 81, 85, 86, 87, 88,
                          94, 90]
        for bpr in Blocked_Payment_Ratings:
            if str(bpr) in Payment_Ratings:
                review = True
                break
        if not review:
            for bs in Blocked_Status:
                if str(bs) in Account_Status:
                    review = True
                    break

        if not review:
            a = 2000
        else:
            a = -1
    # base logic will run for the old customers having equifax score
    elif cibil_score >= 750:
        a = 3000

    else:
        if new_user:
            a = -1
        else:
            a = current_loan

    # returning result
    if a == -1:
        r = {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id,
             'limit': a, 'logic': 'BL0-cibil'}
    else:
        r = {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
             'limit': a, 'logic': 'BL0-cibil'}
    if r['limit'] > 3000 and new_user:
        r['limit'] = 3000
    return r
