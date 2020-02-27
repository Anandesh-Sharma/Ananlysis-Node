def salary_analysis_function(salary,list_loans,current_loan,user_id):
    if 10000<salary<15000:
        a=2000
    elif salary>=15000:
        a=3000
    else:
        a=0
    if a<current_loan:
        a = current_loan
    return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': a, 'logic': 'BL0'}
    
def loan_analysis_function(loan_dict,list_loans,current_loan,user_id):
    if loan_dict['PAY_WITHIN_30_DAYS']:
        max_amount=loan_dict['MAX_AMOUNT']
        current_open_amount=loan_dict['CURRENT_OPEN_AMOUNT']
        current_open=loan_dict['CURRENT_OPEN']
        total_loan=loan_dict['TOTAL_LOANS']
        if( total_loan-current_open>2):
            if sum(current_open_amount>0):
                a=max_amount-sum(current_open_amount)
            else:
                a=max_amount/2
            if a<2000:
                a=2000
            for i in list_loans[::-1]:
                if i>a:
                    continue
                a=i
                break
        if current_open>a:
            a=current_open
        return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': a, 'logic': 'BL0'}
    else:
        return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                     'limit': 0, 'logic': 'BL0'}

def loan_salary_analysis_function(salary,loan_dict,list_loans,current_loan,user_id):
    if loan_dict['PAY_WITHIN_30_DAYS']:
        if 10000<salary<15000:
            max_amount=loan_dict['MAX_AMOUNT']
            current_open_amount=loan_dict['CURRENT_OPEN_AMOUNT']
            current_open=loan_dict['CURRENT_OPEN']
            total_loan=loan_dict['TOTAL_LOANS']
            if( total_loan-current_open>2):
                if sum(current_open_amount>0):
                    a=max_amount-sum(current_open_amount)
                else:
                    a=max_amount/2
                if a<2000:
                    a=2000
                for i in list_loans[::-1]:
                    if i>a:
                        continue
                    a=i
                    break
                if a>3000:
                    a=3000
            if current_open>a:
                a=current_open
        elif 15000<salary<25000:
            max_amount=loan_dict['MAX_AMOUNT']
            current_open_amount=loan_dict['CURRENT_OPEN_AMOUNT']
            current_open=loan_dict['CURRENT_OPEN']
            total_loan=loan_dict['TOTAL_LOANS']
            if( total_loan-current_open>2):
                if sum(current_open_amount>0):
                    a=max_amount-sum(current_open_amount)
                else:
                    a=max_amount/2
                if a<2000:
                    a=2000
                for i in list_loans[::-1]:
                    if i>a:
                        continue
                    a=i
                    break
                if a>4000:
                    a=4000
            if current_open>a:
                a=current_open
        elif salary>25000:
            max_amount = loan_dict['MAX_AMOUNT']
            current_open_amount = loan_dict['CURRENT_OPEN_AMOUNT']
            current_open = loan_dict['CURRENT_OPEN']
            total_loan = loan_dict['TOTAL_LOANS']
            if( total_loan-current_open>2):
                if sum(current_open_amount>0):
                    a=max_amount-sum(current_open_amount)
                else:
                    a=max_amount/2
                if a<2000:
                    a=2000
                for i in list_loans[::-1]:
                    if i>a:
                        continue
                    a=i
                    break
                if a>4000:
                    a=4000
            if current_open>a:
                a=current_open
    else:
        a=0
    return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                    'limit': a, 'logic': 'BL0'}