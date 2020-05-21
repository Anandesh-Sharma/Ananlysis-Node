import requests
from datetime import datetime, date
URL = 'https://admin.credicxotech.com/api/get_user_info/'
Auth = 'Bearer ' + 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires
    url_refresh = 'https://admin.credicxotech.com/api/token/refresh/'
    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'}
    res = requests.post(url=url_refresh, data=refresh_token)
    r = res.json()
    return r['access']


def get_loan_details(user_id):
    # ==> fetches reference relation and number from the api
    global Auth
    profile_data = None
    param = {'user_id': user_id}
    try:
        res = requests.get(url=URL, params=param, headers={'Authorization': Auth})
        if res.status_code == 404:
            raise BaseException
        if res.status_code == 401:
            access_token = generate_access_token()
            Auth = 'Bearer ' + access_token
            res = requests.get(url=URL, params=param, headers={'Authorization': Auth})
            if res.status_code == 404:
                raise BaseException
            else:
                data = res.json()
                if 'error' not in data:
                    expected_date = []
                    repayment_date = []
                    allowed_limit = []
                    total_loans = len(data['loans'])/2
                    for i in data['loans']:
                        allowed_limit.append(i['loan_type__amount'])

                    for dict in data['loans']:
                        expected_date.append(dict['loanrepaymentdates__repayment_date'])
                    for dict in data['transaction_status']:
                        repayment_date.append(dict['date_time'])


        else:
            data = res.json()
            if 'error' not in data:
                expected_date = []
                repayment_date = []
                allowed_limit = []
                total_loans = len(data['loans']) / 2
                for i in data['loans']:
                    allowed_limit.append(i['loan_type__amount'])

                for dict in data['loans']:
                    expected_date.append(dict['loanrepaymentdates__repayment_date'])
                for dict in data['transaction_status']:
                    repayment_date.append(dict['date_time'])
    except BaseException as e:
        print(f"Error in fetching data from api : {e}")
    finally:
        return total_loans,allowed_limit,expected_date,repayment_date


def repayment_history(user_id):
    total_loans,allowed_limit,expected_date,repayment_date = get_loan_details(user_id)
    loan_limit = allowed_limit[-1]
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

            # else:
            #     overdue = date.today() - expected_date[i]
            #     if overdue <= 3:
            #         overdue_report['0-3_days'] += 1
            #     elif (i > 3 and i <= 7):
            #         overdue_report['3-7_days'] += 1
            #     elif (i > 7 and i <= 12):
            #         overdue_report['7-12_days'] += 1
            #     elif (i > 12 and i <= 15):
            #         overdue_report['12-15_days'] += 1
            #     else:
            #         overdue_report['more_than_15'] += 1

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




    return total_loans,loan_limit,overdue_report,pending_emi


