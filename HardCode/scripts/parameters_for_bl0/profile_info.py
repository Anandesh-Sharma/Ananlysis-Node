import requests
from datetime import datetime, date

URL = 'https://admin.credicxotech.com/api/get_user_info/'
Auth = 'Bearer ' + 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires
    url_refresh = 'https://admin.credicxotech.com/api/token/refresh/'
    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU5MTg5NzY4NywianRpIjoiMDgxMDJmOGYzZGY0NDdhNTg3MDM5OGIwM2Q1ZWYzMjciLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.n9MRdhDHx-NdQElB0AHgerpDdLYl5Ufw_oSXJoUrB0o'}
    res = requests.post(url=url_refresh, data=refresh_token)
    r = res.json()
    return r['access']


def get_profile_info(user_id):
    """
    :returns age of the user
    :rtype: str
    """
    global Auth
    age = None
    app_data = None
    reference_relation, reference_number = None, None
    expected_date = []
    repayment_date = []
    allowed_limit = []
    total_loans = 0
    no_of_contacts = 0
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
                    no_of_contacts = data['profile']['kyc']['contacts_count']
                    age = data['profile']['dob']
                    app_data = data['apps']
                    if data['profile']['preference__relation'] and data['profile']['preference_number']:
                        reference_relation = data['profile']['preference__relation'].lower()
                        reference_number = data['profile']['preference_number']

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

        else:
            data = res.json()
            if 'error' not in data:
                no_of_contacts = data['profile']['kyc']['contacts_count']
                age = data['profile']['dob']
                app_data = data['apps']
                if data['profile']['preference__relation'] and data['profile']['preference_number']:
                    reference_relation = data['profile']['preference__relation'].lower()
                    reference_number = data['profile']['preference_number']
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
        pass
        # print(f"Error in fetching data from api : {e}")
    finally:
        return age,app_data,total_loans,allowed_limit,expected_date,repayment_date,reference_number,reference_relation,no_of_contacts