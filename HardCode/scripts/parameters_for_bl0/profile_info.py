import requests
from datetime import datetime, date

URL = 'https://admin.credicxotech.com/api/get_user_info/'


def generate_access_token():
    refresh_url = 'https://admin.credicxotech.com/api/token/refresh/'
    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU5MjY0MzE3MCwianRpIjoiNjYyMzI4OTQ2ZTRlNGU0NWEyNjQ2ZDdlOGQzODYwOWUiLCJ1c2VyX2lkIjoxNTcwMTEsImN1cnJlbnRfc3RlcCI6MTAxLCJkZXNpZ25hdGlvbiI6WyJNTF9NYW5hZ2VyIiwiMTciXSwibmFtZSI6IlRlc3QiLCJlbWFpbCI6ImFuYW5kZXNoc2hhcm1hQGdtYWlsLmNvbSIsInBob25lX251bWJlciI6OTk5Njk0NDk0M30.Q0G3C33JUA9m2Fwm-PN7dcHincc8WRhd0zzhtnRL7pA'}

    res = requests.post(url=refresh_url, data=refresh_token)
    r = res.json()
    return r['access']


def get_profile_info(user_id):
    """
    :returns age of the user
    :rtype: str
    """
    Auth = 'Bearer ' + generate_access_token()
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
                print(data)

                if 'error' not in data:
                    no_of_contacts = data['profile']['KYC']['contacts_count']
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
                no_of_contacts = data['profile']['KYC']['contacts_count']
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
        return age, app_data, total_loans, allowed_limit, expected_date, repayment_date, reference_number, reference_relation, no_of_contacts
