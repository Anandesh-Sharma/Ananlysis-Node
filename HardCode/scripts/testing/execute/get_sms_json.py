import json
import os
import requests as re


def generate_access_token():
    URL = 'https://admin.credicxotech.com/api/token/refresh/'

    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4NjY4NjU0NCwianRpIjoiNDk5ZGQ4MDlmYWEzNDFlNGI0NmY4M2IwNTZkODA3OTUiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIn0.CUNH83SYpbsK6xj5xIHstj7Q8OIQyRR0m_ug6tqJWf0'}

    res = re.post(url=URL, data=refresh_token)
    r = res.json()
    return r['access']


def get_sms_data(user_id, access_token):
    URL = 'https://admin.credicxotech.com/UserInfo/' + str(user_id) + '/sms_data.json'
    # access_token = generate_access_token()
    Auth = 'Bearer ' + access_token

    sms_data = re.get(url=URL, headers={'Authorization': Auth})

    if sms_data.status_code == 404:
        print("sms_data does not exist")
        return -1

    if sms_data.status_code == 401:

        access_token = generate_access_token()
        Auth = 'Bearer ' + access_token
        sms_data = re.get(url=URL, headers={'Authorization': Auth})
        if sms_data.status_code == 404:
            print("sms_data does not exist")
            return -1
        else:
            sms = sms_data.json()

    else:
        sms = sms_data.json()

    if not os.path.exists('input_data'):
        os.makedirs('input_data')

    sms_path = os.path.join('../input_data', 'sms_data_' + str(user_id) + '.json')
    with open(sms_path, 'a', encoding='utf-8') as f:
        json.dump(sms, f, ensure_ascii=False, indent=4)


def get_sms(user_id):
    access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNTg0MjU4NTM0LCJqdGkiOiI3N2JjOTE1Yjc3ZTY0MGRhOGI4ZDAxMmQ1NGI2Yjk2MCIsInVzZXJfaWQiOjE3LCJjdXJyZW50X3N0ZXAiOjEsImRlc2lnbmF0aW9uIjpbInN1cGVydXNlcl9zdXBlcnVzZXIiXSwibmFtZSI6IlN1cmFqIEJvaGFyYSIsImVtYWlsIjoic3VyYWouYm9oYXJhLjU4OUBnbWFpbC5jb20ifQ.nt6c0vETWzhdV45eaY-SBEb92zJBuTGPusWExED729Y'

    try:
        get_sms_data(user_id, access_token)
    except BaseException as e:
        print("Error: ", str(e))
        print("cust_id: ", user_id)
