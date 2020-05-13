import json
import os
import requests as re


def generate_access_token():
    URL = 'https://admin.credicxotech.com/api/token/refresh/'

    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU5MTg5NzY4NywianRpIjoiMDgxMDJmOGYzZGY0NDdhNTg3MDM5OGIwM2Q1ZWYzMjciLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.n9MRdhDHx-NdQElB0AHgerpDdLYl5Ufw_oSXJoUrB0o'}

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

    if not os.path.exists('..\input_data'):
        os.makedirs('input_data')

    sms_path = os.path.join('..\input_data', 'sms_data_' + str(user_id) + '.json')
    with open(sms_path, 'a', encoding='utf-8') as f:
        json.dump(sms, f, ensure_ascii=False, indent=4)


def get_cibil_data(user_id, access_token):
    URL = 'https://admin.credicxotech.com/UserInfo/' + str(user_id) + '/experian_cibil.xml'
    Auth = 'Bearer ' + access_token

    cibil_data = re.get(url=URL, headers={'Authorization': Auth})

    if cibil_data.status_code == 404:
        print("Cibil does not exist")
        return -1
    elif cibil_data.status_code == 401:
        access_token = generate_access_token()
        Auth = 'Bearer ' + access_token
        cibil_data = re.get(url=URL, headers={'Authorization': Auth})
        if cibil_data.status_code == 404:
            print("Cibil does not exist")
            return -1
        else:
            cibil = cibil_data.text

    else:
        cibil = cibil_data.text

    cibil_path = os.path.join('..\input_data', 'cibil_data_' + str(user_id) + '.xml')
    with open(cibil_path, 'w') as f:
        f.write(cibil)


def get_sms(user_id):
    access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'

    try:
        get_sms_data(user_id, access_token)
    except BaseException as e:
        print("Error: ", str(e))
        print("cust_id: ", user_id)


def get_cibil(user_id):
    access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'

    try:
        get_cibil_data(user_id, access_token)
    except BaseException as e:
        print("Error: ", str(e))
        print("cust_id: ", user_id)
