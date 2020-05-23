import requests as re
import json
import os
import argparse
from tqdm import tqdm

ap = argparse.ArgumentParser()
ap.add_argument('-id', '--user_id', type=str, required=True)
args = vars(ap.parse_args())


# to fetch the data run >>> python3 get_user_data.py --id user_id

def generate_access_token():
    URL = 'https://admin.credicxotech.com/api/token/refresh/'
    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU5MjY0MzE3MCwianRpIjoiNjYyMzI4OTQ2ZTRlNGU0NWEyNjQ2ZDdlOGQzODYwOWUiLCJ1c2VyX2lkIjoxNTcwMTEsImN1cnJlbnRfc3RlcCI6MTAxLCJkZXNpZ25hdGlvbiI6WyJNTF9NYW5hZ2VyIiwiMTciXSwibmFtZSI6IlRlc3QiLCJlbWFpbCI6ImFuYW5kZXNoc2hhcm1hQGdtYWlsLmNvbSIsInBob25lX251bWJlciI6OTk5Njk0NDk0M30.Q0G3C33JUA9m2Fwm-PN7dcHincc8WRhd0zzhtnRL7pA'}

    res = re.post(url=URL, data=refresh_token)
    print(res)
    r = res.json()
    print(r)
    return r['access']


def get_sms_data(user_id, access_token):
    URL = 'https://admin.credicxotech.com/UserInfo/' + str(user_id) + '/sms_data.json'
    # access_token = generate_access_token()
    Auth = 'Bearer ' + access_token

    sms_data = re.get(url=URL, headers={'Authorization': Auth})
    print(sms_data.status_code)

    if sms_data.status_code == 404:
        print("sms_data does not exist")
        return False

    if sms_data.status_code == 401:
        access_token = generate_access_token()
        Auth = 'Bearer ' + access_token
        sms_data = re.get(url=URL, headers={'Authorization': Auth})
        sms = sms_data.json()
        # print("sms_data does not exist")
        # return False

    else:
        sms = sms_data.json()

    if not os.path.exists(str(user_id)):
        os.makedirs(str(user_id))

    sms_path = os.path.join(str(user_id), 'sms_data.json')
    with open(sms_path, 'w', encoding='utf-8') as f:
        json.dump(sms, f, ensure_ascii=False, indent=4)
    return True


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

        cibil = cibil_data.text

    else:
        cibil = cibil_data.text

    cibil_path = os.path.join(str(user_id), 'cibil_data.xml')
    with open(cibil_path, 'w') as f:
        f.write(cibil)


def get_user_data(l):
    access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNTkwMjk4ODM0LCJqdGkiOiJkZGM4YWZkYmQyNzI0OGQzOGFiMjYyM2EyN2FjM2YyNyIsInVzZXJfaWQiOjE1NzAxMSwiY3VycmVudF9zdGVwIjoxMDEsImRlc2lnbmF0aW9uIjpbIk1MX01hbmFnZXIiLCIxNyJdLCJuYW1lIjoiVGVzdCIsImVtYWlsIjoiYW5hbmRlc2hzaGFybWFAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5OTk2OTQ0OTQzfQ.ajMmHPal5g6QzjRvWBYfUJP_lFmgmZUx79lSMhEwqnM'
    for user_id in l:
        sms_bool = get_sms_data(user_id, access_token)
        if sms_bool:
            get_cibil_data(user_id, access_token)

os.chdir('C:/Users/shreya/Desktop/Users_Info/')
list_of_ids = [int(item) for item in args['user_id'].split(' ')]
get_user_data(list_of_ids)

