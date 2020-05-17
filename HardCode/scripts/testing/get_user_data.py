import requests as re
import json
import os
from time import sleep
from HardCode.scripts.testing.all_repeated_ids import user_ids
from threadedprocess import ThreadedProcessPoolExecutor
import argparse
from tqdm import tqdm


# ap = argparse.ArgumentParser()
# ap.add_argument('-id', '--user_id', type=str, required=True)
# args = vars(ap.parse_args())


# to fetch the data run >>> python3 get_user_data.py --id user_id

def generate_access_token():
    URL = 'https://admin.credicxotech.com/api/token/refresh/'
    refresh_token = {
        'refresh': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTU4OTIyMDY3OSwianRpIjoiNTM2YzM4NWU2NzNjNDg1ODhjZGEwY2UzMDA4NTA2NzgiLCJ1c2VyX2lkIjoxNywiY3VycmVudF9zdGVwIjoxLCJkZXNpZ25hdGlvbiI6WyJzdXBlcnVzZXJfc3VwZXJ1c2VyIl0sIm5hbWUiOiJTdXJhaiBCb2hhcmEiLCJlbWFpbCI6InN1cmFqLmJvaGFyYS41ODlAZ21haWwuY29tIiwicGhvbmVfbnVtYmVyIjo5MjY3OTg4NTY1fQ.e9_9BKfyehBBYzwJQ9uvmmrcAh2GR0JzCYSED-hhLKQ'}
    res = re.post(url=URL, data=refresh_token)
    r = res.json()
    return r['access']


def get_sms_data(user_id):
    access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNTg5MjExODI2LCJqdGkiOiJlZmYwZGY2Y2RiYTk0YjYxOTE4MWIyZDM0Y2Q1Zjk1NSIsInVzZXJfaWQiOjE3LCJjdXJyZW50X3N0ZXAiOjEsImRlc2lnbmF0aW9uIjpbInN1cGVydXNlcl9zdXBlcnVzZXIiXSwibmFtZSI6IlN1cmFqIEJvaGFyYSIsImVtYWlsIjoic3VyYWouYm9oYXJhLjU4OUBnbWFpbC5jb20iLCJwaG9uZV9udW1iZXIiOjkyNjc5ODg1NjV9.hREL2z__p8zffk7vtgUOd96Tac8R0PZmezazZXjp7KQ'
    URL = 'https://admin.credicxotech.com/UserInfo/' + str(user_id) + '/sms_data.json'
    Auth = 'Bearer ' + access_token
    sms_data = re.get(url=URL, headers={'Authorization': Auth})
    print(sms_data, user_id)
    if sms_data.status_code == 404:
        return False

    if sms_data.status_code == 401 or sms_data.status_code == 502:
        sleep(5)
        print('401 and re-generate access token')
        access_token = generate_access_token()
        Auth = 'Bearer ' + access_token
        sms_data = re.get(url=URL, headers={'Authorization': Auth})
        sms = sms_data.json()

    else:
        try:
            sms = sms_data.json()
        except:
            sms = {}

    if not os.path.exists(str(user_id)):
        os.makedirs(str(user_id))

    sms_path = os.path.join(str(user_id), 'sms_data.json')
    with open(sms_path, 'w', encoding='utf-8') as f:
        json.dump(sms, f, ensure_ascii=False, indent=4)
    return True


def get_cibil_data(user_id):
    access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNTg5MjExODI2LCJqdGkiOiJlZmYwZGY2Y2RiYTk0YjYxOTE4MWIyZDM0Y2Q1Zjk1NSIsInVzZXJfaWQiOjE3LCJjdXJyZW50X3N0ZXAiOjEsImRlc2lnbmF0aW9uIjpbInN1cGVydXNlcl9zdXBlcnVzZXIiXSwibmFtZSI6IlN1cmFqIEJvaGFyYSIsImVtYWlsIjoic3VyYWouYm9oYXJhLjU4OUBnbWFpbC5jb20iLCJwaG9uZV9udW1iZXIiOjkyNjc5ODg1NjV9.hREL2z__p8zffk7vtgUOd96Tac8R0PZmezazZXjp7KQ'
    URL = 'https://admin.credicxotech.com/UserInfo/' + str(user_id) + '/experian_cibil.xml'
    Auth = 'Bearer ' + access_token

    cibil_data = re.get(url=URL, headers={'Authorization': Auth})

    if cibil_data.status_code == 404:
        return -1
    elif cibil_data.status_code == 401 or cibil_data.status_code == 502:
        sleep(5)
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
    # os.chdir('/home/ravan/credicxo-projects/Users Data for AN/')
    os.chdir('/home/ravan/credicxo-projects/Users Data for AN/')
    with ThreadedProcessPoolExecutor(max_processes=8, max_threads=16) as p:
        p.map(get_sms_data, l)
        p.map(get_cibil_data, l)
    # for user_id in l:
    #     print(f"Getting Data for id : {user_id}")
    #     sms_bool = get_sms_data(user_id)
    #     if sms_bool:
    #         get_cibil_data(user_id)
    # print("this id is skipped!")


# list_of_ids = [int(item) for item in args['user_id'].split(' ')]
# user_ids = [16161, 23777, 20463, 5011, 47572]
# user_ids = [281894]
get_user_data(user_ids)
