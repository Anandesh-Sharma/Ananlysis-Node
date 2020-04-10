import json
import os
import warnings
import requests
from HardCode.scripts.testing.execute.data_fetch import get_sms, get_cibil
from HardCode.scripts.testing.user_ids import *

warnings.filterwarnings('ignore')
from tqdm import tqdm


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires

    url = 'http://localhost:8888/api/token/'

    credentials = {'username': 'root', 'password': 'root'}

    res = requests.post(url=url, data=credentials, verify=False)
    res = res.json()
    return res['access']


def execute_bl0(**kwargs):
    user_id = kwargs.get('user_id')
    cibil_score = kwargs.get('cibil_score')
    # cibil_xml = kwargs.get('cibil_file')

    if os.path.exists(os.path.join('..\input_data', 'cibil_data_' + str(user_id) + '.xml')):
        cibil_xml = open(os.path.join('..\input_data', 'cibil_data_' + str(user_id) + '.xml'))
    else:
        cibil_xml = None
    sms_json = open(os.path.join('..\input_data', 'sms_data_' + str(user_id) + '.json'), 'rb')

    new_user = 1
    all_loan_amount = [1000, 2000, 3000, 4000]
    current_loan_amount = 0

    url = 'http://localhost:8888/hard_code/bl0/'
    token = generate_access_token()
    Auth = 'Bearer ' + str(token)
    payload = {
        'user_id': user_id,
        'new_user': new_user,
        'cibil_score': cibil_score,
        'current_loan_amount': current_loan_amount,
        'all_loan_amount': all_loan_amount,

    }
    files = [('sms_json', sms_json), ('cibil_xml', cibil_xml)]
    result = requests.post(url=url, data=payload, files=files, headers={'Authorization': Auth})
    result = result.json()
    print(result['Model_0']['score'])
    # if not os.path.exists('../result'):
    #     os.mkdir('result')

    with open(str(user_id) + '.json', 'w', encoding='utf-8') as fp:
        json.dump(result, fp, ensure_ascii=False, indent=4)


def testing(user_id):
    if not os.path.exists(os.path.join('..\input_data', 'sms_data_' + str(user_id) + '.json')):
        get_sms(user_id=user_id)

    if not os.path.exists(os.path.join('..\input_data', 'cibil_data_' + str(user_id) + '.xml')):
        get_cibil(user_id=user_id)

    try:
        if str(user_id).isdigit():
            if os.path.exists(os.path.join('..\input_data', 'sms_data_' + str(user_id) + '.json')):

                # ==> cibil score is passed 807 by default

                execute_bl0(user_id=int(user_id), cibil_score=807)

                print(f"result generated successfully : {user_id}")
            else:
                msg = 'sms json does not exists'
                raise BaseException(msg)
        else:
            msg = 'user id must contain only numbers'
            raise BaseException(msg)

    except BaseException as e:
        print(f"the following error occurred : {e}")


# user_id = input('enter user id: ')
# testing(user_id=user_id)
# #
l = non_defaulters
l.sort(reverse=True)
print(len(l))
print(l)

# from concurrent.futures import ThreadPoolExecutor

# with ThreadPoolExecutor() as exc:
#     exc.map(testing,(i for i in l))

# #
for i in tqdm(l[:10]):
    testing(i)
