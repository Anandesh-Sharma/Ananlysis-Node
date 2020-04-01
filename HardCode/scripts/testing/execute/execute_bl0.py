import json
import os
import warnings
import requests
from HardCode.scripts.testing.execute.get_sms_json import get_sms

warnings.filterwarnings('ignore')


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires

    url = 'http://localhost:8888/api/token/'

    credentials = {'username': 'root', 'password': 'root@123'}

    res = requests.post(url=url, data=credentials, verify=False)
    res = res.json()
    return res['access']


def execute_bl0(**kwargs):
    user_id = kwargs.get('user_id')
    cibil_score = kwargs.get('cibil_score')
    cibil_xml = kwargs.get('cibil_file')

    cibil_xml = open(os.path.join('../input_data', cibil_xml), 'r')

    sms_json = open(os.path.join('../input_data', 'sms_data_' + str(user_id) + '.json'), 'rb')

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
    if not os.path.exists('../result'):
        os.mkdir('result')

    with open(os.path.join('../result', str(user_id) + '.json'), 'w', encoding='utf-8') as fp:
        json.dump(result, fp, ensure_ascii=False, indent=4)


def testing(user_id):
    if not os.path.exists(os.path.join('../input_data', 'sms_data_' + str(user_id) + '.json')):
        get_sms(user_id=user_id)

    try:
        if str(user_id).isdigit():
            if os.path.exists(os.path.join('../input_data', 'sms_data_' + str(user_id) + '.json')):

                # ==> cibil score is passed 807 by default

                execute_bl0(user_id=int(user_id), cibil_score=807,
                            cibil_file='cibil_data.xml')

                print(f"result generated successfully : {user_id}")
            else:
                msg = 'sms json does not exists'
                raise BaseException(msg)
        else:
            msg = 'user id must contain only numbers'
            raise BaseException(msg)

    except BaseException as e:
        print(f"the following error occurred : {e}")


testing(user_id=347737)
