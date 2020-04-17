import json
import os
import warnings
import requests
from HardCode.scripts.testing.execute.data_fetch import get_sms, get_cibil
from HardCode.scripts.testing.user_ids import *

warnings.filterwarnings('ignore')
from tqdm import tqdm
import pandas as pd


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
    # df = {"cust_id": user_id,"score": [result['Model_0']['score']],
    #        "secured": [result['Model_0']['parameters']['deduction_parameters']['secured_unsecured_val']['secured_unsecured_loans_count']['secured']],
    #       "unsecured": [result['Model_0']['parameters']['deduction_parameters']['secured_unsecured_val']['secured_unsecured_loans_count']['unsecured_count']],
    #
    #          "age_of_oldest_trade":  [result['Model_0']['parameters']['deduction_parameters']['age_of_oldest_trade_val']['age_of_oldest_trade']],
    #         "active_count":  [result['Model_0']['parameters']['deduction_parameters']['active_close_val']['active_count']],
    #       "close_count":[result['Model_0']['parameters']['deduction_parameters']['active_close_val']['closed_count']],
    #         "loan_app_count_percentage": [result['Model_0']['parameters']['deduction_parameters']['loan_app_count_val']['loan_app_count']],
    #          'verification_similarity': [result['Model_0']['parameters']['deduction_parameters']['reference_val']['reference']['result']['similarity_score']],
    #          "due_days":  [result['Model_0']['parameters']['deduction_parameters']['loan_val']['due_days']],
    #       "max_loan_limit": [result['Model_0']['parameters']['deduction_parameters']['loan_val']['max_limit']],
    #       "ecs_count": [result['Model_0']['parameters']['deduction_parameters']['ecs_val']['ecs_count']],
    #       "payment_rating":[result['Model_0']['parameters']['deduction_parameters']['payment_rating_val']['payment_rating']],
    #
    #       "available_balance":[result['Model_0']['parameters']['deduction_parameters']['available_balance_val']['available_balance']['balance_on_loan_date']],
    #       "last_month_avbl_bal":[result['Model_0']['parameters']['deduction_parameters']['available_balance_val']['available_balance']['last_month_bal']],
    #       "scnd_last_month_bal":[result['Model_0']['parameters']['deduction_parameters']['available_balance_val']['available_balance']['second_last_month_bal']],
    #       "third_last_month_bal":[result['Model_0']['parameters']['deduction_parameters']['available_balance_val']['available_balance']['third_last_month_bal']],
    #       "loan_app_list":[result['Model_0']['parameters']['deduction_parameters']['loan_val']['loan_app_list']],
    #       "rejection_reason": [result['Model_0']['rejection_reasons']],
    #       }
    #
    # data = pd.DataFrame.from_dict(df)
    # data.to_csv('result_both_emi_npa.csv', mode='a', header=False)



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


user_id = input('enter user id: ')
testing(user_id=user_id)

l = npa_ids1
l.sort(reverse=True)
print(len(l))
print(l)

# from concurrent.futures import ThreadPoolExecutor

# with ThreadPoolExecutor() as exc:
#     exc.map(testing,(i for i in l))


# for i in tqdm(l[:200]):
#     testing(i)
