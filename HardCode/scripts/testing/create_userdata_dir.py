from time import sleep
import json
import requests
from analysisnode import Checksum
from analysisnode.settings import BASE_DIR, CHECKSUM_KEY, PROCESSING_DOCS
import os
from threadedprocess import ThreadedProcessPoolExecutor
from glob import glob
from HardCode.scripts.testing.user_ids import ids

API_ENDPOINT = 'http://127.0.0.1:8000/hard_code/bl0/'
API_ENDPOINT_1 = 'http://127.0.0.1:8000/hard_code/bl0/before_kyc/'
API_ENDPOINT_REJ = 'https://mlnode.credicxotech.com/hard_code/bl0/pre_rejection_status/'
API_FETCH = "http://127.0.0.1:8000/hard_code/bl0/fetch_params/"
API_SET = "http://127.0.0.1:8000/hard_code/bl0/set_params/"


def request_main(user_id):
    temp = {'user_id': user_id}
    files = {}
    headers = {
        'CHECKSUMHASH': Checksum.generate_checksum(temp, CHECKSUM_KEY)
    }
    temp['all_loan_amount'] = ["1000", "2000", "3000", "4000"]
    files['sms_json'] = open(
        f'C:/Users/shreya/Desktop/Users_Info/{user_id}/sms_data.json', 'rb')
    if os.path.exists(
            f'C:/Users/shreya/Desktop/Users_Info/{user_id}/cibil_data.xml'):
        files['cibil_xml'] = open(
            f'C:/Users/shreya/Desktop/Users_Info/{user_id}/cibil_data.xml', 'rb')

    print(headers, temp)
    print(requests.post(API_ENDPOINT, data=temp, headers=headers, files=files).text)


def test_before_kyc(user_id):
    temp = {'user_id': user_id}
    headers = {
        'CHECKSUMHASH': Checksum.generate_checksum(temp, CHECKSUM_KEY)
    }
    print(headers, temp)
    # print(requests.post(API_ENDPOINT_1, data=temp, headers=headers).text)
    print(requests.post(API_ENDPOINT_1, data=temp, headers=headers,
                        files={
                            'sms_json': open(
                                f'C:/Users/shreya/Desktop/Users_Info/{user_id}/sms_data.json')}).text)


def test_pre_rejection(user_id):
    temp = {'user_id': user_id}
    headers = {
        'CHECKSUMHASH': Checksum.generate_checksum(temp, CHECKSUM_KEY)
    }
    print(requests.post(API_ENDPOINT_REJ, data=temp, headers=headers).text)


def test_fetch_params():
    Token = "b1b4ed126feaddd525745e3cef0f71cd1bc1add6"
    headers = {"Authorization": f"Token {Token}"}
    print(requests.get(API_FETCH, headers=headers).text)


def test_set_params():
    Token = "b1b4ed126feaddd525745e3cef0f71cd1bc1add6"
    params = json.dumps({"name": "Anandesh", "job": "Data Engineer"})
    temp = {'params': params}
    headers = {"Authorization": f"Token {Token}"}
    print(requests.post(API_SET, headers=headers, data=temp).text)


def run_the_chunk(user_ids_chunk):
    with ThreadedProcessPoolExecutor(max_processes=8, max_threads=16) as p:
        p.map(request_main, user_ids_chunk)


if __name__ == "__main__":
    # os.chdir("/home/ravan/credicxo-projects/Users Data for AN/")
    # user_ids = list(map(int, glob("*")))
    # user_ids_with_chunks = list()
    # chunk_size = 0
    # chunks = list()
    # for user_id in user_ids:
    #     if chunk_size == 100:
    #         user_ids_with_chunks.append(chunks)
    #         chunk_size = 0
    #         chunks = list()
    #     chunks.append(user_id)
    #     chunk_size += 1
    # count = 0
    # for user_ids_chunk in user_ids_with_chunks:
    #     current_ids_processing_len = os.listdir(PROCESSING_DOCS)
    #     print(f'PROCESSING CHUNK : {count}')
    #     while len(current_ids_processing_len) != 0:
    #         sleep(10)
    #         current_ids_processing_len = os.listdir(PROCESSING_DOCS)
    #     run_the_chunk(user_ids_chunk)
    #     count += 1
    # os.chdir('users_data')
    # print(os.getcwd())
    request_main(123244)