import requests
from analysisnode import Checksum
from analysisnode.settings import BASE_DIR, CHECKSUM_KEY
import os
from glob import glob
API_ENDPOINT = 'http://127.0.0.1:8000/hard_code/bl0/'
API_ENDPOINT_REJ = 'http://127.0.0.1:8000/hard_code/bl0/pre_rejection_status/'


def request_main(user_id):
    temp = {'user_id': user_id}
    headers = {
        'CHECKSUMHASH': Checksum.generate_checksum(temp, CHECKSUM_KEY)
    }
    temp['all_loan_amount'] = [1000, 2000, 3000, 4000]
    print(headers, temp)
    print(requests.post(API_ENDPOINT, data=temp, headers=headers,
                        files={
                            'sms_json': open(f'/home/ravan/credicxo-projects/Users Data for AN/{user_id}/sms_data.json')}).text)


def test_pre_rejection(user_id):
    temp = {'user_id': user_id}
    headers = {
        'CHECKSUMHASH': Checksum.generate_checksum(temp, CHECKSUM_KEY)
    }
    print(requests.post(API_ENDPOINT_REJ, data=temp, headers=headers).text)


if __name__ == "__main__":
    os.chdir("/home/ravan/credicxo-projects/Users Data for AN/")
    # user_ids = list(map(int, glob("*")))
    # for user_id in user_ids:
    #     request_main(user_id)
    request_main(223751)
    # test_pre_rejection(223751)
