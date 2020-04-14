import requests
from analysisnode import Checksum
from analysisnode.settings import BASE_DIR, CHECKSUM_KEY
import os
from glob import glob
API_ENDPOINT = 'http://127.0.0.1:8000/hard_code/bl0/'


def request_main(user_id):
    temp = {'user_id': user_id}
    headers = {
        'CHECKSUMHASH': Checksum.generate_checksum(temp, CHECKSUM_KEY)
    }
    temp['all_loan_amount'] = [1000, 2000, 3000, 4000]
    print(headers, temp)
    print(requests.post(API_ENDPOINT, data=temp, headers=headers,
                        files={
                            'sms_json': open(f'/home/iam/Downloads/credicxo-projects/Users Data for AN/{user_id}/sms_data.json')}).text)


if __name__ == "__main__":
    os.chdir("/home/iam/Downloads/credicxo-projects/Users Data for AN/")
    user_ids = list(map(int, glob("*")))
    print(user_ids)
    for user_id in user_ids:
        request_main(user_id)

