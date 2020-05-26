import json
import os
from time import sleep
import shutil
from HardCode.scripts import BL0
from HardCode.scripts.cibil.Analysis import analyse
from HardCode.scripts.cibil.apicreditdata import convert_to_df
from analysisnode.settings import PROCESSING_DOCS, CHECKSUM_KEY, FINAL_RESULT
from threadedprocess import ThreadedProcessPoolExecutor
from analysisnode import Checksum
import requests

API_ENDPOINT = 'https://api.credicxotech.com/api/ml_analysis/callback/'


def parallel_proccess_user_records(user_id):
    user_data = json.load(open(PROCESSING_DOCS + str(user_id) + '/user_data.json'))
    cibil_df = {'status': False, 'data': None, 'message': 'None'}
    if os.path.exists(PROCESSING_DOCS + str(user_id) + '/experian_cibil.xml'):
        response_parser = convert_to_df(open(PROCESSING_DOCS + str(user_id) + '/experian_cibil.xml'))
        cibil_df = response_parser
    sms_json = json.load(open(PROCESSING_DOCS + str(user_id) + '/sms_data.json', 'rb'))

    try:
        if len(sms_json) == 0:
            limit = analyse(user_id=user_id, current_loan=user_data['current_loan_amount'], cibil_df=cibil_df,
                            new_user=user_data['new_user'], cibil_score=user_data['cibil_score'])
            response_bl0 = {
                "cust_id": user_id,
                "status": True,
                "message": "No messages found in sms_json",
                "result": {
                    "loan_salary": -9,
                    "loan": -9,
                    "salary": -9,
                    "cibil": limit
                }
            }
        else:

            response_bl0 = BL0.bl0(cibil_xml=cibil_df, cibil_score=user_data['cibil_score'], user_id=int(user_id)
                                   , new_user=user_data['new_user'], list_loans=user_data['all_loan_amount'],
                                   current_loan=user_data['current_loan_amount'], sms_json=sms_json)
        shutil.rmtree(PROCESSING_DOCS + str(user_id))

        try:
            os.makedirs(FINAL_RESULT + str(user_id))
        except FileExistsError:
            pass

    except Exception as e:
        print(f"error in middleware {e}")
        limit = analyse(user_id=user_id, current_loan=user_data['current_loan_amount'], cibil_df=cibil_df,
                        new_user=user_data['new_user'], cibil_score=user_data['cibil_score'])
        response_bl0 = {
            "cust_id": user_id,
            "status": True,
            "message": "Exception occurred, I feel lonely in middleware",
            "result": {
                "loan_salary": -9,
                "loan": -9,
                "salary": -9,
                "cibil": limit
            }
        }
    with open(FINAL_RESULT + str(user_id) + '/user_data.json', 'w') as json_file:
        json.dump(response_bl0, json_file, ensure_ascii=True, indent=4)
    print(requests.post(API_ENDPOINT, data=response_bl0,
                        headers={'CHECKSUMHASH': Checksum.generate_checksum(response_bl0, CHECKSUM_KEY)}).json())


def process_user_records(user_ids):
    with ThreadedProcessPoolExecutor(max_processes=8, max_threads=16) as p:
        p.map(parallel_proccess_user_records, user_ids)


if __name__ == "__main__":
    while True:
        no_of_dirs = len(os.listdir(PROCESSING_DOCS))
        if no_of_dirs > 0:
            directories = os.listdir(PROCESSING_DOCS)
            user_ids = [user_id for user_id in directories]
            process_user_records(user_ids)
            print("***********")
            print("Done : ")
            print(user_ids)
        print("SLEEPING.....zzzzzz")
        sleep(10)
