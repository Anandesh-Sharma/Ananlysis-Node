import json
import os
import requests
import shutil
from HardCode.scripts import BL0
from HardCode.scripts.cibil.Analysis import analyse
from HardCode.scripts.cibil.apicreditdata import convert_to_df
from analysisnode import Checksum
from analysisnode.settings import PROCESSING_DOCS, CHECKSUM_KEY, FINAL_RESULT
from threadedprocess import ThreadedProcessPoolExecutor


# API_ENDPOINT = 'https://api.credicxotech.com/api/ml_analysis/callback/'


def parallel_proccess_user_records(user_id):
    user_data = json.load(open(PROCESSING_DOCS + str(user_id) + '/user_data.json'))
    cibil_df = {'status': False, 'data': None, 'message': 'None'}
    if os.path.exists(PROCESSING_DOCS + str(user_id) + '/experian_cibil.xml'):
        response_parser = convert_to_df(open(PROCESSING_DOCS + str(user_id) + '/experian_cibil.xml'))
        cibil_df = response_parser
    sms_json = json.load(open(PROCESSING_DOCS + str(user_id) + '/sms_data.json'))

    try:
        if len(sms_json) == 0:
            BL0.set_processing_bool(user_id, False)
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
    # print(requests.post(API_ENDPOINT, data=response_bl0,
    #                     headers={'CHECKSUMHASH': Checksum.generate_checksum(response_bl0, CHECKSUM_KEY)}).json())


def process_user_records():
    directories = os.listdir(PROCESSING_DOCS)
    user_ids = [user_id for user_id in directories]
    print(user_ids)
    for user_id in user_ids:
        parallel_proccess_user_records(user_id)
    # with ThreadedProcessPoolExecutor(max_processes=8, max_threads=16) as p:
    #     p.map(parallel_proccess_user_records, user_ids)


process_user_records()
