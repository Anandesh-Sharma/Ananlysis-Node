import json
import os
from datetime import datetime
from time import sleep
import shutil
import pytz
from HardCode.scripts import BL0
from HardCode.scripts.Util import conn
from HardCode.scripts.cibil.apicreditdata import convert_to_df
from analysisnode.settings import PROCESSING_DOCS, CHECKSUM_KEY, FINAL_RESULT
from concurrent.futures import ProcessPoolExecutor
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
            response_bl0 = {
                "status": True,
                "cust_id": user_id,
                "message": "No messages found in sms_json",
                "result": False
            }
        else:

            response_bl0 = BL0.bl0(cibil_xml=cibil_df, cibil_score=user_data['cibil_score'], user_id=int(user_id)
                                   , new_user=user_data['new_user'], list_loans=user_data['all_loan_amount'],
                                   current_loan=user_data['current_loan_amount'], sms_json=sms_json)
        shutil.rmtree(PROCESSING_DOCS + str(user_id))

    except Exception as e:
        print(f"error in middleware {e}")
        response_bl0 = {
            "cust_id": user_id,
            "status": False,
            "message": str(e),
            "result": False
        }

    response_bl0['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
    temp_response_bl0 = response_bl0
    del temp_response_bl0["cust_id"]
    conn().analysisresult.bl0.update_one({'cust_id': int(user_id)}, {"$push": {
        "result": temp_response_bl0}}, upsert=True)

    print(requests.post(API_ENDPOINT, data=response_bl0,
                        headers={'CHECKSUMHASH': Checksum.generate_checksum(response_bl0, CHECKSUM_KEY)}).json())


def process_user_records(user_ids):
    with ProcessPoolExecutor() as p:
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
        sleep(5)
