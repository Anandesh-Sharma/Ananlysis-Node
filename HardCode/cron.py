import json
import os
import requests
from HardCode.scripts import BL0
from HardCode.scripts.cibil.Analysis import analyse
from HardCode.scripts.cibil.apicreditdata import convert_to_df
from analysisnode import Checksum
from analysisnode.settings import PROCESSING_DOCS, CHECKSUM_KEY, FINAL_RESULT

API_ENDPOINT = 'https://testing.credicxotech.com/api/ml_analysis/callback/'


def process_user_records():
    directories = os.listdir(PROCESSING_DOCS)
    for user_id in directories:
        locals().update(json.load(open(PROCESSING_DOCS + str(user_id) + '/user_data.json')))
        cibil_df = {'status': False, 'data': None, 'message': 'None'}
        if os.path.exists(PROCESSING_DOCS + str(user_id) + '/experian_cibil.xml'):
            response_parser = convert_to_df(open(PROCESSING_DOCS + str(user_id) + '/experian_cibil.xml'))
            cibil_df = response_parser

        # try:
        #     if only_classifier:
        #         response_classifier = run_classifier(user_id=user_id, sms_json=sms_json)
        #         return Response(response_classifier, 200)
        # except BaseException as e:
        #     print(f"Error in classification {e}")
        #     response_classifier = False
        #     return Response(response_classifier, 400)

        try:
            response_bl0 = BL0.bl0(cibil_xml=cibil_df, cibil_score=cibil_score, user_id=user_id
                                   , new_user=new_user, list_loans=all_loan_amount,
                                   current_loan=current_loan_amount, sms_json=sms_json)
            os.removedirs(PROCESSING_DOCS + str(user_id))

            try:
                os.makedirs(FINAL_RESULT + str(user_id))
            except FileExistsError:
                pass

            with open(FINAL_RESULT + str(user_id) + '/user_data.json', 'w') as json_file:
                json.dump(response_bl0, json_file, ensure_ascii=True, indent=4)

        except Exception as e:
            print(f"error in middleware {e}")
            limit = analyse(user_id=user_id, current_loan=current_loan_amount, cibil_df=cibil_df, new_user=new_user,
                            cibil_score=cibil_score)
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
        print(requests.post(API_ENDPOINT, data=response_bl0,
                            headers={'CHECKSUMHASH': Checksum.generate_checksum(response_bl0, CHECKSUM_KEY)}).json())