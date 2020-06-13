# from datetime import datetime
# import pytz
# import requests
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
import json
import os
# from analysisnode import Checksum
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY, PROCESSING_DOCS
# from HardCode.scripts.before_kyc import before_kyc_function
# from HardCode.scripts.Util import conn, logger_1
from HardCode.scripts.BL0 import bl0

# API_ENDPOINT = 'https://testing.credicxotech.com/api/ml_analysis/callback/'


@api_view(['POST'])
@permission_classes((AllowAny,))
def get_pre_rejection_status(request):
    try:
        print(request.data)
        if not verify_checksum({'user_id': int(request.data.get('user_id'))}, CHECKSUM_KEY,
                               request.headers['CHECKSUMHASH']):
            raise ValueError
    except (AttributeError, ValueError, KeyError):
        return Response({'error': 'INVALID CHECKSUM!!!'}, 400)
    try:
        user_id = int(request.data.get('user_id'))
    except:
        return Response({'status': False, 'message': 'user_id parameter is required'}, 400)
    try:
        sms_json = request.FILES['sms_json']
    except:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)
    try:
        os.makedirs('PROCESSING_DOCS/' + str(user_id)+"_1")
    except FileExistsError:
        pass
    try:
        with open('PROCESSING_DOCS/' + str(user_id) + "_1" + '/sms_data.json', 'wb+') as destination:
            for chunk in sms_json.chunks():
                destination.write(chunk)
    except BaseException as e:
        print(str(e))
    try:
        contacts = request.FILES['contacts']
        with open('PROCESSING_DOCS/' + str(user_id) + "_1" + '/contacts.csv', 'wb+') as destination:
            for chunk in contacts.chunks():
                destination.write(chunk)
    except:
        return Response({'status': False, 'message': 'contacts parameter is required'}, 400)
    try:
        app_data = json.loads(request.data.get('app_data'))
        with open('PROCESSING_DOCS/' + str(user_id) + "_1" + '/app_data.json', 'w+') as destination:
            json.dump(app_data, destination)
    except Exception as e:
        print(e)
        return Response({'status': False, 'message': 'app_data parameter is required'}, 400)
    try:
        # result = before_kyc_function(user_id=user_id, sms_json=sms_json, contacts=contacts, app_data=app_data)
        # temp_result = result
        # temp_result['modified_at'] = str(datetime.now(pytz.timezone('Asia/Kolkata')))
        # conn().analysisresult.before_kyc.update_one({'cust_id': int(user_id)}, {"$push": {
        #     "result": temp_result}}, upsert=True)
        # result['cust_id'] = user_id
        # print(requests.post(API_ENDPOINT, data=result,
        #                     headers={'CHECKSUMHASH': Checksum.generate_checksum(result, CHECKSUM_KEY)}).json())
        #
        # conn().analysisresult.before_kyc.update_one({'cust_id': int(user_id)}, {"$push": {
        #     "result": result}}, upsert=True
        return Response({"status": True, "message": "Files Received for Before_Kyc"}, 200)
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)
