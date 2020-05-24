import json
import os
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from HardCode.scripts.BL0 import bl0
from HardCode.scripts.cibil.apicreditdata import convert_to_df
# from analysisnode.Checksum import verify_checksum
# from analysisnode.settings import CHECKSUM_KEY, PROCESSING_DOCS


@api_view(['POST'])
# @permission_classes((AllowAny,))
def get_cibil_analysis(request):
    try:
        user_id = int(request.data.get('user_id'))
    except:
        return Response({'status': False, 'message': 'user_id parameter is required'}, 400)

    try:
        sms_json = request.data.get('sms_json').read().decode('utf-8')
        sms_json = json.loads(sms_json)
        if sms_json is None:
            raise Exception
    except:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)

    try:
        cibil_xml = request.data.get('cibil_xml')
    except:
        pass

    cibil_df = {'status': False, 'data': None, 'message': 'None'}
    if cibil_xml:
        response_parser = convert_to_df(cibil_xml)
        cibil_df = response_parser

    try:
        response_bl0 = bl0(cibil_xml = cibil_df, user_id = user_id
                               , sms_json = sms_json)
        return Response(response_bl0, 200)
    except Exception as e:
        print(f"error in middleware {e}")
        import traceback
        traceback.print_tb(e.__traceback__)
        response_bl0 = {
            "cust_id": user_id, 
            "status": True, 
            "message": "Exception occurred, I feel lonely in middleware", 
            "result": {
                "loan_salary": -9, 
                "loan": -9, 
                "salary": -9, 
                "cibil": 0
            }
        }
    return Response(response_bl0)