import json

from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from HardCode.scripts.BL0 import bl0
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY


@api_view(['POST'])
@permission_classes((AllowAny,))
def final_results(request):
    print(request.data)
    try:

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
        sms_json = json.load(request.FILES['sms_json'])
    except:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)

    # WRITE THE FUNCTION BELOW
    try:
        response = bl0(user_id=user_id, sms_json=sms_json)
        if response['status']:
            response['result_type'] = 'before_loan'
            final_response = response
        else:
            final_response = {"status": False,
                              "cust_id": user_id,
                              "result_type": "before_loan",
                              "result": False}

        return Response(final_response, 200)
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)
