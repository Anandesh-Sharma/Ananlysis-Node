import json

from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from HardCode.scripts.BL0 import bl0
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY, PROCESSING_DOCS
import os


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
        sms_json = request.FILES['sms_json']
        try:
            os.makedirs(PROCESSING_DOCS + str(user_id)+"_3")
        except FileExistsError:
            pass
        with open(PROCESSING_DOCS + str(user_id)+"_3" + '/sms_data.json', 'wb+') as destination:
            for chunk in sms_json.chunks():
                destination.write(chunk)
    except:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)

    # WRITE THE FUNCTION BELOW
    try:
        with open(PROCESSING_DOCS + str(user_id) + '/user_data.json', 'w') as json_file:
            json.dump({
                'step': 3
            }, json_file, ensure_ascii=True, indent=4)
        return Response({"status": True, "message": "Files Received for Before_Loan"}, 200)
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)
