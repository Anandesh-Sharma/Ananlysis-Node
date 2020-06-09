import json
import os
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY, PROCESSING_DOCS


@api_view(['POST'])
@permission_classes((AllowAny,))
def update_analysis(request):
    try:
        response = request.data
        if not verify_checksum(response, CHECKSUM_KEY, request.headers['CHECKSUMHASH']):
            raise ValueError
    except (AttributeError, ValueError, KeyError):
        return Response({'error': 'INVALID CHECKSUM!!!'}, 400)
    try:
        user_id = int(request.data.get('user_id'))
    except:
        return Response({'status': False, 'message': 'user_id parameter is required'}, 400)

    try:
        sms_json = json.load(request.FILES['sms_json'])
        try:
            os.makedirs(PROCESSING_DOCS + str(user_id)+"_0")
        except FileExistsError:
            pass
        with open(PROCESSING_DOCS + str(user_id)+"_0" + '/sms_data.json', 'wb+') as destination:
            for chunk in sms_json.chunks():
                destination.write(chunk)
    except:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)

    try:
        return Response({"status": True, "message": "Files Received for Updation"}, 200)
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)
