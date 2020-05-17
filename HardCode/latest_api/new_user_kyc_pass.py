import json
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY
# import a updation function


@api_view(['POST'])
@permission_classes((AllowAny,))
def new_user_kyc_pass(request):
    try:
        response = request.data
        try:
            del response['sms_json']
        except:
            pass
        if not verify_checksum(response, CHECKSUM_KEY, request.headers['CHECKSUMHASH']):
            raise ValueError
    except (AttributeError, ValueError, KeyError):
        return Response({'error': 'INVALID CHECKSUM!!!'}, 400)

    # get params
    try:
        user_id = int(request.data.get('user_id'))
    except:
        user_id = -1
    try:
        sms_json = json.load(request.FILES['sms_json'], )
    except:
        sms_json = {}
    try:
        # return Response(new_user_kyc_pass_function(user_id, sms_json))
        pass
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)
