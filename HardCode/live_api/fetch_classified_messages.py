import json
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY, FINAL_RESULT
from HardCode.scripts.fetch_results.fetch_results_mdb import fetch_user_messages

@api_view(['POST'])
@permission_classes((AllowAny,))
def fetch_classified_messages(request):
    try:
        response = request.data
        print(response)
        if not verify_checksum(response, CHECKSUM_KEY, request.headers['CHECKSUMHASH']):
            raise ValueError
    except (AttributeError, ValueError, KeyError):
        return Response({'error': 'INVALID CHECKSUM!!!'}, 400)
    user_id = request.data.get('user_id')
    try:
        return Response(fetch_user_messages(user_id))
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)
