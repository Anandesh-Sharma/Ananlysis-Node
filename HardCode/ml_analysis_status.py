import json
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY, FINAL_RESULT


@api_view(['POST'])
@permission_classes((AllowAny,))
def get_cibil_analysis_status(request):
    try:
        response = request.data
        if not verify_checksum(response, CHECKSUM_KEY, request.headers['CHECKSUMHASH']):
            raise ValueError
    except (AttributeError, ValueError, KeyError):
        return Response({'error': 'INVALID CHECKSUMH!!!'}, 400)
    user_id = request.data.get('user_id')
    try:
        return Response(json.load(open(FINAL_RESULT + str(user_id) + '/user_data.json')))
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)