from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY


@api_view(['POST'])
@permission_classes((AllowAny,))
def final_results(request):
    try:
        response = request.data
        # try:
        #     del response['sms_json']
        # except:
        #     pass
        # try:
        #     del response['cibil_xml']
        # except:
        #     pass
        # try:
        #     del response['new_customer']
        # except:
        #     pass
        if not verify_checksum(response, CHECKSUM_KEY, request.headers['CHECKSUMHASH']):
            raise ValueError
    except (AttributeError, ValueError, KeyError):
        return Response({'error': 'INVALID CHECKSUM!!!'}, 400)
    try:
        user_id = int(request.data.get('user_id'))
    except:
        return Response({'status': False, 'message': 'user_id parameter is required'}, 400)
    # try:
    #     sms_json = request.FILES['sms_json']
    # except:
    #     return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)
    # try:
    #     cibil_xml = request.FILES['cibil_xml']
    # except:
    #     pass

    # WRITE THE FUNCTION BELOW
    try:
        return Response({'user_id': user_id, 'Passed or failed': False})
    except FileNotFoundError:
        return Response({
            'error': 'Results awaited for ' + str(user_id) + '!!'
        }, 400)
