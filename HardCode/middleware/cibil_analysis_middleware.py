from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from HardCode.scripts import bl0
from HardCode.scripts.apicreditdata import convert_to_df
from analysisnode.settings import BASE_DIR
import json


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def get_something(request):
    try:
        user_id = int(request.POST.get('user_id'))
    except:
        return Response({'error': 'user_id parameter is required'}, 400)
    try:
        new_user = int(request.POST.get('new_user'))
    except:
        return Response({'error': 'new_user parameter is required'}, 400)
    try:
        sms_json = int(request.POST.get('sms_json'))
    except:
        return Response({'error': 'sms_json parameter is required'}, 400)
    try:
        cibil_xml = int(request.GET.get('cibil_xml'))
    except:
        return Response({'error': 'cibil_xml parameter is required'}, 400)

    # call parser

    response_parser = convert_to_df(user_id, cibil_xml)
    if response_parser["status"]:
        # call node
        ResponseCibilAnalysis = bl0.cibil_analysis(response_parser["data"], sms_json, user_id, new_user)

    else:
        ResponseCibilAnalysis = {'Status': False, 'Message': 'None', 'Onhold': False, 'User_id': user_id, 'Limit': 0,
                                 'Logic': 'BL0'}

    return Response(ResponseCibilAnalysis, 200)
