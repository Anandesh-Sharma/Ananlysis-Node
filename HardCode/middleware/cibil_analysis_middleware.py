from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from HardCode.scripts import BL0
from HardCode.scripts.apicreditdata import convert_to_df


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def get_cibil_analysis(request):
    try:
        user_id = int(request.data.get('user_id'))
    except:
        return Response({'status': False,'message': 'user_id parameter is required'}, 400)
    try:
        new_user = bool(request.data.get('new_user'))
    except:
        return Response({'status': False,'message': 'new_user parameter is required'}, 400)
    try:
        sms_json = request.data.get('sms_json')
    except:
        return Response({'status': False,'message': 'sms_json parameter is required'}, 400)
    try:
        cibil_xml = request.data.get('cibil_xml')
    except:
        return Response({'status': False,'message': 'cibil_xml parameter is required'}, 400)

    # call parser

    response_parser = convert_to_df(user_id, cibil_xml)
    if response_parser["status"]:
        # call node
        ResponseCibilAnalysis = BL0.bl0(response_parser["data"], sms_json, user_id, new_user)

    else:
        ResponseCibilAnalysis = {'status': False, 'message': 'xml parser status is false', 'onhold': None, 'user_id': user_id, 'limit': 0,
                                 'logic': 'BL0'}

    return Response(ResponseCibilAnalysis, 200)
