from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from HardCode.scripts import BL0
from HardCode.scripts.apicreditdata import convert_to_df
import json

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def get_cibil_analysis(request):
    try:
        user_id = int(request.data.get('user_id'))
    except:
        return Response({'status': False, 'message': 'user_id parameter is required'}, 400)
    try:
        new_user = request.data.get('new_user')
        if new_user is None:
            raise Exception
        new_user = bool(int(new_user))
    except:
        return Response({'status': False, 'message': 'new_user parameter is required'}, 400)
    try:
        sms_json = request.data.get('sms_json').read().decode('utf-8')
        sms_json = json.loads(sms_json)
        if sms_json is None:
            raise Exception
    except:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)
    #try:
     #   cibil_xml = request.data.get('cibil_xml')
      #  if cibil_xml is None:
      #      raise Exception
   # except:
     #   return Response({'status': False, 'message': 'cibil_xml parameter is required'}, 400)
     
    try:
        cibil_score = request.data.get('cibil_score')
        if cibil_score is None:
            raise Exception
    except:
        return Response({'status': False, 'message': 'cibil_score parameter is required'}, 400)
    try:
        current_loan_amount = request.data.get('current_loan_amount')
        if current_loan_amount is None:
            raise Exception
    except:
        return Response({'status': False, 'message': 'current_loan_amount parameter is required'}, 400)

    try:
        all_loan_amount = request.data.get('all_loan_amount')
        if all_loan_amount is None:
            raise Exception
    except:
        return Response({'status': False, 'message': 'all_loan_amount parameter is required'}, 400)

    # call parser
    try:
        all_loan_amount = list(map(lambda x: int(float(x)), all_loan_amount.split(',')))
    except Exception as e:
        return Response({'status': False, 'message': 'all_loan_amount values must be int convertible'}, 400)

    try:
        current_loan_amount = int(current_loan_amount)
    except:
        return Response({'status': False, 'message': 'current_loan_amount parameter must be int convertible'}, 400)

    # response_parser = convert_to_df(user_id, cibil_xml)
    # if response_parser["status"]:
        # call node
        # response_parser["data"] removed (!= Cibil Analysis)
    try:
    
        ResponseCibilAnalysis = BL0.bl0(cibil_score, sms_json, user_id, new_user, all_loan_amount,
                                        current_loan_amount)
    except Exception as e:
        ResponseCibilAnalysis = {'status': False, 'message': e, 'onhold': None,
                                 'user_id': user_id, 'limit': 0,
                                 'logic': 'BL0'}

    return Response(ResponseCibilAnalysis, 200)
