from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from HardCode.scripts import BL0
from HardCode.scripts.cibil.Analysis import analyse
from HardCode.scripts.cibil.apicreditdata import convert_to_df
import json
import ast


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
        pass

    try:
        sms_json = request.data.get('sms_json').read().decode('utf-8')
        sms_json = json.loads(sms_json)
        if sms_json is None:
            raise Exception
    except:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)

    try:
        cibil_xml = request.data.get('cibil_xml')
    except:
        pass

    try:
        cibil_score = request.data.get('cibil_score')
        if cibil_score is None:
            raise Exception
    except:
        pass
    try:
        current_loan_amount = request.data.get('current_loan_amount')
        if current_loan_amount is None:
            raise Exception
    except:
        pass

    try:
        all_loan_amount = request.data.get('all_loan_amount')
        if all_loan_amount is None:
            raise Exception
    except:
        pass

    # call parser
    try:
        all_loan_amount = list(map(lambda x: int(float(x)), all_loan_amount.split(',')))
    except:
        pass

    try:
        current_loan_amount = int(current_loan_amount)
    except:
        pass

    cibil_df = {'status': False, 'data': None, 'message': 'None'}
    if cibil_xml:
        response_parser = convert_to_df(cibil_xml)
        cibil_df = response_parser

    try:
        response_bl0 = BL0.bl0(cibil_xml=cibil_df, cibil_score=cibil_score, user_id=user_id
                               , new_user=new_user, list_loans=all_loan_amount,
                               current_loan=current_loan_amount, sms_json=sms_json,cibil_file=cibil_xml)
        return Response(response_bl0, 200)
    except Exception as e:
        print(f"error in middleware {e}")
        import traceback
        traceback.print_tb(e.__traceback__)
        limit = analyse(user_id=user_id, current_loan=current_loan_amount, cibil_df=cibil_df, new_user=new_user,
                        cibil_score=cibil_score)
        response_bl0 = {
            "cust_id": user_id,
            "status": True,
            "message": "Exception occurred, I feel lonely in middleware",
            "result": {
                "loan_salary": -9,
                "loan": -9,
                "salary": -9,
                "cibil": limit
            }
        }
    return Response(response_bl0, 200)
