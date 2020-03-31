from rest_framework.decorators import permission_classes, api_view
# from HardCode.scripts.classification import run_classifier
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
        new_user = request.data.get('new_user', '1')
        if new_user is None:
            raise Exception
        new_user = bool(int(new_user))
    except:
        pass

        # return Response({'status': False, 'message': 'new_user parameter is required'}, 400)
        # try:
        #     # Bool
        #     only_classifier = request.data.get('classify_message')
        #     only_classifier = ast.literal_eval(only_classifier)
        # except:
        #     pass
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
        # return Response({'status': False, 'message': 'cibil_xml parameter is required'}, 400)

    try:
        cibil_score = request.data.get('cibil_score', 600)
        if cibil_score is None:
            raise Exception
    except:
        pass
        # return Response({'status': False, 'message': 'cibil_score parameter is required'}, 400)
    try:
        current_loan_amount = request.data.get('current_loan_amount', 0)
        if current_loan_amount is None:
            raise Exception
    except:
        pass
        # return Response({'status': False, 'message': 'current_loan_amount parameter is required'}, 400)

    try:
        all_loan_amount = request.data.get('all_loan_amount', '1000,2000,3000,4000')
        if all_loan_amount is None:
            raise Exception
    except:
        pass
        # return Response({'status': False, 'message': 'all_loan_amount parameter is required'}, 400)

    # call parser
    try:
        all_loan_amount = list(map(lambda x: int(float(x)), all_loan_amount.split(',')))
    except:
        pass
        # return Response({'status': False, 'message': 'all_loan_amount values must be int convertible'}, 400)

    try:
        current_loan_amount = int(current_loan_amount)
    except:
        pass
        # return Response({'status': False, 'message': 'current_loan_amount parameter must be int convertible'}, 400)

    cibil_df = {'status': False, 'data': None, 'message': 'None'}
    if cibil_xml:
        response_parser = convert_to_df(cibil_xml)
        cibil_df = response_parser

    # try:
    #     if only_classifier:
    #         response_classifier = run_classifier(user_id=user_id, sms_json=sms_json)
    #         return Response(response_classifier, 200)
    # except BaseException as e:
    #     print(f"Error in classification {e}")
    #     response_classifier = False
    #     return Response(response_classifier, 400)

    try:
        response_bl0 = BL0.bl0(cibil_xml=cibil_df, cibil_score=cibil_score, user_id=user_id
                               , new_user=new_user, list_loans=all_loan_amount,
                               current_loan=current_loan_amount, sms_json=sms_json)
        return Response(response_bl0, 200)
    except Exception as e:
        print(f"error in middleware {e}")
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
