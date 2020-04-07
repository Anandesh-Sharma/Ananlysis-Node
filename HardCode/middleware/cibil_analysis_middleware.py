import json
import os
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from analysisnode.Checksum import verify_checksum
from analysisnode.settings import CHECKSUM_KEY, PROCESSING_DOCS


@api_view(['POST'])
@permission_classes((AllowAny,))
def get_cibil_analysis(request):
    try:
        response = request.data
        try:
            del (response['sms_json'])
        except:
            pass
        try:
            del (response['cibil_xml'])
        except:
            pass
        if not verify_checksum(response, CHECKSUM_KEY, request.headers['CHECKSUMHASH']):
            raise ValueError
    except (AttributeError, ValueError, KeyError):
        return Response({'error': 'INVALID CHECKSUM!!!'}, 400)
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
        sms_json = request.FILES['sms_json']
        try:
            os.makedirs(PROCESSING_DOCS + str(user_id))
        except FileExistsError:
            pass
        with open(PROCESSING_DOCS + str(user_id) + '/sms_data.json', 'wb+') as destination:
            for chunk in sms_json.chunks():
                destination.write(chunk)

        if sms_json is None:
            raise Exception

    except TypeError:
        return Response({'status': False, 'message': 'sms_json parameter is required'}, 400)

    try:
        cibil_xml = request.FILES['cibil_xml']
        with open(PROCESSING_DOCS + str(user_id) + '/experian_cibil.xml', 'wb+') as destination:
            for chunk in cibil_xml.chunks():
                destination.write(chunk)
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
    with open(PROCESSING_DOCS + str(user_id) + '/user_data.json', 'w') as json_file:
        json.dump({
        'current_loan_amount': current_loan_amount,
        'all_loan_amount': all_loan_amount,
        'cibil_score': cibil_score,
        'user_id': user_id,
        'new_user': new_user,
    }, json_file, ensure_ascii=True, indent=4)
        # return Response({'status': False, 'message': 'current_loan_amount parameter must be int convertible'}, 400)
    return Response({'message': 'FILES RECEIVED!!'})
