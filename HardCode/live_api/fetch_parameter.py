from HardCode.scripts.Util import conn
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response


@api_view(['GET'])
@permission_classes((IsAuthenticated,))
def fetch_inputs(request):
    try:
        try:
            connect = conn()
            input_model = connect.analysis.dynamic_input.find_one({'model': 'decision_value_bl0'})
            input_model = input_model['data']
            data = {'status': True, 'message': 'success', "input_model": input_model}
            connect.close()
            return Response(data)
        except BaseException as e:
            return Response({'status': False, 'message': str(e)})
    except:
        return Response({'status': False, 'message': 'unauthorized access'}, 400)
