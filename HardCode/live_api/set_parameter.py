from HardCode.scripts.Util import conn
from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
import json


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def set_inputs(request):
    try:
        try:
            data = json.loads(request.data.get('params'))
            client = conn()
            client.analysis.dynamic_input.update_one({"model": "decision_value_bl0"}, {"$set": {"data": data}},
                                                     upsert=True)
            return Response({'status': True, 'message': "data receviced successfully"})
        except BaseException as e:
            return Response({'status': False, 'message': str(e)})
    except:
        return Response({'status': False, 'message': 'unauthorized access'}, 400)
