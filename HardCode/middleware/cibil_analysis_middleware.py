from rest_framework.decorators import permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response


def get_something(request):
    try:
        user_id = int(request.GET.get('user_id'))
        filter_type = {
            'user_id': user_id
        }
    except:
        return Response({
            'error': 'user_id is a required parameter!!'
        }, 400)
    response = {}

    #call parser

    #call node

    return  Response(response)
