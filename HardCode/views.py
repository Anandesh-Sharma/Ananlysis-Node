from django.shortcuts import render
from django.views.generic.base import View
from .middleware.cibil_analysis_middleware import get_something
from rest_framework.response import Response


# @ensure_csrf_cookie

class CibilAnalysis(View):

    def post(self, request):
        #print(request.POST)

        return get_something(request,)
