from django.views.generic.base import View
from .middleware.cibil_analysis_middleware import get_cibil_analysis


class CibilAnalysis(View):

    def post(self, request):
        #print(request.POST)

        return get_cibil_analysis(request,)
