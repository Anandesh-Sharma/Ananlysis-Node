from django.views.generic.base import View
from .middleware.cibil_analysis_middleware import get_cibil_analysis
from .ml_analysis_status import get_cibil_analysis_status


class CibilAnalysis(View):
    def post(self, request):
        return get_cibil_analysis(request,)


class CibilAnalysisStatus(View):
    def post(self, request):
        return get_cibil_analysis_status(request)
