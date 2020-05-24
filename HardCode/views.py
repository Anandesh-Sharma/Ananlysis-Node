from django.views.generic.base import View
from .middleware.cibil_analysis_middleware import get_cibil_analysis
from HardCode.live_api.ml_analysis_status import get_cibil_analysis_status
from HardCode.live_api.pre_rejection_status import get_pre_rejection_status
from HardCode.live_api.fetch_parameter import fetch_inputs
from HardCode.live_api.set_parameter import set_inputs


class CibilAnalysis(View):
    def post(self, request):
        return get_cibil_analysis(request, )


class CibilAnalysisStatus(View):
    def post(self, request):
        return get_cibil_analysis_status(request, )


class PreRejection(View):
    def post(self, request):
        return get_pre_rejection_status(request, )


class FetchParameter(View):
    def get(self, request):
        return fetch_inputs(request, )


class SetParameter(View):
    def post(self, request):
        return set_inputs(request, )
