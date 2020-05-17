from django.views.generic.base import View
from .middleware.cibil_analysis_middleware import get_cibil_analysis
from .ml_analysis_status import get_cibil_analysis_status
from HardCode.pre_rejection_status import get_pre_rejection_status
from HardCode.latest_api.before_kyc import before_kyc
from HardCode.latest_api.after_kyc_fails import after_kyc
from HardCode.latest_api.updation import udpation
from HardCode.latest_api.new_user_kyc_pass import new_user_kyc_pass
from HardCode.latest_api.old_user_pass import old_user


class OldUser(View):
    def post(self, request):
        return old_user(request, )


class NewUserKycPass(View):
    def post(self, request):
        return new_user_kyc_pass(request, )


class Updation(View):
    def post(self, request):
        return udpation(request, )


class CibilAnalysis(View):
    def post(self, request):
        return get_cibil_analysis(request, )


class CibilAnalysisStatus(View):
    def post(self, request):
        return get_cibil_analysis_status(request, )


class PreRejection(View):
    def post(self, request):
        return get_pre_rejection_status(request, )


class BeforeKyc(View):
    def post(self, request):
        return before_kyc(request, )


class AfterKyc(View):
    def post(self, request):
        return after_kyc(request, )
