from django.urls import path
from django.views.decorators.csrf import csrf_exempt
from HardCode.views import CibilAnalysis, CibilAnalysisStatus, PreRejection, BeforeKyc, AfterKyc, Updation, \
    NewUserKycPass, OldUser

urlpatterns = [
    path('bl0/', csrf_exempt(CibilAnalysis.as_view())),
    path('bl0/status/', csrf_exempt(CibilAnalysisStatus.as_view())),
    path('bl0/pre_rejection_status/', csrf_exempt(PreRejection.as_view())),
    path('before_kyc/', csrf_exempt(BeforeKyc.as_view())),
    path('after_kyc_fails/', csrf_exempt(AfterKyc.as_view())),
    path('updation/', csrf_exempt(Updation.as_view())),
    path('new_user_kyc_pass/', csrf_exempt(NewUserKycPass.as_view())),
    path('old_user/', csrf_exempt(OldUser.as_view())),
]
