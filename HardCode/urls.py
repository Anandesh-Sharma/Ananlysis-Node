from django.urls import path
from django.views.decorators.csrf import csrf_exempt
from HardCode.views import CibilAnalysis, CibilAnalysisStatus, PreRejection, FetchParameter, SetParameter, \
    GetCibilDecisions, FinalResults,FetchClassifiedMessages, UpdateAnalysis

urlpatterns = [
    path('bl0/', csrf_exempt(CibilAnalysis.as_view())),
    path('bl0/status/', csrf_exempt(CibilAnalysisStatus.as_view())),
    path('bl0/before_kyc/', csrf_exempt(PreRejection.as_view())),
    path('bl0/fetch_params/', csrf_exempt(FetchParameter.as_view())),
    path('bl0/set_params/', csrf_exempt(SetParameter.as_view())),
    path('bl0/fetch_classified_messages/', csrf_exempt(FetchClassifiedMessages.as_view())),
    path('bl0/before_cibil/', csrf_exempt(GetCibilDecisions.as_view())),
    path('bl0/before_loan/', csrf_exempt(FinalResults.as_view())),
    path('bl0/update_analysis/', csrf_exempt(UpdateAnalysis.as_view()))
]
