from django.urls import path
from django.views.decorators.csrf import csrf_exempt
from HardCode.views import CibilAnalysis, CibilAnalysisStatus, PreRejection, FetchParameter, SetParameter, \
    GetCibilDecisions, FinalResults

urlpatterns = [
    path('bl0/', csrf_exempt(CibilAnalysis.as_view())),
    path('bl0/status/', csrf_exempt(CibilAnalysisStatus.as_view())),
    path('bl0/pre_rejection_status/', csrf_exempt(PreRejection.as_view())),
    path('bl0/fetch_params/', csrf_exempt(FetchParameter.as_view())),
    path('bl0/set_params/', csrf_exempt(SetParameter.as_view())),
    path('bl0/get_cibil_decision/', csrf_exempt(GetCibilDecisions.as_view())),
    path('bl0/final_results/', csrf_exempt(FinalResults.as_view())),
]
