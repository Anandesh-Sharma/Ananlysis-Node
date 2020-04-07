from django.urls import path
from django.views.decorators.csrf import csrf_exempt
from HardCode.views import CibilAnalysis, CibilAnalysisStatus

urlpatterns = [
    path('bl0/', csrf_exempt(CibilAnalysis.as_view())),
    path('bl0/status/', csrf_exempt(CibilAnalysisStatus.as_view())),
]
