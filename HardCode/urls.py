from django.urls import path,include
from django.views.decorators.csrf import csrf_exempt
from HardCode.views import CibilAnalysis

urlpatterns = [
    path('bl0/', csrf_exempt(CibilAnalysis.as_view())),
]
