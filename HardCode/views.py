from django.shortcuts import render
from django.views.generic.base import View

from .middleware.cibil_analysis_middleware import get_something


class CibilAnalysis(View):
    def get(self,requst):
        return "haha"