from django.shortcuts import render
from django.http import HttpResponse
# Create your views here.

def home(request):
    return render(request, 'SqlProfiles/home.html', {'message':'dictionary value'})

def index(request):
    return render(request, 'SqlProfiles/index.html')
