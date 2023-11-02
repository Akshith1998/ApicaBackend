from django.urls import path
from lru.views import *

urlpatterns=[
    path('getCacheValue',getCacheValue,name='getCacheValue'),
    path('setCacheValue',setCacheValue,name='setCacheValue')
]