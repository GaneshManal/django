from django.conf.urls import url, include
# from django.contrib import admin
from watchdog.api_controller import *

urlpatterns = [
    url(r'^template/', include('template.urls')),
    url(r'^monitor/(?P<tid>.+)/(?P<oper>.+)$', ClusterMonitor.monitor, name='monitor'),
    url(r'^job/(?P<tid>.+)/(?P<oper>.+)$', JobMonitor.perform, name='perform'),
]
