from django.conf.urls import url, include

from orchestrator.restcontroller.monitor_apis import *

urlpatterns = [
    url(r'^template/', include('template.urls')),
    url(r'^monitor/(?P<tid>.+)/(?P<oper>.+)$', ClusterMonitor.monitor, name='start'),
]
