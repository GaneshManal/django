from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^all$', views.TemplateManager.list, name='list'),
    url(r'^add$', views.TemplateManager.add, name='add'),
    url(r'^delete/(?P<id>.+)/$', views.TemplateManager.delete, name='delete'),
    url(r'^show/(?P<id>.+)/$', views.TemplateManager.show, name='show'),
    url(r'^status/(?P<tid>.+)/$', views.TemplateManager.status, name='status'),
]
