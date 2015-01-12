from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

from .nhpcdb.dbopts import put, query

urlpatterns = patterns('',
    url(r'^put/', put),
    url(r'^query/', query),
)

