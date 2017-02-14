from django.conf.urls import patterns, url


urlpatterns = patterns('pangenomics.views',
    url(r'^find-queries/(?P<chromosome>[\w.\-]+)/(?P<matched>\d+)/(?P<intermediate>\d+)/(?P<support>\d+)/$', 'findQueries'),
)
