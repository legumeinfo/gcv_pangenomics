from django.conf.urls import patterns, url


urlpatterns = patterns('pangenomics.views',
    url(r'^find-query-intervals/(?P<chromosome>[\w.\-]+)/(?P<matched>\d+)/(?P<intermediate>\d+)/(?P<support>\d+)/$', 'afsQueryIntervals'),
    url(r'^find-query-genes/(?P<chromosome>[\w.\-]+)/(?P<matched>\d+)/(?P<intermediate>\d+)/(?P<support>\d+)/$', 'afsQueryGenes'),
)
