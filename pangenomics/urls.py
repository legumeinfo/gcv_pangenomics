from django.conf.urls import patterns, url


urlpatterns = patterns('pangenomics.views',
    url(r'^find-queries/(?P<chromosome>\d+)/(?P<matched>\d+)/(?P<intermediate>\d+)/(?P<support>\d+)/$', 'findQueries'),
)
