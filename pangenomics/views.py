from django.http import HttpResponse, HttpResponseBadRequest, Http404
from django.shortcuts import get_object_or_404
import json
from django.views.decorators.csrf import csrf_exempt
from services.models import Feature
from pangenome import findIntervals, combineIntervals, findQueryIntervals, intervalsToQueries

import time


#########
# views #
#########

# returns query intervals on the given chromosome for the given parameters
@csrf_exempt
def findQueries(request, chromosome, matched, intermediate, support):
    print 'chromosome:', chromosome
    print 'matched:', matched
    print 'intermediate:', intermediate
    print 'support:', support
    # parse the POST data (Angular puts it in the request body)
    #POST = json.loads(request.body)

    # make sure the request type is POST and that it contains query parameters
    #if request.method == 'POST' and 'chromosome' in POST and 'matched' in POST and 'intermediate' in POST and 'support' in POST:

    # parse the parameters
    #chromosome = POST['chromosome']
    chromosome_obj = get_object_or_404(Feature, name=chromosome)
    #matched = POST['matched']
    try:
        matched = int(matched)
        if matched <= 0:
            raise ValueError('"matched" must be a positive integer')
    except:
        return HttpResponseBadRequest
    #intermediate = POST['intermediate']
    try:
        intermediate = int(intermediate)
        if intermediate < 0:
            raise ValueError('"intermediate" must be zero or a positive integer')
    except:
        return HttpResponseBadRequest
    #support = POST['support']
    try:
        support = int(support)
        if support <= 0:
            raise ValueError('"support" must be a positive integer')
    except:
        return HttpResponseBadRequest

    # find exact match intervals
    print 'find intervals'
    t0 = time.time()
    path_intervals = findIntervals(chromosome_obj.pk)
    duration = time.time() - t0
    print duration, 'seconds'

    # combine exact matches to make inexact matches
    print 'combine intervals'
    t0 = time.time()
    intervals = combineIntervals(path_intervals, intermediate, matched)
    duration = time.time() - t0
    print duration, 'seconds'

    # find all inexact matches that overlap enough to be considered a query
    print 'query intervals'
    t0 = time.time()
    query_intervals = findQueryIntervals(intervals, support)
    duration = time.time() - t0
    print duration, 'seconds'

    # convert the intervals to queries (ordered lists of gene families)
    #queries = intervalsToQueries(chromosome, query_intervals)

    #return HttpResponse(
    #    json.dumps(queries),
    #    content_type='application/json; charset=utf8'
    #)

    #return HttpResponseBadRequest
    return HttpResponse(json.dumps(query_intervals))
