from django.dispatch import receiver
from signals import app_ready
from apps import PangenomicsConfig
from services.models import Feature, GeneOrder, GeneFamilyAssignment
import networkx as nx
from collections import defaultdict
from Queue import PriorityQueue


def writeToFile(data, filename, indent=2):
    import json
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=indent)


GENE_FAMILY_PAN = None


# listen for ready signal from AppConfig
@receiver(app_ready, sender=PangenomicsConfig, dispatch_uid='pangenome')
def ready_handler(sender, **kwargs):
    print 'Constructing gene family pan-genome graph. This may take a few minutes.'
    global GENE_FAMILY_PAN
    gene_paths, family_paths = getPaths()
    GENE_FAMILY_PAN = pathsToGraph(gene_paths, family_paths)


#################################
# pan-genome graph construction #
#################################


def pathsToGraph(gene_paths, family_paths):
    # make the graph
    g = nx.Graph(gene_paths=gene_paths, family_paths=family_paths)
    for chromosome, path in g.graph['family_paths'].iteritems():
        g.add_nodes_from(path)

    # initialize each node
    for n in g.nodes_iter():
        g.node[n]['paths'] = defaultdict(list)

    # thread each path through the graph
    for chromosome, path in g.graph['family_paths'].iteritems():
        l = len(path) - 1
        for i in range(l):
            n1 = path[i]
            g.node[n1]['paths'][chromosome].append(i)
            n2 = path[i + 1]
            g.add_edge(n1, n2)  # edges are not actually used by findQueries
        g.node[path[l]]['paths'][chromosome].append(l)

    return g


def getPaths():
    # get all the chromosome gene orders
    gene_orders = GeneOrder.objects.all()
    
    # group gene orders by chromosome
    grouped_orders = defaultdict(list)
    for o in gene_orders:
        grouped_orders[o.chromosome_id].append(o)
    
    # sort the grouped gene orders by number
    for chromosome, orders in grouped_orders.iteritems():
        orders.sort(lambda a, b: a.number - b.number)
    
    # get the families for all the ordered genes
    gene_ids = map(lambda o: o.gene_id, gene_orders)
    family_assigns = GeneFamilyAssignment.objects\
        .only('gene_id', 'family_label')\
        .filter(gene_id__in=gene_ids)
    gene_families = dict((o.gene_id, o.family_label) for o in family_assigns)
    
    # make a path of ordered gene families for each chromosome
    chromosome_gene_paths = {}
    chromosome_family_paths = {}
    for chromosome, orders in grouped_orders.iteritems():
        gene_path = []
        family_path = []
        for o in orders:
            gene_path.append(o.gene_id)
            family = gene_families.get(o.gene_id, o.gene_id)
            family_path.append(family)
        chromosome_gene_paths[chromosome] = gene_path
        chromosome_family_paths[chromosome] = family_path

    return chromosome_gene_paths, chromosome_family_paths


##############################
# pan-genome graph searching #
##############################


def indexOf(l, e):
  try:
    return l.index(e)
  except ValueError:
    return -1


def findIntervals(chromosome):
  intervals = defaultdict(list)
  prev = defaultdict(list)
  # traverse each node on the target chromosome's path
  for i, n in enumerate(GENE_FAMILY_PAN.graph['family_paths'][chromosome]):
    current = defaultdict(list)
    # for each path that traverses the node
    for p in set(prev.keys()) | set(GENE_FAMILY_PAN.node[n]['paths'].keys()):
      if p == chromosome:
        continue
      # check if it is the next node of a known interval
      nums = GENE_FAMILY_PAN.node[n]['paths'][p][:]
      for interval in prev[p]:
        num = interval[1][1]
        j = max(indexOf(nums, num + 1), indexOf(nums, num - 1))
        if j != -1:
          interval[1] = (i, nums.pop(j))
          current[p].append(interval)
        else:
          intervals[p].append(interval)
      # all remaining nodes are new intervals
      for num in nums:
        end = (i, num)
        current[p].append([end, end])
    prev = current
  for p, p_intervals in prev.iteritems():
    intervals[p] += p_intervals
  return intervals


def compareIntervals(a, b):
  return min(a[0][1], a[1][1]) - min(b[0][1], b[1][1])


def combineIntervals(path_intervals, intermediate, matched):
  combined_intervals = []
  # for each path
  for p, p_intervals in path_intervals.iteritems():
    # sort the intervals by their ordering on p's path
    sorted_intervals = sorted(p_intervals, cmp=compareIntervals)
    # combine intervals that are no more than intermediate apart
    l = len(sorted_intervals)
    i = 0
    while i < l:
      interval = sorted_intervals[i]
      ref_interval = [interval[0][0], interval[1][0]]
      p_interval = None
      if interval[0][1] < interval[1][1]:
        p_interval = [interval[0][1], interval[1][1]]
      else:
        p_interval = [interval[1][1], interval[0][1]]
      count = ref_interval[1] - ref_interval[0] + 1
      j = i + 1
      while j < l:
        neighbor = sorted_intervals[j]
        ref_dist = min(abs(neighbor[0][0] - ref_interval[0]),
                       abs(neighbor[0][0] - ref_interval[1]),
                       abs(neighbor[1][0] - ref_interval[0]),
                       abs(neighbor[1][0] - ref_interval[1])) - 1
        p_dist = min(neighbor[0][1] - p_interval[0],
                     neighbor[0][1] - p_interval[1],
                     neighbor[1][1] - p_interval[0],
                     neighbor[1][1] - p_interval[1]) - 1
        if ref_dist <= intermediate and p_dist <= intermediate:
          ends = ref_interval + [neighbor[0][0], neighbor[1][0]]
          ref_interval = [min(ends), max(ends)]
          p_interval[1] = max(neighbor[0][1], neighbor[1][1])
          count += neighbor[1][0] - neighbor[0][0] + 1
        else:
          break
        j += 1
      if count >= matched:
        combined_intervals.append(ref_interval)
      i = j
  return combined_intervals


def findQueryIntervals(intervals, support):
  sorted_intervals = sorted(intervals, cmp=lambda a, b: a[0] - b[0])
  q = PriorityQueue()
  queries = []
  start = None
  for s, t in sorted_intervals:
    end = None
    while not q.empty() and q.queue[0] < s:
      car = q.get()
      if q.qsize() == support - 1:
        end = car
    q.put(t)
    if q.qsize() == support:
      start = s
    elif q.qsize() < support and start is not None:
      queries.append((start, end))
      start = None
  end = None
  while q.qsize() >= support:
    end = q.get()
  if end is not None:
    queries.append((start, end))
  return queries


def intervalsToQueries(chromosome, intervals):
  path = GENE_FAMILY_PAN.graph['gene_paths'][chromosome]
  gene_ids = []
  gene_intervals = []
  for s, t in intervals:
    int_ids = path[s:t+1]
    gene_ids += int_ids
    gene_intervals.append(int_ids)
  genes = Feature.objects.only('name').filter(pk__in=gene_ids)
  gene_names = dict((g.pk, g.name) for g in genes)
  queries = [map(lambda g: gene_names[g], query) for query in gene_intervals]
  return queries
