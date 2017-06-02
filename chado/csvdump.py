# Python
import sys
# SQLAlchemy
from collections    import defaultdict
from sqlalchemy     import create_engine
from sqlalchemy.orm import sessionmaker
# local
from orm import Feature, Featureloc, GeneFamilyAssignment, GeneOrder, Organism


def connect(user, password, db, host='localhost', port=5432):
  '''Returns a session object'''
  # connect view the PostgreSQL URL
  url = 'postgresql://{}:{}@{}:{}/{}'
  url = url.format(user, password, host, port, db)
  # create a connection object
  con = create_engine(url, client_encoding='utf8')
  # create a session for the ORM
  Session = sessionmaker(bind=con)
  return Session()


def dump(session):
  # get all gene_orders
  null_fam = -1
  gene_orders = session.query(GeneOrder).all()
  # create the initial gene objects
  genes = dict((o.gene_id, {
    'id':         o.gene_id,
    'number':     o.number,
    'chromosome': o.chromosome_id,
    'family':     null_fam
  }) for o in gene_orders)
  gene_ids = list(genes.keys())

  # get each gene's name via the feature table
  gene_features = session.query(Feature.feature_id, Feature.name)\
    .filter(Feature.feature_id.in_(gene_ids)).all()
  for f in gene_features:
    genes[f.feature_id]['name'] = f.name

  # get each gene's fmin, fmax, and strand via the featureloc table
  featurelocs = session.query(Featureloc)\
    .filter(Featureloc.feature_id.in_(gene_ids)).all()
  chromosome_ids = set()
  for l in featurelocs:
    g = genes[l.feature_id]
    g['fmin']   = l.fmin
    g['fmax']   = l.fmax
    g['strand'] = l.strand
    chromosome_ids.add(l.srcfeature_id)

  # get each gene's family via the gene_family_assignment table
  family_assignments = session.query(GeneFamilyAssignment)\
    .filter(GeneFamilyAssignment.gene_id.in_(gene_ids)).all()
  family_ids = set([null_fam])
  for f in family_assignments:
    genes[f.gene_id]['family'] = f.family_label
    family_ids.add(f.family_label)
  families = [{'id': f, 'name': f} for f in family_ids]
  dumpGenes(list(genes.values()))
  dumpGeneFamilies(families)

  # get all the chromosomes
  chromosome_features = session.query(
    Feature.feature_id,
    Feature.name,
    Feature.seqlen,
    Feature.organism_id
  ).filter(Feature.feature_id.in_(chromosome_ids)).all()
  chromosomes = []
  organism_ids = set()
  for f in chromosome_features:
    chromosomes.append({
      'id':       f.feature_id,
      'name':     f.name,
      'length':   f.seqlen,
      'organism': f.organism_id
    })
    organism_ids.add(f.organism_id)
  dumpChromosomes(chromosomes)

  # get all the organisms
  raw_organisms = session.query(Organism)\
    .filter(Organism.organism_id.in_(organism_ids)).all()
  organisms = [{
    'id':      o.organism_id,
    'genus':   o.genus,
    'species': o.species
  } for o in raw_organisms]
  dumpOrganisms(organisms)


def dumpChromosomes(chromosomes):
  headers = ['id', 'name', 'length', 'organism']
  writeCSV('chromosomes', headers, chromosomes)


def dumpGenes(genes):
  headers = ['id', 'name', 'fmin', 'fmax', 'strand', 'number', 'family',
    'chromosome']
  writeCSV('genes', headers, genes)


def dumpGeneFamilies(families):
  headers = ['id', 'name']
  writeCSV('families', headers, families)


def dumpOrganisms(organisms):
  headers = ['id', 'genus', 'species']
  writeCSV('organisms', headers, organisms)


def writeCSV(name, headers, data):
  with open(name + '.csv', 'w') as f:
    f.write(','.join(headers) + '\n')
    for d in data:
      line = [str(d[c]) for c in headers]
      f.write(','.join(line) + '\n')


if __name__ == '__main__':
  argc = len(sys.argv)
  if argc < 3:
    sys.exit('{0} <user> <db> [host] [port]'.format(sys.argv[0]))
  user, db = sys.argv[1:3]
  host = 'localhost' if argc < 4 else sys.argv[3]
  port = 5432
  if argc >= 5:
    try:
      port = int(sys.argv[4])
    except:
      pass
  session = connect(user, '', db, host=host, port=port)
  dump(session)
