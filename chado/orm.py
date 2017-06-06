from sqlalchemy import Column, create_engine, ForeignKey, Integer, SmallInteger, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Feature(Base):
  __tablename__ = 'feature'
  feature_id = Column(Integer, primary_key=True)
  organism_id = Column(Integer, ForeignKey('organism.organism_id'))
  name = Column(String)
  uniquename = Column(String)
  seqlen = Column(Integer)
 
 
class Featureloc(Base):
  __tablename__ = 'featureloc'
  featureloc_id = Column(Integer, primary_key=True)
  feature_id = Column(Integer, ForeignKey('feature'))
  srcfeature_id = Column(Integer, ForeignKey('feature'))
  fmin = Column(Integer)
  fmax = Column(Integer)
  strand = Column(SmallInteger)


class GeneFamilyAssignment(Base):
  __tablename__ = 'gene_family_assignment'
  gene_family_assignment_id = Column(Integer, primary_key=True)
  gene_id = Column(Integer, ForeignKey('feature'))
  family_label = Column(String)


class GeneOrder(Base):
  __tablename__ = 'gene_order'
  gene_order_id = Column(Integer, primary_key=True)
  chromosome_id = Column(Integer, ForeignKey('feature'))
  gene_id = Column(Integer, ForeignKey('feature'))
  number = Column(Integer)


class Organism(Base):
  __tablename__ = 'organism'
  organism_id = Column(Integer, primary_key=True)
  genus = Column(String)
  species = Column(String)
