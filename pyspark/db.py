from collections           import defaultdict
from graphframe            import GraphFrame
from neo4j.v1              import GraphDatabase
from pyspark.sql           import SQLContext, Window
from pyspark.sql.functions import col, lag


class DB(object):
  """
  An abstract class that defines the interface that each database class must
  implement.

  Todo:
    * Use the Abstract Base Classes (abc) module?
  """

  def __init__(self, sc):
    """Constructor.

    Args:
      sc (pyspark.SparkContext): The SparkContext used to get or create the
        SQLContext used to create DataFrames for GraphFrames.
    """
    self.sqlc = SQLContext.getOrCreate(sc)

  def loadDeBruijnGraph(self):
    """
    Loads a De Bruijn graph.
    
    Returns:
      graphframe.GraphFrame: A De Bruijn graph.
    """
    raise NotImplementedError("loadDeBruijnGraph was not implemented")

  def loadGeneGraph(self):
    """
    Loads a gene graph.
    
    Returns:
      graphframe.GraphFrame: A gene graph.
    """
    raise NotImplementedError("loadGeneGraph was not implemented")


class Neo4j(DB):
  """
  A driver for the Neo4j database that implements the DB interface.
  
  Todo:
    * Can loading transaction results be speed-up/parallelized?
  """
  
  def __init__(self, sc,
               uri='bolt://localhost:7687', user='neo4j', password='neo4j'):
    super().__init__(sc)
    self.driver = GraphDatabase.driver(uri, auth=(user, password))

  def geneAFStoBlocks(self, afs):
    return blocks

  def loadDeBruijnGraph(self):
    pass

  def loadGeneGraph(self):
    data  = []
    query = ("MATCH "
             "(family:GeneFamily)<-[:GeneToGeneFamily]-(gene:Gene)"
             "-[:GeneToChromosome]->(chromosome:Chromosome) "
             "RETURN family, gene, chromosome "
             "ORDER BY chromosome.id, gene.number")
    with self.driver.session() as session:
      with session.begin_transaction() as tx:
        for record in tx.run(query):
          family_id     = record["family"].id
          gene_number   = record["gene"]["number"]
          chromosome_id = record["chromosome"].id
          data.append((chromosome_id, family_id, gene_number))
    columns  = ["chromosome_id", "family_id", "gene_number"]
    paths    = self.sqlc.createDataFrame(data, columns)
    vertices = paths.select(col("family_id").alias("id")).distinct()
    window   = Window.partitionBy("chromosome_id").orderBy("gene_number")
    edges    = paths.withColumn("src", lag("family_id").over(window))\
                    .select("src", col("family_id").alias("dst"))\
                    .filter(col("src").isNotNull())\
                    .distinct()
    g = GraphFrame(vertices, edges)
    return paths, g
