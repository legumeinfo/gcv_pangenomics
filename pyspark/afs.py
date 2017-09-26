from graphframe            import GraphFrame
from pyspark.sql           import Window
from pyspark.sql.functions import col, greatest, max as psf_max, min as psf_min,\
   row_number, struct


def approximateFrequentSubpaths(sc, paths, q, maxgap, minsize):
  """
  Compares the query chromosome to each other chromosome in the database,
  identifying subpaths that are approximately the same between the query and the
  other chromosomes.

  Args:
    sc (pyspark.SparkContext): The Apache Spark context to use when computing.
    g (pysparl.sql.DataFrame): The paths on which to perform the computation.
    q (Long): The query chromosome id.
    maxgap (Long): The maximum size of any gap between matching nodes in the
      query or another chromosome.
    minsize (Long): The minimum number of matched nodes a block must have to be
      returned.

  Returns:
    ?: The set of approximate frequent subpaths identified.
  """
  # get query chromosome
  query = paths\
    .select("family_id", col("gene_number").alias("query_number"))\
    .where(col("chromosome_id") == q)
  # get gene family content shared with other chromosomes
  shared = paths\
    .filter(col("chromosome_id") != q)\
    .join(query, paths.family_id == query.family_id)\
    .orderBy("chromosome_id", "gene_number")\
    .select("chromosome_id", "gene_number", "query_number")
  # build a graph
  v = shared\
    .select(struct("gene_number", "query_number").alias("id"), "chromosome_id")
  w = Window\
    .partitionBy("r1.chromosome_id", "r1.gene_number", "r1.query_number")\
    .orderBy("dist", "r2.gene_number", "r2.query_number")
  e = shared.alias("r1")\
    .join(shared.alias("r2"),
      (col("r1.chromosome_id") == col("r2.chromosome_id"))
      & (col("r1.gene_number") < col("r2.gene_number"))
      & (col("r1.query_number") < col("r2.query_number"))
      & (greatest(col("r2.gene_number") - col("r1.gene_number"),
        col("r2.query_number") - col("r1.query_number")) < maxgap))\
    .withColumn("dist", greatest(
      col("r2.gene_number") - col("r1.gene_number"),
      col("r2.query_number") - col("r1.query_number")))\
    .withColumn("rn", row_number().over(w))\
    .filter("rn = 1")\
    .drop("rn")\
    .select(struct("r1.gene_number", "r1.query_number").alias("src"),
      struct("r2.gene_number", "r2.query_number").alias("dst"))
  g = GraphFrame(v, e)
  # compute the connected components and aggregate into AFSs
  c = g.connectedComponents()
  afs = c\
    .groupBy("chromosome_id", "component")\
    .agg(psf_min("id").alias("min"), psf_max("id").alias("max"))
  afs.show()
  return afs
