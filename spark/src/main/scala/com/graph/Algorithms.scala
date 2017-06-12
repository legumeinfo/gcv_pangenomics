package graph

// graph
import graph.types.GeneGraph
// Apache Spark
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

class Algorithms(sc: SparkContext) {
  def approximateFrequentSubpaths(g: GeneGraph) = {
    // should be able to compute on a gene or de Bruijn graph
  }
  def frequentedRegions(g: GeneGraph, chrId: Long) = {
    // get all the nodes of the target chromosome
    //g.vertices.filter
    // should be able to compute on a gene or de Bruijn graph
  }
}
