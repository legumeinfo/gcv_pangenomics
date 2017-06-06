package graph

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

class GraphLoader(sc: SparkContext) {
  def loadGeneGraph() = {
    // graph will be loaded from Neo4j, for now
    // caching?
  }
  def loadDeBruijnGraph() = {
    // graph will be loaded from Neo4j, for now
    // caching?
  }
}
