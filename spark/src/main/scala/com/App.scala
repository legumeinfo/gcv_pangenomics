// graph
import graph.{Algorithms, GraphLoader}
// Apache Spark
import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]): Unit = {
    // create the Spark context
    val conf = new SparkConf()
      .setAppName("my-app")
      .setMaster("local")
    val sc = new SparkContext(conf)
    // construct the graph
    val graphLoader = new GraphLoader(sc)
    val geneGraph = graphLoader.loadGeneGraph()
    // run the AFS algorithm
    val algorithms = new Algorithms(sc)
    algorithms.approximateFrequentSubpaths(geneGraph)
  }
}
