import org.apache.spark.{SparkConf, SparkContext}
import graph.{Algorithms, GraphLoader}

object App {
  def main(args: Array[String]): Unit = {
    // create the Spark context
    val conf = new SparkConf()
      .setAppName("my-app")
      .setMaster("local")
    val sc = new SparkContext(conf)
    // construct the graph
    val graphLoader = new GraphLoader(sc)
    graphLoader.loadGeneGraph()
  }
}
