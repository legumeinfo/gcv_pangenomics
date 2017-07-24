// db
import db.{JSON, Neo4j}
// graph
import graph.Algorithms
// Apache Spark
import org.apache.spark.{SparkConf, SparkContext}

object App {
  def quit() {
    println("program <queryId> [<intermediate> <matched>]")
    System.exit(0)
  }

  def parseArgs(args: Array[String]): (Long, Int, Int) = {
    var id: Long = -1L  // invalid
    var intermediate: Int = 5
    var matched: Int = 10
    if (args.size == 0) {
      quit()
    }
    try {
      id = args(0).toLong
    } catch {
      case _: Throwable => {
        println("failed to parse <queryId>")
        quit()
      }
    }
    if (args.size > 1) {
      try {
        intermediate = args(1).toInt
      } catch {
        case _: Throwable => {
          println("failed to parse <intermediate>")
          println("using default: " + intermediate)
        }
      }
    }
    if (args.size > 2) {
      try {
        matched = args(2).toInt
      } catch {
        case _: Throwable => {
          println("failed to parse <matched>")
          println("using default: " + matched)
        }
      }
    }
    (id, intermediate, matched)
  }

  def main(args: Array[String]): Unit = {
    val (id, intermediate, matched) = parseArgs(args)
    // create the Spark context
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("gcv-pangenomics")
    val sc = new SparkContext(conf)
    // construct the graph
    val db = new Neo4j(sc)
    val geneGraph = db.loadGeneGraph()
    // run the AFS algorithms
    val algorithms = new Algorithms(sc)
    //val intervals = algorithms.approximateFrequentSubpaths(
    //  geneGraph, id, intermediate, matched
    //).collect()
    // dump the AFS data to a GCV macro-synteny JSON
    //val intervalData = db.loadIntervalData(id, intervals)
    //val json = new JSON()
    //val macroJSON = json.afsToMacroSynteny(id, intervals, intervalData)
    //json.dump("macro.json", macroJSON)
    // run the FR algorithm
    val regions = algorithms.frequentedRegions(geneGraph, 0.75, 10)
  }
}
