package graph

// make the class automatically release resources
import java.io.Closeable
// Apache Spark
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
// Neo4j
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.Values.parameters

class GraphLoader(sc: SparkContext) extends Closeable {

  // constructor
  private val _driver: Driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    AuthTokens.basic("neo4j", "neo4j"))
  private val _session: Session = _driver.session()

  // destructor
  override def close() = {
    _session.close();
    _driver.close();
  }

  // public
  def loadGeneGraph() = {
    // graph will be loaded from Neo4j, for now
    // caching?
  }

  def loadDeBruijnGraph() = {
    // graph will be loaded from Neo4j, for now
    // caching?
  }
}
