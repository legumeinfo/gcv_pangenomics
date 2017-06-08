package graph

// scala
import scala.collection.mutable.{Map, Set}
// graph
import graph.types.{GeneVertex, GeneEdge, GeneGraph}
// make the class automatically release resources
import java.io.Closeable
// Apache Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
// Neo4j
import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase, Record, Session,
                            StatementResult}
import org.neo4j.driver.v1.types.Node

class GraphLoader(
  sc: SparkContext,
  uri: String = "bolt://localhost:7687",
  user: String = "neo4j",
  password: String = "neo4j"
) extends Closeable {

  // constructor
  private val _driver: Driver = GraphDatabase.driver(
    uri,
    AuthTokens.basic(user, password))
  private val _session: Session = _driver.session()

  // destructor
  override def close() = {
    _session.close()
    _driver.close()
  }

  // public
  def loadGeneGraph(): GeneGraph = {
    // create data structures
    val vertexData = Map[Long, Map[Long, Set[Int]]]().withDefaultValue(
      Map[Long, Set[Int]]().withDefaultValue(Set())
    )
    val edgeData: Set[GeneEdge] = Set()
    // fetch the data
    val data: StatementResult = _session.run("MATCH (f:GeneFamily)<-[:GeneToGeneFamily]-(g:Gene)-[:GeneToChromosome]->(c:Chromosome) RETURN f, g, c ORDER BY c.id, g.number")
    var prevNumber: Int = -1
    var prevFamily: Long = -1L
    // populate data structures
    while (data.hasNext()) {
      val datum: Record      = data.next()
      val familyId: Long     = datum.get("f").asNode().id()
      val geneNumber: Int    = datum.get("g").asNode().get("number").asInt()
      val chromosomeId: Long = datum.get("c").asNode().id()
      vertexData(familyId)(chromosomeId).add(geneNumber)
      if (prevNumber == geneNumber-1) {
        edgeData.add(Edge(prevFamily, familyId))
      }
      prevNumber = geneNumber
      prevFamily = familyId
    }
    // create RDDs
    val vertexArray: Array[(Long, GeneVertex)] =
      vertexData.map{case (f, paths) => {
        (f, GeneVertex(paths = paths.map{case (c, numbers) => {
          (c, numbers.toSet)
        }}.toMap))
      }}.toArray
    val vertices: RDD[(VertexId, GeneVertex)] = sc.parallelize(vertexArray)
    val edges: RDD[GeneEdge] = sc.parallelize(edgeData.toSeq)
    // create graph
    return Graph(vertices, edges)
  }

  def loadDeBruijnGraph() = {
    // graph will be loaded from Neo4j, for now
    // caching?
  }
}
