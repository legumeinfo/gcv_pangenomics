package db

// scala
import scala.collection.JavaConverters._
import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{Map, Set}

// db
import db.types.{Chromosome, Gene, Organism}
// graph
import graph.types.{GeneVertex, GeneEdge, GeneGraph, Interval, Intervals}
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

class Neo4j(
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
  def loadDeBruijnGraph() = {
    // graph will be loaded from Neo4j, for now
    // caching?
  }

  def loadGeneGraph(): GeneGraph = {
    // create data structures
    val vertexData = Map[Long, Map[Long, Set[Int]]]()
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
      vertexData.getOrElseUpdate(familyId, Map[Long, Set[Int]]())
        .getOrElseUpdate(chromosomeId, Set()) += geneNumber
      if (prevNumber == geneNumber-1) {
        edgeData += Edge(prevFamily, familyId)
      }
      prevNumber = geneNumber
      prevFamily = familyId
    }
    // create RDDs
    val vertexArray: Array[(Long, GeneVertex)] =
      vertexData.map{case (f, paths) => (f, GeneVertex(paths))}.toArray
    val vertices: RDD[(VertexId, GeneVertex)] = sc.parallelize(vertexArray)
    val edges: RDD[GeneEdge] = sc.parallelize(edgeData.toSeq)
    // create graph
    return Graph(vertices, edges)
  }

  def loadIntervalData(
    chromosomeId: Long,
    intervals: Array[(Long, Intervals, Intervals)]
  ): (IMap[Long, IMap[Int, Gene]], IMap[Long, Organism]) = {
    // get the query chromosome
    val chromosomeNode: Node = _session.run(
      "MATCH (c:Chromosome) WHERE id(c) = " + chromosomeId + " RETURN c"
    ).single().get("c").asNode()
    val chromosome = Chromosome(
      chromosomeNode.id(),
      chromosomeNode.get("name").asString(),
      chromosomeNode.get("length").asInt()
    )
    // make a list of gene numbers for each chromosome, including the query
    val (relatives, chromosomeIntervals, relativeNumbers) = intervals.map{
    case (r, forward, reverse) => {
      val (cIntervals, rIntervals) = (forward ++ reverse).unzip
      val rNumbers = rIntervals.flatMap{case (min, max) => List(min, max)}.toSet
      (r, cIntervals, rNumbers)
    }}.unzip3
    val chromosomeNumbers = relatives.zip(relativeNumbers).toMap +
      (chromosomeId -> chromosomeIntervals.flatten.flatMap{case (min, max) => {
        List(min, max)
      }}.toSet)
    // create a (chromosome -> (gene number -> gene)) map
    var chromosomeGenesByNumber = chromosomeNumbers.map{
    case (chromosome, numbers) => {
      val geneNumbers = numbers.mkString(",")
      val data: StatementResult = _session.run("MATCH (c:Chromosome)<-[:GeneToChromosome]-(g:Gene) WHERE id(c) = " + chromosome + " AND g.number in [" + geneNumbers + "] RETURN g")
      val genes = data.asScala.map(r => {
        val g = r.get("g").asNode()
        val n = g.get("number").asInt()
        (n, Gene(
          g.id(),
          n,
          g.get("strand").asInt(),
          g.get("name").asString(),
          g.get("fmin").asInt(),
          g.get("fmax").asInt()
        ))
      }).toMap
      (chromosome, genes)
    }}
    // create a (chromosome -> organism) map
    val chromosomeIds = relatives.mkString(",")
    val data: StatementResult = _session.run("MATCH (c:Chromosome)-[:ChromosomeToOrganism]->(o:Organism) WHERE id(c) in [" + chromosomeIds + "] RETURN c, o")
    val chromosomeOrganisms = data.asScala.map(r => {
      val c = r.get("c").asNode()
      val o = r.get("o").asNode()
      (c.id(), Organism(
        o.id(),
        o.get("genus").toString(),
        o.get("species").toString()
      ))
    }).toMap
    return (chromosomeGenesByNumber, chromosomeOrganisms)
  }
}
