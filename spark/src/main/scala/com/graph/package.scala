package graph

// scala
import scala.collection.immutable.{Map, Set}
// Apache Spark
import org.apache.spark.graphx.{Graph, Edge}

package object types {
  // gene graph types
  case class GeneVertex(paths: Map[Long, Set[Int]]) extends Serializable
  type GeneEdge  = Edge[Unit]
  type GeneGraph = Graph[GeneVertex, Unit]
  // de Bruijn graph types
  case class DeBruijnVertex(paths: Map[Long, Set[Int]]) extends Serializable
  type DeBruijnEdge  = Edge[Unit]
  type DeBruijnGraph = Graph[DeBruijnVertex, DeBruijnEdge]
}
