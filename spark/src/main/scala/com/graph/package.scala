package graph

// scala
import scala.collection.mutable.{Map, Set}
// Apache Spark
import org.apache.spark.graphx.{Graph, Edge}

package object types {
  // TODO: create generic graph class for Gene/DeBruijn to extend
  // gene graph types
  case class GeneVertex(paths: Map[Long, Set[Int]]) extends Serializable
  type GeneEdge  = Edge[Unit]
  type GeneGraph = Graph[GeneVertex, Unit]
  // de Bruijn graph types
  case class DeBruijnVertex(paths: Map[Long, Set[Int]]) extends Serializable
  type DeBruijnEdge  = Edge[Unit]
  type DeBruijnGraph = Graph[DeBruijnVertex, DeBruijnEdge]
}
