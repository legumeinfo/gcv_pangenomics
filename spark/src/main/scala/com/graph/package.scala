package graph

// scala
import scala.collection.{Set => ISet}
import scala.collection.{Map => IMap}
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
  // FR graph types
  case class FRVertex(
    nodes: Array[Long],
    intervals: IMap[Long, Array[(Double, Double, Int)]],
    supporting: ISet[Long]
  ) extends Serializable
  type FREdge  = Edge[Unit]
  type FRGraph = Graph[FRVertex, Unit]

  // Approximate Frequent Subpath types
  type Interval  = ((Int, Int), (Int, Int))
  type Intervals = List[Interval]
}
