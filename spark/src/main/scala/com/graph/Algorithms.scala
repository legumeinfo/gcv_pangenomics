package graph

// scala
import scala.collection.mutable.SortedMap  // source copied from 2.12.x...
// graph
import graph.types.{GeneGraph, GeneVertex, Interval, Intervals}
// Apache Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}

class Algorithms(sc: SparkContext) {
  def approximateFrequentSubpaths(
    g: GeneGraph,
    chromosomeId: Long,
    intermediate: Int,
    matched: Int
  ): RDD[(Long, Intervals, Intervals)] = {
    // only consider gene families on the query chromosome
    val chromosome = g.vertices.filter{
      case (id: VertexId, v: GeneVertex) => {
        v.paths.contains(chromosomeId) && id != 42740  // hard-coded null-fam...
      }
    }
    // get all the chromosomes that share families with the query chromosome
    val relatives = chromosome.map{case (id: VertexId, v: GeneVertex) => {
      v.paths.keys.toSet
    }}.reduce((s1, s2) => s1.union(s2)) - chromosomeId
    // create a group of sorted gene number pairs for each relative chromosome
    val chromosomes = chromosome.flatMap{case (id: VertexId, v: GeneVertex) => {
      relatives.collect{case c if v.paths.contains(c) => {
        for (refN <- v.paths(chromosomeId)) yield {
          for (relN <- v.paths(c)) yield (c, ((refN, relN), (0, (-1, -1))))
        }
      }.flatten }.flatten
    }}.groupByKey
    // find approximately matching intervals
    val int = intermediate + 1
    val intervals = chromosomes.map{case (c, pairPaths) => {
      // construct incidence "matrix"
      val forwardM = SortedMap(pairPaths.to: _*)
      val reverseM = forwardM.clone()
      val (pairs, _) = pairPaths.unzip
      val relative = pairs.toList.sortBy(relP => (relP._2, relP._1)).foreach(
      relP => {
        forwardM.from(relP).foreach{case (refP, (score, pair)) => {
          if (relP._1 < refP._1 && relP._2 < refP._2) {
            val refDist = refP._1 - relP._1
            val relDist = refP._2 - relP._2
            if (refDist <= int && relDist <= int) {
              val (ptrScore, _) = forwardM(relP)
              val newScore = ptrScore + math.max(int - refDist, int - relDist) + 1
              if (score < newScore) {
                forwardM(refP) = (newScore, relP)
              }
            }
          }
        }}
        reverseM.to(relP).foreach{case (refP, (score, pair)) => {
          if (relP._1 > refP._1 && relP._2 < refP._2) {
            val refDist = relP._1 - refP._1
            val relDist = refP._2 - relP._2
            if (refDist <= int && relDist <= int) {
              val (ptrScore, _) = reverseM(relP)
              val newScore = ptrScore + math.max(int - refDist, int - relDist) + 1
              if (score < newScore) {
                reverseM(refP) = (newScore, relP)
              }
            }
          }
        }}
      })
      // traceback algorithm
      def traceback(m: SortedMap[(Int, Int), (Int, (Int, Int))]): Intervals = {
        val (ends, _) = m.toList.filter{case (refP, (score, relP)) => {
          score > 0
        }}.sortBy{case (refP, path) => {
          (-path._1, -path._2._1, -path._2._2)
        }}.unzip
        val intervals = {for (refP <- ends if m.contains(refP)) yield {
          val (refN, maxRel) = refP
          var current = refP
          var prev = refP
          while (m.contains(current)) {
            prev = current
            current = m(current)._2
            m.remove(prev)
          }
          val ref = if (prev._1 < refN) (prev._1, refN) else (refN, prev._1)
          (ref, (prev._2, maxRel))
        }}.filter(interval => {
          interval._1._2 - interval._1._1 + 1 >= matched &&
          interval._2._2 - interval._2._1 + 1 >= matched
        })
        return intervals
      }
      val forward = traceback(forwardM)
      val reverse = traceback(reverseM)
      (c, forward, reverse)
    }}.filter{case (c, forward, reverse) => {
      forward.nonEmpty || reverse.nonEmpty
    }}
    return intervals
  }

  //private def _coarsen(
  //  g: Graph[V, E],
  //  pred: Triplet[V, E] => Boolean,
  //  reduce: (V,V) => V
  //): Graph[V,E] = {
  //  // Restrict graph to contractable edges
  //  val subG = g.subgraph(v => True, pred)
  //  // Compute connected component id for all V
  //  val cc: Col[Id, Id] = ConnectedComp(subG).vertices
  //  // Merge all vertices in same component
  //  val superVerts = g.vertices.leftJoin(cc).map {
  //  (vId, (vProp, cc)) => (cc, vProp))
  //  }.reduceByKey(reduce)
  //  // Link remaining edges between components
  //  val invG = g.subgraph(v=>True, !pred)
  //  val remainingEdges =
  //  invG.leftJoin(cc).triplets.map {
  //  e => ((e.src.cc, e.dst.cc), e.attr)
  //  }
  //  // Return the final graph
  //  Graph(superVerts, remainingEdges)
  //}

  private def _coarsen(g: GeneGraph) {
    // 1) compute support for each edge
    //g.triplets.map(e => {
    g = g.mapTriplets(e => {
      // a) union the nodes' sub-node sets
      // b) for each path p:
      //   i) union its node sets
      //   ii) compute alpha (penetrance: fraction of nodes it traverses)
      //   iii) 
    })
    // 2) compute maximal weighted matching
    // 3) construct new graph with vertices of independent edges set combined
    return g
  }

  case class CoarsenVertex(
    nodes: Array[Long],
    intervals: Map[Long, Array[(Float, Float, Int)]]
  ) extends Serializable

  // supporting path
  def combineOverlappingIntervals(
    intervals: Array[(Float, Float, Int)]
  ): Array[(Float, Float, Int)] = {
    // 1) create an array of (begin/end, increment, penetrance) tuples
    val (begins, ends) = intervals.map{case (begin, end, count) => {
      ((begin, 1, count), (end, -1, 0))
    }}.unzip
    val intervalPoints = (begins ++ end).sortBy{case (p, i, _) => (p, -i)}
    // 2) combine overlapping intervals into a single interval
    val combinedIntervals = Array[(Float, Float, Int)]()
    var counter = 0
    var begin: Float = 0
    var penetrance = 0
    for ((p, i, c) <- intervalPoints) {
      if (counter == 0) {
        begin = p
        penetrance = 0
      }
      counter += i
      penetrance += c
      if (counter == 0) {
        combinedIntervals :+ (begin, p, pentrance)
      }
    }
    combinedIntervals
  }

  def frequentedRegions(g: GeneGraph, alpha: Float, kappa: Integer) = {
    // create the initial FR graph
    halfKappa = kappa.toFloat/2
    g = g.mapVertices((id, v) =>  {
      val intervals = v.paths.map{case (p, nums) => {
        val numArray = nums.toArray.map(n => {
          (n.asFloat - halfKappa, n.asFloat + halfKappa, 1)
        })
        (p, combineIntervals(intervals).map{case (b, e, _) => (b, e, 1)})
      }}
      (id, CoarsenVertex(Array(id), CoarsenVertex(intervals)))
    })
    // perform hierarchical clustering via coarsening
    while (g.numVertices > 1) {
      g = _coarsen(g)
    }
  }
}
