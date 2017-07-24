package graph

// scala
import scala.collection.mutable.SortedMap  // source copied from 2.12.x...
// graph
import graph.types.{FRGraph, FREdge,  FRVertex, GeneGraph, GeneVertex,
                    Interval, Intervals}
// Apache Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}

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

  def coarsen(g: FRGraph, alpha: Double): FRGraph = {
    // 1) compute support for each edge
    val mergeGraph = g.mapTriplets(e => {
      // a) union the nodes' sub-node sets
      val src = e.srcAttr
      val dst = e.dstAttr
      val nodes = src.nodes ++ dst.nodes
      val size = nodes.size.toDouble
      // b) for each path p:
      //   i) union its node sets
      val intervals = (src.intervals.keySet ++ dst.intervals.keySet).map(p => {
        if (src.intervals.contains(p) && !dst.intervals.contains(p)) {
          p -> src.intervals(p)
        } else if (!src.intervals.contains(p) && dst.intervals.contains(p)) {
          p -> dst.intervals(p)
        } else {
          p -> combineIntervals(src.intervals(p) ++ dst.intervals(p))
        }
      }).toMap
      //   ii) compute alpha (penetrance: fraction of nodes it traverses)
      //   iii) add p to supporting if it satisfies alpha/kappa constraints
      val supporting = intervals.filter{case (p, i) => {
        i.filter{case (_, _, s) => alpha <= (s / size)}.nonEmpty
      }}.keySet
      FRVertex(nodes, intervals, supporting)
    }).mapVertices((id, _) => (true, (0, 0L)))
    // 2) greedily compute maximal weighted matching (greedy is ideal here)
    val initial = (-1, -1L)
    val matched = mergeGraph.pregel(initial, Int.MaxValue, EdgeDirection.Either)(
      // Vertex Program
      (id, prev, msg) => {
        val (active, weightMatch) = prev
        if (active) {
          (!(weightMatch == msg), msg)
        } else {
          prev
        }
      },
      // Send Message
      edgeTriplet => {
        // TODO: is src always the current vertex?
        val src = edgeTriplet.srcId
        val dst = edgeTriplet.dstId
        val (srcActive, (srcWeight, srcMatch)) = edgeTriplet.srcAttr
        val (dstActive, (dstWeight, dstMatch)) = edgeTriplet.dstAttr
        val w = edgeTriplet.attr.nodes.size
        if (srcActive && dstActive) {
          val m = if (srcWeight <= w) (w, src) else initial
          Iterator((dst, m))
        } else if (!srcActive && dstActive && srcMatch == dst) {
          Iterator((dst, (w, src)))
        } else {
          Iterator.empty
        }
      },
      // Merge Message
      (a, b) => if (a._1 > b._1 || (a._1 == b._1 && a._2 > b._2)) a else b
    )
    // 3) construct new FRGraph by contracting matching edges
    val contractions = matched.triplets.filter(e => {
      val (srcActive, (_, srcMatch)) = e.srcAttr
      val (dstActive, (_, dstMatch)) = e.dstAttr
      !srcActive && !dstActive && e.srcId == dstMatch && e.dstId == srcMatch
    }).map(e => {
      (math.min(e.srcId, e.dstId), e.attr)
    })
    val edges: RDD[FREdge] = matched.triplets.filter(e => {
      val (srcActive, (_, srcMatch)) = e.srcAttr
      val (dstActive, (_, dstMatch)) = e.dstAttr
      !(!srcActive && !dstActive && e.srcId == dstMatch && e.dstId == srcMatch)
    }).map(e => {
      val (srcActive, (_, srcMatch)) = e.srcAttr
      val (dstActive, (_, dstMatch)) = e.dstAttr
      if (!srcActive && !dstActive) {
        Edge(math.min(e.srcId, srcMatch), math.min(e.dstId, dstMatch))
      } else if (!srcActive && dstActive) {
        Edge(math.min(e.srcId, srcMatch), e.dstId)
      } else if (srcActive && !dstActive) {
        Edge(e.srcId, math.min(e.dstId, dstMatch))
      } else {
        Edge(e.srcId, e.dstId)
      }
    })
    Graph(g.vertices, edges).joinVertices(contractions)((id, _, a) => a)
  }

  // combines intervals that are overlapping into a single interval
  def combineIntervals(
    intervals: Array[(Double, Double, Int)]
  ): Array[(Double, Double, Int)] = {
    // 1) create an array of (begin/end, increment, penetrance) tuples
    val (begins, ends) = intervals.map{case (begin, end, count) => {
      ((begin, 1, count), (end, -1, 0))
    }}.unzip
    val intervalPoints = (begins ++ ends).sortBy{case (p, i, _) => (p, -i)}
    // 2) combine overlapping intervals into a single interval
    val combinedIntervals = Array[(Double, Double, Int)]()
    var counter = 0
    var begin: Double = 0
    var penetrance = 0
    for ((p, i, c) <- intervalPoints) {
      if (counter == 0) {
        begin = p
        penetrance = 0
      }
      counter += i
      penetrance += c
      if (counter == 0) {
        combinedIntervals :+ (begin, p, penetrance)
      }
    }
    combinedIntervals
  }

  // identifies groups of nodes (regions) that are frequently traversed by a
  // consistent set of paths
  def frequentedRegions(g: GeneGraph, alpha: Double, kappa: Integer) = {
    // create the initial FR graph
    val halfKappa = kappa.toDouble / 2
    var frGraph: FRGraph = g.mapVertices((id, v) =>  {
      val intervals = v.paths.map{case (p, nums) => {
        val rawIntervals = nums.toArray.map(n => {
          (n.toDouble - halfKappa, n.toDouble + halfKappa, 1)
        })
        (p, combineIntervals(rawIntervals).map{case (b, e, _) => (b, e, 1)})
      }}
      FRVertex(Array(id), intervals, intervals.keySet)
    })
    // perform hierarchical clustering via coarsening
    while (frGraph.numVertices > 1) {
      frGraph = coarsen(frGraph, alpha)
    }
  }
}
