package graph

// scala
import scala.collection.mutable.{Map, SortedMap}  // source copied from 2.12.x...
// graph
import graph.types.{FRGraph, FREdge,  FRVertex, GeneGraph, GeneVertex,
                    Interval, Intervals}
// Apache Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId, VertexRDD}

object Algorithms {
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

  def coarsen(
    g: Graph[V, E],
    pred: Triplet[V, E] => Boolean,
    reduce: (V,V) => V
  ): Graph[V,E] = {
    // Restrict graph to contractable edges
    val subG = g.subgraph(v => True, pred)
    // Compute connected component id for all V
    val cc: Col[Id, Id] = ConnectedComp(subG).vertices
    // Merge all vertices in same component
    val superVerts = g.vertices.leftJoin(cc).map {
    (vId, (vProp, cc)) => (cc, vProp))
    }.reduceByKey(reduce)
    // Link remaining edges between components
    val invG = g.subgraph(v=>True, !pred)
    val remainingEdges =
    invG.leftJoin(cc).triplets.map {
    e => ((e.src.cc, e.dst.cc), e.attr)
    }
    // Return the final graph
    Graph(superVerts, remainingEdges)
  }

  def cluster(g: FRGraph, alpha: Double): FRGraph {
    g
  }

  //def coarsen(g: FRGraph, alpha: Double): FRGraph = {
  //  // 1) compute support for each edge
  //  var mergeGraph = g.mapTriplets(e => {
  //    // a) union the nodes' sub-node sets
  //    val src = e.srcAttr
  //    val dst = e.dstAttr
  //    val nodes = src.nodes ++ dst.nodes
  //    val size = nodes.size.toDouble
  //    // b) for each path p:
  //    //   i) union its node sets
  //    val intervals = (src.intervals.keySet ++ dst.intervals.keySet).map(p => {
  //      if (src.intervals.contains(p) && !dst.intervals.contains(p)) {
  //        p -> src.intervals(p)
  //      } else if (!src.intervals.contains(p) && dst.intervals.contains(p)) {
  //        p -> dst.intervals(p)
  //      } else {
  //        p -> combineIntervals(src.intervals(p) ++ dst.intervals(p))
  //      }
  //    }).toMap
  //    //   ii) compute alpha (penetrance: fraction of nodes it traverses)
  //    //   iii) add p to supporting if it satisfies alpha/kappa constraints
  //    val supporting = intervals.filter{case (p, i) => {
  //      i.filter{case (_, _, s) => alpha <= (s / size)}.nonEmpty
  //    }}.keySet
  //    FRVertex(nodes, intervals, supporting)
  //  }).mapVertices((id, _) => -1L)
  //  mergeGraph.cache()
  //  var vertices = mergeGraph.vertices
  //  val edges = mergeGraph.edges
  //  // 2) greedily compute maximal weighted matching (greedy is ideal here)
  //  while (mergeGraph.numEdges > 0) {
  //    val pairedVertices = mergeGraph
  //      .collectEdges(EdgeDirection.Either)
  //      .mapValues((id, edges) => {
  //        if (edges.isEmpty) {
  //          -1L
  //        } else {
  //          val e = edges.reduce((e1, e2) => {
  //            val id1 = if (e1.srcId == id) e1.dstId else e1.srcId
  //            val id2 = if (e2.srcId == id) e2.dstId else e2.srcId
  //            val sup1 = e1.attr.supporting.size
  //            val sup2 = e2.attr.supporting.size
  //            if (sup1 > sup2 || (sup1 == sup2 && id1 > id2)) {
  //              e1
  //            } else {
  //              e2
  //            }
  //          })
  //          if (e.srcId == id) {
  //            e.dstId
  //          } else {
  //            e.srcId
  //          }
  //        }
  //      })
  //    val matchedVertices = Graph(pairedVertices, mergeGraph.edges)
  //      .aggregateMessages[Long](
  //        ec => {
  //          if (ec.srcId == ec.dstAttr && ec.dstId == ec.srcAttr) {
  //            ec.sendToSrc(ec.srcAttr)
  //            ec.sendToDst(ec.dstAttr)
  //          }
  //        },
  //        (a, b) => a  // each vertex will only receive one message
  //      )
  //    vertices = vertices.leftZipJoin(matchedVertices)((id, v1, v2) => {
  //      v2.getOrElse(v1)
  //    })
  //    val mergeVertices = mergeGraph.vertices.leftZipJoin(matchedVertices)(
  //      (id, v1, v2) => {
  //        v2.getOrElse(v1)  // v1 should always be -1
  //      }
  //    )
  //    mergeGraph = Graph(mergeVertices, mergeGraph.edges).subgraph(
  //      vpred = (id, v) => v == -1
  //    )
  //  }
  //  mergeGraph.unpersist()
  //  // 3) construct new FRGraph by contracting matching edges
  //  val contractionGraph = Graph(vertices, edges)
  //  contractionGraph.cache()
  //  val contractedEdges: RDD[FREdge] = contractionGraph
  //    .triplets.filter(e => {
  //      !(e.srcId != e.dstAttr && e.dstId != e.srcAttr)
  //    }).map(e => {
  //      val src = if (e.srcAttr == -1) e.srcId else math.min(e.srcId, e.srcAttr)
  //      val dst = if (e.dstAttr == -1) e.dstId else math.min(e.dstId, e.dstAttr)
  //      Edge(src, dst)
  //    })
  //  val contractedVertices = contractionGraph
  //    .aggregateMessages[FRVertex](
  //      ec => {
  //        if (ec.srcId == ec.dstAttr && ec.dstId == ec.srcAttr) {
  //          if (ec.srcId < ec.dstId) {
  //            ec.sendToSrc(ec.attr)
  //          } else {
  //            ec.sendToDst(ec.attr)
  //          }
  //        }
  //      },
  //      // each vertex will only receive one message
  //      (a, b) => a
  //    )
  //  contractionGraph.unpersist()
  //  val contractedGraph = Graph(g.vertices, contractedEdges)
  //    .joinVertices(contractedVertices)((id, _, a) => a)
  //  contractedGraph.outerJoinVertices(contractedGraph.degrees) {
  //    (id, attr, deg) => (attr, deg.getOrElse(0))
  //  }.subgraph(vpred = (id, attr) => attr._2 > 0)
  //   .mapVertices((id, attr) => attr._1)
  //}

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
    var combinedIntervals = Array[(Double, Double, Int)]()
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
        combinedIntervals = combinedIntervals :+ (begin, p, penetrance)
      }
    }
    combinedIntervals
  }

  // identifies groups of nodes (regions) that are frequently traversed by a
  // consistent set of paths
  def frequentedRegions(
    g: GeneGraph,
    alpha: Double,
    kappa: Integer,
    minsize: Int,
    minsupport: Int
  ): VertexRDD[FRVertex] = {
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
    frGraph.cache()
    // perform hierarchical clustering via coarsening
    var iFRs = frGraph.vertices.filter{case (id, attr) => {
      attr.nodes.size >= minsize && attr.supporting.size >= minsupport
    }}
    while (frGraph.numVertices > 1) {
      frGraph = cluster(frGraph, alpha)
      val newFRs = frGraph.vertices.filter{case (id, attr) => {
        attr.nodes.size >= minsize && attr.supporting.size >= minsupport
      }}
      val subFRs = newFRs.flatMap{case (id, attr) => {
        attr.nodes.map(n => (n, attr.supporting.size))
      }}
      //iFRs = Graph(iFRs.minus(iFRs.innerJoin(subFRs){case (id, attr, _) => attr}) ++ newFRs, iFRs.context.emptyRDD[FREdge]).vertices
      iFRs = Graph(
        iFRs.leftJoin(subFRs){case (id, attr, support) => {
          (attr, attr.supporting.size > support.getOrElse(-1))
        }}.filter{case (id, (attr, keep)) => keep}.mapValues(attr => attr._1)
         ++ newFRs,
        iFRs.context.emptyRDD[FREdge]
      ).vertices
    }
    iFRs
  }
}
