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
    }}.sortBy(t => (t._1, t._2)).groupByKey
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

  def frequentedRegions(g: GeneGraph, chrId: Long) = {
    // get all the nodes of the target chromosome
    // should be able to compute on a gene or de Bruijn graph
  }
}
