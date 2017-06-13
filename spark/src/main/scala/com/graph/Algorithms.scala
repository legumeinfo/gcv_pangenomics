package graph

// graph
import graph.types.{GeneGraph, GeneVertex}
// Apache Spark
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}

class Algorithms(sc: SparkContext) {
  def approximateFrequentSubpaths(
    g: GeneGraph,
    chromosomeId: Long,
    intermediate: Int,
    matched: Int
  ) = {
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
          for (relN <- v.paths(c)) yield (c, (refN, relN))
        }
      }.flatten }.flatten
    }}.sortBy(t => t).groupByKey
    // find inexact matching forward and reverse intervals (islands)
    val int = intermediate + 1
    val intervals = chromosomes.map{case (c, pairs) => {
      val indexed = pairs.zipWithIndex
      val forward = indexed.map{case ((refN, relN), i) => {
        val refCount = (refN-i)/int
        val relCount = (relN-i)/int
        ((refCount, relCount), (refN, relN))
      }}.groupBy{case (count, number) => count}
      val reverse = indexed.map{case ((refN, relN), i) => {
        val refCount = (refN-i)/int
        val relCount = (relN-(pairs.size-(i+1)))/int
        ((refCount, relCount), (refN, relN))
      }}.groupBy{case (count, number) => count}
      def multiMinMax(pairs: Iterable[((Int, Int), (Int, Int))]):
      (Int, Int, Int, Int) = {
        val refMin = pairs.minBy{case (count, (refN, relN)) => refN}._2._1
        val refMax = pairs.maxBy{case (count, (refN, relN)) => refN}._2._1
        val relMin = pairs.minBy{case (count, (refN, relN)) => relN}._2._2
        val relMax = pairs.maxBy{case (count, (refN, relN)) => relN}._2._2
        (refMin, refMax, relMin, relMax)
      }
      def largeEnough(nums: (Int, Int, Int, Int)): Boolean = {
        nums._2 - nums._1 + 1 >= matched
      }
      val forwardIntervals = forward.values.map(multiMinMax).filter(largeEnough)
      val reverseIntervals = reverse.values.map(multiMinMax).filter(largeEnough)
      (c, forwardIntervals, reverseIntervals)
    }}.filter{case (c, forward, reverse) => forward.nonEmpty || reverse.nonEmpty}
    intervals.foreach(println)
  }
  def frequentedRegions(g: GeneGraph, chrId: Long) = {
    // get all the nodes of the target chromosome
    // should be able to compute on a gene or de Bruijn graph
  }
}
