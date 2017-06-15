package db

// Java
import java.io.{PrintWriter, File}
// db
import db.types.{ChromosomeGenes, Chromosomes, ChromosomeOrganisms}
// graph
import graph.types.{Interval, Intervals}
// Apache graph
import org.apache.spark.rdd.RDD

class JSON {
  def afsToMacroSynteny(
    chromosomeId: Long,
    intervals: Array[(Long, Intervals, Intervals)],
    intervalData: (ChromosomeGenes, Chromosomes, ChromosomeOrganisms)
  ): String = {
    val (chromosomeGenes, chromosomes, organisms) = intervalData
    val query = chromosomes(chromosomeId)
    val queryGenes = chromosomeGenes(chromosomeId)
    def blockJSON(orientedIntervals: Intervals, orientation: String): String = {
      orientedIntervals.map{case ((qMin, qMax), (cMin, cMax)) => {"{" +
        "\"start\":" + queryGenes(qMin).fmin + "," +
        "\"stop\":" + queryGenes(qMax).fmax + "," +
        "\"orientation\":\"" + orientation + "\"" +
      "}"}}.mkString(",")
    }
    val json = "{\"chromosome\":\"" + query.name + "\"," +
      "\"length\":" + query.length + ",\"tracks\":[" +
        intervals.map{case (id, forward, reverse) => {
          val o = organisms(id)
          "{\"genus\":\"" + o.genus +"\",\"species\":\"" + o.species + "\"," +
          "\"chromosome\":\"" + chromosomes(id).name + "\",\"blocks\":[" +
            blockJSON(forward, "+") +
            (if (forward.nonEmpty && reverse.nonEmpty) "," else "") +
            blockJSON(reverse, "-") +
          "]}"
        }}.mkString(",") +
      "]}"
    return json
  }

  def dump(path: String, json: String) = {
    val pw = new PrintWriter(new File(path))
    pw.write(json)
    pw.close
  }
}
