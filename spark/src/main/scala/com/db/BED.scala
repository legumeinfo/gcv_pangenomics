package db

// Java
import java.io.{PrintWriter, File}
// graph
import graph.types.FRVertex

object BED {
  def frsToBED(
    frs: Array[(Long, FRVertex)],
    alpha: Double,
    kappa: Int
  ): String = {
    val halfKappa = kappa.toDouble / 2
    frs.map{case (id, v) => {
      val size = v.nodes.size.toDouble
      v.supporting.map(p => {
        v.intervals(p)
          .filter{case (_, _, c) => (c / size) >= alpha}
          .map{case (begin, end, c) => {
            p + "\t" +
            math.round(begin + halfKappa).toInt + "\t" +
            math.round(end - halfKappa).toInt + "\t" +
            id
          }}.mkString("\n")
      }).mkString("\n")
    }}.mkString("\n")
  }
}
