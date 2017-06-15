package db

package object types {
  // class equivalents of db nodes
  case class Organism(id: Long, genus: String, species: String)
  case class Chromosome(id: Long, name: String, length: Int)
  case class Gene(
    id: Long,
    number: Int,
    strand: Int,
    name: String,
    fmin: Int, 
    fmax: Int
  )
  case class GeneFamily(id: Long, name: String)
}
