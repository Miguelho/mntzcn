package org.miguelho.kmeans.model

object Geo

case class BoundaryBox(head: Coordinate, tail: Coordinate*) {
  val points: List[Coordinate] = List(head) ++ tail
}

case class Coordinate(long: Double, lat: Double)



