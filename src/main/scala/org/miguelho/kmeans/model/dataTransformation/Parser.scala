package org.miguelho.kmeans.model.dataTransformation

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.miguelho.kmeans.model.{BoundaryBox, Coordinate}
import org.miguelho.kmeans.util.Context

class Parser extends Serializable {

  def parseTelephoneEvents(raw: DataFrame)(implicit ctx: Context): RDD[TelephoneEvent] = {
    import ctx.sparkSession.implicits._
    raw.map(
        (r: Row) => parseTelephoneEventRows(r).get
      ).rdd
  }

  def parseAntennas(raw: DataFrame)(implicit ctx: Context): RDD[Antenna] = {
    import ctx.sparkSession.implicits._
    raw.map(
      (r: Row) => parseAntenna(r)
    ).rdd
  }

  def parseClients(raw: DataFrame)(implicit ctx: Context): RDD[Client] = {
    import ctx.sparkSession.implicits._
    raw.map(
      (r: Row) => parseClient(r).get
    ).rdd
  }

  def parseTelephoneEventRows(row: Row): Option[TelephoneEvent] = {
    val values = row.mkString(";").split(";").map(_.trim).toList
    if (values.isEmpty)
      None
    else
      Some(TelephoneEvent(values.head,values(1),values(2)))
  }

  def parseAntenna(row: Row): Antenna = {
    Antenna(row.getString(0), row.getString(1).toInt, row.getString(2).toDouble, row.getString(3).toDouble)
  }

  def parseCity(row: Row): Option[City] = {
    val values = row.mkString(";").split(";").map(_.trim).toList
    val coordinates = values.slice(2, values.size).map( coordinate => toCoordinate(coordinate.split(",")))
    if (values.isEmpty)
      None
    else
      Some(City(values.head, values(1).toInt,
        coordinates.head,
        coordinates(1),
        coordinates(2),
        coordinates(3),
        coordinates(4)
      ))
  }

  def toCoordinate(coordinateAsArray: Array[String]): Coordinate = {
    Coordinate(coordinateAsArray(0).toDouble,coordinateAsArray(1).toDouble)
  }

  def parseClient(row: Row): Option[Client] = {
    val values = row.mkString(";").split(";").map(_.trim).toList
    if (values.isEmpty)
      None
    else
      Some(Client(values.head, values(1).toInt,
        values(2), values(3),
        values(4), values(5)
      ))
  }
}

/**
  * ClientId;Date;AntennaId
  * 1665053N;05/06/2017-09:30:00.000;A01
  */
case class TelephoneEvent(clientId: String, date: String,  antennaId: String) extends Serializable {
  val pattern = "dd/MM/yyyy-HH:mm:ss.SSS"
  //"2007-12-03T10:15:30"
  def asLocalDateTime(): LocalDateTime = {
    LocalDateTime.parse(date, DateTimeFormatter.ofPattern(pattern))
  }
}

/**
  * AntennaId;Intensity;X;Y
  * A01;100;-3.710762;40.425788
  * */
case class Antenna(antennaId: String, intensity: Int, lng: Double, lat: Double) {

  val point: Coordinate = Coordinate(lng,lat)

  /**
    * Calculates if the antenna is within a Boundary Box
    *
    * @see Implementation inspired by https://stackoverflow.com/a/26290444/7508578
    */
  def isIn(bb: BoundaryBox): Boolean = {
    val boundaryBox: List[Coordinate] = bb.points
    boundaryBox.sliding(2).foldLeft(false) { case (c, List(i, j)) =>
      val cond = {

        ( (i.lat <= point.lat && point.lat < j.lat) || (j.lat <= point.lat && point.lat < i.lat) ) &&
          (
            point.long < (j.long - i.long) * (point.lat - i.lat) / (j.lat - i.lat) + i.long
            )
      }


      if (cond) !c else c
    }
  }
}

/**
  * CityName;Population;X1;X2;X3;X4;X5
  * Madrid;3165541;-3.7906265259,40.3530853269;-3.5769081116,40.3530853269;-3.5769081116,40.5349377098;-3.7906265259,40.5349377098;-3.7906265259,40.3530853269
  * */
case class City(cityName: String, population: Int, x1: Coordinate, x2: Coordinate, x3: Coordinate, x4: Coordinate, x5: Coordinate) {
  def toBoundaryBox: BoundaryBox = BoundaryBox(x1,x2,x3,x4,x5)
}

/**
  * ClientId;Age;Gender;Nationality;CivilStatus;SocioeconomicLevel
  * 1666517V;50;F;JPN;Single;Low
  * */
case class Client(clientId: String, age: Int, gender: String, nationality: String, civilStatus: String, socioeconomicLevel: String)