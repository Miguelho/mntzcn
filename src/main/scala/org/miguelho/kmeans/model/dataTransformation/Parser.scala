package org.miguelho.kmeans.model.dataTransformation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.miguelho.kmeans.model.{Antenna, City, Client, Coordinate, TelephoneEvent}
import org.miguelho.kmeans.util.Context

object Parser {

  implicit class PostProcess(dataset: Dataset[_]){

    def header: String = dataset.take(1)(0).getClass.getDeclaredFields.map(_.getName).mkString(",")

    def toText: RDD[String] = {
      implicit val encoder: ExpressionEncoder[String] = ExpressionEncoder[String]
      dataset.sparkSession.sparkContext.
        parallelize[String](Array[String](header)).
        union(
          dataset.
            map( _.toString).
            coalesce(1).
            rdd
        ).
        coalesce(1)
    }
  }
}

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
    val separator = ";"
    val datePattern = raw"(\d{7}[A-Z]{1})$separator(\d{2}).*(\d{2}).*(\d{4})-(\d{2}):(\d{2}):(\d{2})\.(\d{3})$separator(A\d{2})".r

    row.mkString(separator).trim match {
      case datePattern(clientId,day,month,year,hour,minutes,seconds,ms,antennaId) =>
        Some(TelephoneEvent(clientId, s"$day/$month/$year-$hour:$minutes:$seconds.$ms", antennaId))
      case _ => None
    }

  }

  def parseAntenna(row: Row): Antenna = {
    row.mkString(";").split(";").map(_.trim) match {
      case Array(antennaId, intensity, x, y) =>
        Antenna(antennaId, intensity.toInt, x.toDouble, y.toDouble)
    }
  }

  def parseCity(row: Row): Option[City] = {
    def toCoordinate(coordinateAsArray: Array[String]): Coordinate = {
      Coordinate(coordinateAsArray(0).toDouble,coordinateAsArray(1).toDouble)
    }

    row.mkString(";").split(";").map(_.trim) match {
      case Array(cityName, population, x1,x2,x3,x4,x5) =>
        Seq(x1,x2,x3,x4,x5).map(_.split(",")).map(toCoordinate) match {
          case Seq(c1,c2,c3,c4,c5) =>
            Some(City(cityName, population.toInt, c1,c2,c3,c4,c5))
          case _ => None
        }
      case _ => None
    }
  }

  def parseClient(row: Row): Option[Client] = {
    row.mkString(";").split(";").map(_.trim) match {
      case Array(clientId, age, gender, nationality, civilStatus, socioeconomicLevel) =>
        Some(Client(clientId, age.toInt, gender, nationality, civilStatus, socioeconomicLevel))
      case _ => None
    }
  }
}