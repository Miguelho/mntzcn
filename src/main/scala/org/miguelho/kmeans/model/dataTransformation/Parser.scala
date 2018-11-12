package org.miguelho.kmeans.model.dataTransformation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.miguelho.kmeans.model.{Antenna, City, Client, Coordinate, TelephoneEvent}
import org.miguelho.kmeans.util.Context

object Parser {

  implicit class PostProcess(dataset: Dataset[_]){

    def header: String = dataset.take(1)(0).getClass.getDeclaredFields.map(field => field.getName).mkString(",")

    def toText: RDD[String] = {
      implicit val encoder: ExpressionEncoder[String] =  ExpressionEncoder[String]
      dataset.sparkSession.sparkContext.parallelize[String](Array[String](header))
        .union(
          dataset.map( joinedRow => joinedRow.toString).coalesce(1)
            .rdd)
        .coalesce(1)
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

    def prepareEvent(raw: String): TelephoneEvent = {
      raw.trim match {
        case datePattern(clientId,day,month,year,hour,minutes,seconds,ms,antennaId) =>
          TelephoneEvent(clientId, s"$day/$month/$year-$hour:$minutes:$seconds.$ms", antennaId)
      }
    }
    val values = row.mkString(separator)

    if (values.isEmpty)
      None
    else
      Some(prepareEvent(values))
  }

  def parseAntenna(row: Row): Antenna = {
    val values = row.mkString(";").split(";").map(_.trim).toList
    Antenna(values.head, values(1).toInt,
      values(2).toDouble, values(3).toDouble)
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