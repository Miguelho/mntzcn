package org.miguelho.kmeans

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

package object model {

  /**
    * ClientId;Date;AntennaId
    * 1665053N;05/06/2017-09:30:00.000;A01
    */
  case class TelephoneEvent(clientId: String, date: String,  antennaId: String){
    //"2007-12-03T10:15:30"
    def asLocalDateTime(): LocalDateTime = {
      LocalDateTime.parse(date, DateTimeFormatter.ofPattern("dd/MM/yyyy-HH:mm:ss.SSS"))
    }

    def getDayOfWeekAndHour: String = {
      s"#${asLocalDateTime().getDayOfWeek.getValue};${asLocalDateTime().getHour}"
    }

    def getDayOfWeekAndHourIndex: Int = {
      ( 24 * ( asLocalDateTime().getDayOfWeek.getValue - 1) ) + asLocalDateTime().getHour
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

}
