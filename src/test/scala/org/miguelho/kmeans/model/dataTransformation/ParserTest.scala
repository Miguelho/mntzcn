package org.miguelho.kmeans.model.dataTransformation

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.miguelho.kmeans.model.{Antenna, City, Client, Coordinate, TelephoneEvent}
import org.miguelho.kmeans.util.ContextSpecification
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class ParserTest extends ContextSpecification {

  val cut: Parser = new Parser

// ISO PATTERN EXAMPLE: 2011-12-03T10:15:30
  "Parser with valid data " should {
      val date = "05/06/2017-09:30:00.000"
      val testEvent = s"1665053N;$date;A01"
      val testAntenna = "A01;100;-3.710762;40.425788"
      val testCity = "Madrid;3165541;-3.7906265259,40.3530853269;-3.5769081116,40.3530853269;-3.5769081116,40.5349377098;-3.7906265259,40.5349377098;-3.7906265259,40.3530853269"
      val testCity2 = "Logroño;150876;-2.5417900085,42.4284616342;-2.3403453827,42.4284616342;-2.3403453827,42.5200036645;-2.5417900085,42.5200036645;-2.5417900085,42.4284616342"

      "parse and" should {
        val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))
        "parse and return a valid event" in {
          val expected = TelephoneEvent("1665053N", date, "A01")
          assert(expected == parsedRow.get)
        }

        "return a valid LocalDateTime" in {
          val localDateTime = parsedRow match {
            case Some(x) => x.asLocalDateTime()
            case _ => None
          }
          val expectedDateISO = "2017-06-05T09:30"
          assert(localDateTime.toString == expectedDateISO)
        }

        "return a valid #day;hour pattern" in {
          val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))
          assert(parsedRow.get.getDayOfWeekAndHour equals "#1;9")
        }

        "return 9 when asked by the 9th hour of the first day of the week" in {
          val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))

          assert(parsedRow.get.getDayOfWeekAndHourIndex == 9)
        }

        "return 0 when asked by the 00 hour of the first day of the week" in {
          val date = "05/06/2017-00:30:00.000"
          val testEvent = s"1665053N;$date;A01"
          val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))

          assert(parsedRow.get.getDayOfWeekAndHourIndex == 0)
        }

        "return 168 when asked by the 23 hour of the last day of the week" in {
          val date = "09/09/2018-23:30:00.000"
          val testEvent = s"1665053N;$date;A01"
          val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))

          assert(parsedRow.get.getDayOfWeekAndHourIndex == 167)
        }

        "return 24 when asked by the 00 hour of the second day of the week" in {
          val date = "04/09/2018-00:30:00.000"
          val testEvent = s"1665053N;$date;A01"
          val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))

          assert(parsedRow.get.getDayOfWeekAndHourIndex == 24)
        }

        "return 164 when asked by the 20 hour of the last day of the week" in {
          val date = "09/09/2018-20:30:00.000"
          val testEvent = s"1665053N;$date;A01"
          val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))

          assert(parsedRow.get.getDayOfWeekAndHourIndex == 164)
        }
        //1665060U;05-06-/2017-09:30:00.000;A01
        "cleanUp should filter invalid characters in the DATEPATTERN" in {
          val date = "05-06-/2017-09:30:00.000"
          val testEvent = s"1665053N;$date;A01"
          val parsedRow = cut.parseTelephoneEventRows(Row.fromSeq(testEvent.split(";")))

          assert(parsedRow.isDefined)
        }

      }

      "parse and return a valid antenna" in {
        val expected = Antenna("A01",100,-3.710762d,40.425788)
        val parsedRow = cut.parseAntenna(Row.fromSeq(testAntenna.split(";")))
        assert(expected == parsedRow)
      }
    //Madrid;3165541;-3.7906265259,40.3530853269;-3.5769081116,40.3530853269;-3.5769081116,40.5349377098;-3.7906265259,40.5349377098;-3.7906265259,40.3530853269
      "parse and return a valid city" in {
        val expected = City("Madrid",3165541,Coordinate(-3.7906265259,40.3530853269),Coordinate(-3.5769081116,40.3530853269),Coordinate(-3.5769081116,40.5349377098), Coordinate(-3.7906265259,40.5349377098),Coordinate(-3.7906265259,40.3530853269))
        val parsedRow = cut.parseCity(Row.fromSeq(testCity.split(";"))).get
        assert(expected == parsedRow)
      }

      //Madrid;3165541;-3.7906265259,40.3530853269;-3.5769081116,40.3530853269;-3.5769081116,40.5349377098;-3.7906265259,40.5349377098;-3.7906265259,40.3530853269
      "An antenna in Madrid should evaluate its method 'isIn' to True given the Madrid BoundaryBox" in {
        val antenna = Antenna("A01",100,-3.710762d,40.425788d)
        val madrid = City("Madrid",3165541,Coordinate(-3.7906265259,40.3530853269),Coordinate(-3.5769081116,40.3530853269),Coordinate(-3.5769081116,40.5349377098), Coordinate(-3.7906265259,40.5349377098),Coordinate(-3.7906265259,40.3530853269))
        assert(antenna isIn madrid.toBoundaryBox)
      }

      "An antenna in Madrid should evaluate its method 'isIn' to false given the Logroño BoundaryBox" in {
        val antenna = Antenna("A01",100,-3.710762d,40.425788d)
        val logrono = cut.parseCity(Row.fromSeq(testCity2.split(";"))).get
        assert( !(antenna isIn logrono.toBoundaryBox) )
      }

      "return the right output header" in {
        import ctx.sparkSession.implicits._
        import Parser._

        val expectedHeader = "clientId,date,antennaId"
        val testEvents: Seq[String] = Seq("1665053N;03/09/2018-09:30:00.000;A01",
          "1665053N;03/09/2018-00:30:00.000;A01",
          "1665053N;03/09/2018-23:30:00.000;A01"
        )
        val rawDf = ctx.sparkSession.sparkContext.parallelize(testEvents).toDF
        val rdd = cut.parseTelephoneEvents(rawDf).toDS().toText

        val actualHeader=rdd.collect()(0)
        print(actualHeader)
        assert(actualHeader.equals(expectedHeader))
      }
    }


}
