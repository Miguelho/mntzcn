package org.miguelho.kmeans.model

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.miguelho.kmeans.model.dataTransformation.{Antenna, DataReader, Parser, TelephoneEvent}
import org.miguelho.kmeans.util.Fixture
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClusterModelTest extends Fixture{

  val cut: ClusterModel = new ClusterModel
  val parser = new Parser
  val events: RDD[TelephoneEvent] = parser.parseTelephoneEvents(DataReader.load("events"))
  val antennas: RDD[Antenna] = parser.parseAntennas(DataReader.load("antennas"))

    /*"groupedEvents should run correctly" in {
      val rdd: RDD[(String, Iterable[(String, Iterable[TelephoneEvent])])] = cut.groupedEvents(events)
      assert( rdd.count() === antennas.distinct.count())
    }*/

    "extract features method should produce a tuple of 2 elements, ((clientId -  antennaId), featuresCol)" in {
      import ctx.sparkSession.implicits._

      val testEvents: Seq[String] = Seq("1665053N;03/09/2018-09:30:00.000;A01",
        "1665053N;03/09/2018-00:30:00.000;A01",
        "1665053N;03/09/2018-23:30:00.000;A01"
      )
      val rawDf = ctx.sparkSession.sparkContext.parallelize(testEvents).toDF

      val df = cut.extractFeatures(parser.parseTelephoneEvents(rawDf)).
        toDF("clientId - antennaId", "featuresCol")

      df.show()
    }

    /*"doClustering should return a KMeansModel" in {
      val model = cut.doClustering(cut.extractFeatures(events))

      assert(model.isInstanceOf[KMeansModel])
      assert( model.clusterCenters.length == 2)
      println(model.clusterCenters.mkString)
    }*/

    "ClusterModel should predict events" in {
      cut.process
    }


}
