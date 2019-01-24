package org.miguelho.kmeans.model.features

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, StructType}
import org.junit.runner.RunWith
import org.miguelho.kmeans.model.dataTransformation.{DataReader, Parser}
import org.miguelho.kmeans.model.{Antenna, Sample, TelephoneEvent}
import org.miguelho.kmeans.util.Fixture
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ActivityToVectorTest extends Fixture{

  val parser = new Parser
  val events: RDD[TelephoneEvent] = parser.parseTelephoneEvents(DataReader.load(DataReader.TableNames.event))
  val antennas: RDD[Antenna] = parser.parseAntennas(DataReader.load(DataReader.TableNames.antenna))

    "extract features method should produce a tuple of 2 elements, ((clientId -  antennaId), featuresCol)" in {
      import ctx.sparkSession.implicits._
      // GIVEN
      val testEvents: Seq[String] = Seq("1665053N;03/09/2018-09:30:00.000;A01",
        "1665053N;03/09/2018-00:30:00.000;A01",
        "1665053N;03/09/2018-23:30:00.000;A01"
      )
      val rawDf = ctx.sparkSession.sparkContext.parallelize(testEvents).toDF("clientId, Date, AntennaId")

      // WHEN
      val df = new ActivityToVector().
        setInputCol("Date").
        setOutputCol("features").
        transform(parser.parseTelephoneEvents(rawDf).toDS())

      // THEN
      df.show(false)
      val zippedResult = df.schema.zip(ScalaReflection.schemaFor[Sample].dataType.asInstanceOf[StructType])
      assert(zippedResult.forall({case (actual: StructField, expected: StructField) => actual.name equals expected.name}))
    }

}
