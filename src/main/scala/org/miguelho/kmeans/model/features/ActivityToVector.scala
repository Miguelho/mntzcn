package org.miguelho.kmeans.model.features

import java.util.UUID

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.miguelho.kmeans.model.{Activity, Inactivity, Sample}
import org.miguelho.kmeans.model.implicits._

object ActivityToVector extends DefaultParamsReadable[ActivityToVector]

class ActivityToVector(override val uid: String) extends Transformer with DefaultParamsWritable{
  def this() = this(UUID.randomUUID().toString)

  final val inputCol = new Param[String](this,"date","The input Col to normalize")
  final val outputCol = new Param[String](this,"features","The output Col to normalize")

  def setInputCol(value:String): this.type = set(inputCol,value)
  def setOutputCol(value:String): this.type = set(outputCol,value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    import dataset.sparkSession.implicits._

    dataset.as(eventEncoder).
      rdd.
      groupBy(event => (event.clientId, event.antennaId)).
      flatMapValues(
        _.map(_.getDayOfWeekAndHourIndex)).
      groupByKey.
      mapValues(t => List.tabulate(168)( i => {
        if(t.toSeq.contains(i)) Activity else Inactivity
      })).
      map(s => Sample(s._1._1, s._1._2, Vectors.dense(s._2.toArray))).
      toDF("clientId","antennaId",$(outputCol))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    ScalaReflection.schemaFor[Sample].dataType.asInstanceOf[StructType]
  }

}
