package org.miguelho.kmeans.model.dataTransformation

import org.apache.spark.sql.{DataFrame, Dataset}
import org.miguelho.kmeans.util.Context

object DataReader extends DataReaderTrait{

  case class TableName(name: String)

  case object TableNames {
    val event = TableName("event")
    val city = TableName("city")
    val antenna = TableName("antenna")
    val client = TableName("client")
  }

  def load(tableName: TableName)(implicit ctx: Context): DataFrame = {
    super.load(ctx.configuration.metaConfiguration.events.dataModel.tables(tableName.name).location)
  }

}

trait DataReaderTrait {

  protected def load(path: Option[String], format: String = "csv")(implicit ctx: Context): DataFrame = {
    val spark = ctx.sparkSession
    path match {
      case Some(x) =>
        spark.read.format(format).option("header","true").load(x)
      case None =>
        spark.emptyDataFrame
    }
  }
}

