package org.miguelho.kmeans.util

import org.apache.spark.sql.SparkSession

case class Context(configuration: Configuration, sparkSession: SparkSession)

object Context extends Serializable with Spark {

  def init(args: Array[String]): Context = {
    println("Initialize Context")

    val path = args(0)

    val tableName = List("antennas", "cities", "clients", "events")

    val dataModel = DataModel(
      "",
      tableName.foldLeft(Map.empty[String, TableMetaData])( (a,b) => a + (b -> TableMetaData("", b, Some(s"$path/$b"), Some("csv"), None) ))
    )

    val metaConf = MetaConfiguration("dd/MM/yyyy-HH:mm:ss.SSS",//date pattern
      Events(dataModel))


    Context(Configuration(metaConf),spark)
  }
}
