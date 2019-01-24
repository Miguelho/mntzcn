package org.miguelho.kmeans.util

import org.apache.spark.sql.SparkSession
import org.miguelho.kmeans.model.dataTransformation.Parser
import org.miguelho.kmeans.util.io.CommandLineArguments

case class Context(commandLineArguments: CommandLineArguments,
                   configuration : Configuration,
                   sparkSession: SparkSession, parser: Parser)

object Context extends Serializable with ParserTrait {

  def init(args: Array[String]): Context = {
    println("Initialize Context")

    val cmd = args match {
      case Array(sparkUrl, event_input, dimension_input, output) => CommandLineArguments(sparkUrl, event_input, dimension_input, output)
      case Array(event_input, dimension_input, output) => CommandLineArguments("local", event_input, dimension_input,output)
      case _ => CommandLineArguments("local", "utad", "utad","result")
    }


    val factTable = List("event")
    val dimensionTable = List("antenna", "city", "client")

    val dataModel = DataModel(
      "",
      dimensionTable.foldLeft(Map.empty[String, TableMetaData])( (a,b) => a + (b -> TableMetaData("", b, Some(s"${cmd.dimension_input}/$b"), Some("csv"), None) )) ++
        factTable.foldLeft(Map.empty[String, TableMetaData])( (a,b) => a + (b -> TableMetaData("", b, Some(s"${cmd.fact_input}/$b"), Some("csv"), None) ))
    )

    val metaConf = MetaConfiguration("dd/MM/yyyy-HH:mm:ss.SSS",//date pattern
      Events(dataModel))

    val spark = buildContext(cmd.sparkUrl, SparkSession.builder())

    noLogs(spark)

    Context(cmd,Configuration(metaConf),spark, parser)
  }

  def noLogs(spark: SparkSession): Unit = spark.sparkContext.setLogLevel("ERROR")

  def buildContext(sparkUrl: String, builder: SparkSession.Builder): SparkSession = {
    builder.
      config("spark.master", sparkUrl).
      config("spark.app.name","Mntzcn").
      enableHiveSupport().
      getOrCreate()
  }
}
