package org.miguelho.kmeans.util

import org.apache.spark.sql.SparkSession
import org.miguelho.kmeans.model.dataTransformation.Parser
import org.miguelho.kmeans.util.io.CommandLineArguments

case class Context(commandLineArguments: CommandLineArguments, configuration : Configuration, sparkSession: SparkSession, parser: Parser)

object Context extends Serializable with Spark with ParserTrait {

  def init(args: Array[String]): Context = {
    println("Initialize Context")

    val cmd = CommandLineArguments(args(0), args(1))

    val tableName = List("antennas", "cities", "clients", "events")

    val dataModel = DataModel(
      "",
      tableName.foldLeft(Map.empty[String, TableMetaData])( (a,b) => a + (b -> TableMetaData("", b, Some(s"${cmd.input}/$b"), Some("csv"), None) ))
    )

    val metaConf = MetaConfiguration("dd/MM/yyyy-HH:mm:ss.SSS",//date pattern
      Events(dataModel))

    noLogs()

    Context(cmd,Configuration(metaConf),spark, parser)
  }

  def noLogs(): Unit = spark.sparkContext.setLogLevel("ERROR")

}
