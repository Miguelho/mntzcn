package org.miguelho.kmeans.util

case class Configuration(metaConfiguration: MetaConfiguration)

case class MetaConfiguration(dateFormat: String, events: Events)

case class Events(dataModel: DataModel)

/**
  * @param schema Schema
  * @param tables hdfsFiles
  * */
case class DataModel(schema: String, tables: Map[String, TableMetaData])

case class TableMetaData(schema: String, tableName: String, location: Option[String], fileFormat: Option[String], column: Option[String]){}

