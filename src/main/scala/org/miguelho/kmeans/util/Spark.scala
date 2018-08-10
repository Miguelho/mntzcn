package org.miguelho.kmeans.util

import org.apache.spark.sql.SparkSession

trait Spark {

  @transient lazy implicit val spark: SparkSession = buildContext(SparkSession.builder()) //buenísima implementación, permite inyectar un builder mock

  def buildContext(builder: SparkSession.Builder): SparkSession = {
    builder.
      config("spark.master","local").
      config("spark.app.name","Mntzcn").
      getOrCreate()

  }
}
