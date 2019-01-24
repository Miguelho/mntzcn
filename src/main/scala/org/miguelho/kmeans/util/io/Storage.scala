package org.miguelho.kmeans.util.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Dataset


trait Storage[T] extends LazyLogging{
  val fs: T

  def save(dataset: Dataset[_], path: String, folder: String, format: String = "csv"): Unit
  def rename(old: String, newPath: String): Unit

}