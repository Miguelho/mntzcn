package org.miguelho.kmeans.util.io

import org.apache.spark.sql.Dataset


trait Storage[T] {
  val fs: T

  def save(dataset: Dataset[_], path: String, folder: String, format: String = "csv"): Unit
  def rename(old: String, newPath: String): Unit

}