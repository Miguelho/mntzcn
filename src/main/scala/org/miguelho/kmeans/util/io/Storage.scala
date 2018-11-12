package org.miguelho.kmeans.util.io

import java.time.LocalTime

import org.apache.spark.sql.Dataset
import org.miguelho.kmeans.model.dataTransformation.Parser._

import scala.util.{Failure, Try}


object Storage {

  def save(dataset: Dataset[_], path: String, folder: String): Unit = {
    val outputPath = s"$path$folder"
    Try(dataset.toText.saveAsTextFile(outputPath)) match {
      case Failure(_) => dataset.toText.saveAsTextFile(s"$path${folder}-${LocalTime.now().getHour}${LocalTime.now().getMinute}")
      case _ => println(s"Data saved in $outputPath")
    }
  }

}

trait Storage {
  def save(dataset: Dataset[_], path: String, folder: String): Unit
}
