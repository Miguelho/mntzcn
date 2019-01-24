package org.miguelho.kmeans.util.io

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Try}

object HDFSStorage extends HDFSStorage {
  override val fs: FileSystem = FileSystem.get(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)
}

trait HDFSStorage extends Storage[FileSystem] {

  def save(dataset: Dataset[_],
           path: String,
           folder: String = s"${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM_dd-HH-mm-ss"))}",
           format: String = "csv"): Unit = {

    val outputPath = s"$path/$folder"
    val writer = dataset.coalesce(1).write.option("header","true").option("delimiter",";")

    Try(writer.
      format(format).
      save(outputPath)) match {
        case Failure(_) =>
          val fallbackFolder = s"$outputPath-${LocalTime.now().getHour}${LocalTime.now().getMinute}"
          this.logger.warn(s"ERROR while HDFSStorage.save. Saving into folder $fallbackFolder")
          writer.csv(fallbackFolder)
          val outputName = fs.globStatus(new Path(s"$fallbackFolder/part*"))(0).getPath.getName
          rename(s"$fallbackFolder/$outputName", s"$fallbackFolder/analysis.$format")
        case _ =>
          val outputName = fs.globStatus(new Path(s"$outputPath/part*"))(0).getPath.getName
          rename(s"$outputPath/$outputName", s"$outputPath/analysis.$format")
          this.logger.info(s"Data saved in $outputPath")
    }
  }

  override def rename(old: String, newPath: String): Unit = {
    fs.rename(new Path(old), new Path(newPath))
  }

}
