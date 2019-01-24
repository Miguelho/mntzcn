package org.miguelho.kmeans.util

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, WordSpec}

class ContextSpecification extends WordSpec with BeforeAndAfter{

  val factPath = s"${System.getProperty("user.dir")}/utad"
  val dimensionPath = s"${System.getProperty("user.dir")}/utad"
  val outputPath = "/Users/miguelhalysortuno/Documents/Master/TFM/data/kmeans/output/"

  val args: Array[String] = Array(
    factPath, dimensionPath, outputPath
  )

  System.setSecurityManager(null) // FIXME Spark 2.3.0 requires it

  @transient lazy implicit val ctx: Context = Context.init(args)

  override protected def before(fun: => Any)(implicit pos: Position): Unit = {
    FileUtils.deleteDirectory(new File("metastore_db"))
  }
}
