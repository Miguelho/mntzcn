package org.miguelho.kmeans.util

import org.scalatest.WordSpec

class ContextSpecification extends WordSpec {

  val dataPath = s"${System.getProperty("user.dir")}/utad"
  val outputPath = "/Users/miguelhalysortuno/Documents/Master/TFM/data/kmeans/output/"

  val args: Array[String] = Array(
    dataPath, outputPath
  )

  @transient lazy implicit val ctx: Context = Context.init(args)

}
