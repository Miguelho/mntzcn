package org.miguelho.kmeans.model.dataTransformation

import org.miguelho.kmeans.util.Context
import org.scalatest.WordSpec

class ContextSpecification extends WordSpec {

  val args: Array[String] = Array(
    s"${System.getProperty("user.dir")}/utad"
  )

  @transient lazy implicit val ctx: Context = Context.init(args)

}
