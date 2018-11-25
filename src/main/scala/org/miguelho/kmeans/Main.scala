package org.miguelho.kmeans

import org.miguelho.kmeans.model.ProcessComponent
import org.miguelho.kmeans.util.Context

object Main extends Serializable with ProcessComponent{

  def main(args: Array[String]): Unit = {
    val ctx = Context.init(args)
  }
}
