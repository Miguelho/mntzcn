package org.miguelho.kmeans


import org.miguelho.kmeans.model.{Engine, ProcessComponent}
import org.miguelho.kmeans.util.Context

object Main extends Serializable {

  def main(args: Array[String]): Unit = {
    val ctx = Context.init(args)

    new ProcessComponent {
      override val mntzcn: Engine = new Engine
    }.mntzcn.process(ctx)
  }
}
