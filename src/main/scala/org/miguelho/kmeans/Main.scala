package org.miguelho.kmeans

import org.apache.hadoop.fs.FileSystem
import org.miguelho.kmeans.model.{Engine, ProcessComponent}
import org.miguelho.kmeans.util.Context
import org.miguelho.kmeans.util.io.HDFSStorage

object Main extends Serializable {

  def main(args: Array[String]): Unit = {
    val ctx = Context.init(args)

    new ProcessComponent {
      override val mntzcn: Engine = new Engine
    }.mntzcn.process(ctx)
  }
}
