package org.miguelho

import org.junit.runner.RunWith
import org.miguelho.kmeans.Main
import org.miguelho.kmeans.util.Fixture
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MainTest extends Fixture {

  "Main should run mntzcn" in {
    val cmdArgs = Array("utad", "/Users/miguelhalysortuno/Documents/Master/TFM/data/kmeans/output")

    Main.main(cmdArgs)
  }

}
