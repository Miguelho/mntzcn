package org.miguelho

import org.junit.runner.RunWith
import org.miguelho.kmeans.Main
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MainTest extends FlatSpec {

  it should "start up" in {
    val cmdArgs = Array("utad")

    Main.main(cmdArgs)
  }
}
