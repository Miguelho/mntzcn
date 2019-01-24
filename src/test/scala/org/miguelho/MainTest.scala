package org.miguelho

import org.junit.runner.RunWith
import org.miguelho.kmeans.Main
import org.miguelho.kmeans.util.Fixture
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MainTest extends Fixture {

  "Main should run mntzcn" in {
    Main.main(args)
  }

}
