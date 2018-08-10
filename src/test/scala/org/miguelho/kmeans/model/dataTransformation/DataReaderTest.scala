package org.miguelho.kmeans.model.dataTransformation

import org.miguelho.kmeans.util.Fixture

class DataReaderTest extends Fixture {

  val cut: DataReader.type = DataReader
  "DataReader " should {

    "return a dataframe holding the events information" in {
      val antennas = cut.load("events").collect()
      assert(antennas != null)
    }

  }

}
