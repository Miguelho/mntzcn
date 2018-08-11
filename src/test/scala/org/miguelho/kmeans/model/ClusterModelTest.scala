package org.miguelho.kmeans.model

import org.junit.runner.RunWith
import org.miguelho.kmeans.model.dataTransformation.{DataReader, Parser}
import org.miguelho.kmeans.util.Fixture
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClusterModelTest extends Fixture{

  val cut: ClusterModel = new ClusterModel
  val parser = new Parser
    "groupedEvents should run correctly" in {
      val events  = parser.parseTelephoneEvents(DataReader.load("events"))
      val antennas = parser.parseAntennas(DataReader.load("antennas"))
      cut.groupedEvents(events)
    }

    "extract features method should apply oneHotEncoder " in {
      intercept[NotImplementedError]{
        cut.extractFeatures(parser.parseClients(DataReader.load("clients")))
      }

    }


}
