package org.miguelho.kmeans.model

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.clustering.KMeans
import org.miguelho.kmeans.model.dataTransformation._
import org.miguelho.kmeans.util.Context
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class ClusterModel {

  def process(implicit ctx: Context): Unit = {
    val parser = new Parser

    val events = parser.parseTelephoneEvents(DataReader.load("events"))
    val clients = parser.parseClients(DataReader.load("clients"))

    KMeans.train( data = extractFeatures(clients).map( t => t._2), k = 2, maxIterations = 10)

  }

  /**
    * Aggregates events by Antenna and date
    *
    * 1º Calculate PairRDD (antennaId, [TelephoneEvent])
    * 2º Calculate PairRDD (antennaId (date, List[TelephoneEvent]))
    * 3º Calculate PairRDD aggregating by antenna and date (antennaId, List[(fecha, list[TelephoneEvent])]
    *
    * @return RDD with as many values as antennas and all TelephoneEvents groupedBy Date
    * */
  def groupedEvents(events: RDD[TelephoneEvent])(implicit ctx: Context): RDD[(String, Iterable[(String, Iterable[TelephoneEvent])])] ={
    val byAntenna = events.map( event => (event.antennaId, event)).groupByKey()
    val byDate = byAntenna.flatMapValues( iterable => iterable.groupBy( ev => ev.date))
    val collect = byDate.groupByKey()
    collect
  }

  def extractFeatures(rdd: RDD[Client])(implicit ctx: Context): RDD[(String, Vector)] = ???

  def doClustering(vectors: Vector): Unit = ???

}
