package org.miguelho.kmeans.model

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.miguelho.kmeans.model.dataTransformation._
import org.miguelho.kmeans.util.Context
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.miguelho.kmeans._

case class JoinedRow(clientId: String, antennaId: String, K: Int, age: Int, gender: String, nat: String, civil: String, socioEco:String)

class ClusterModel extends Serializable {

  def process(implicit ctx: Context): Unit = {
    import ctx.sparkSession.implicits._

    val events = ctx.parser.parseTelephoneEvents(DataReader.load("events"))


    val Array(train, test) = events.randomSplit(Array[Double](0.8d,0.2d), 1)

    //Feaute extraction
    val preparedEvents = extractFeatures(train)

    //Clustering model
    val model = trainModel(preparedEvents)

    //Print model
    println("******")
    model.clusterCenters.foldLeft(0)((k, vector ) => {println(k, vector); k+1})
    println("******")

    //Prediction
    val predictions = extractFeatures(test).
      mapValues( l => Vectors.dense(l.toArray)).
      mapValues( v => model.predict(v))

    predictions.foreach(t => println(t))


    //ClientId;Age;Gender;Nationality;CivilStatus;SocioeconomicLevel
    val clientsDF = ctx.parser.
      parseClients(DataReader.load("clients")).
      toDF("clientId","age","gender","nationality","civilStatus","socioeconomicLevel")

    // Split tuple, join in with Clients DF, groupBy X ...
    val processed = predictions.
      map(tuple => (tuple._1._1, tuple._1._2, tuple._2)).
      toDF("clientId", "antennaId", "K").
      join(clientsDF, "clientId").
      rdd.
      cache()

    def parseJoinRow( r: Row): JoinedRow = {
      JoinedRow(r.getString(0), r.getString(1), r.getInt(2), r.getInt(3), r.getString(4), r.getString(5), r.getString(6),r.getString(7))
    }

    // Group clusters by nationality
    processed.map(parseJoinRow).
      groupBy(r => (r.nat,r.K)).
      foreach( println )

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

  def extractFeatures(events: RDD[TelephoneEvent])(implicit ctx: Context): RDD[((String, String), List[Double])] = {
    events.groupBy(event => (event.clientId, event.antennaId)).
      flatMapValues(list =>
        list.map(evnt => evnt.getDayOfWeekAndHourIndex)).
      groupByKey.
      mapValues(t => List.tabulate(168)( i => {
        if(t.toSeq.contains(i)) Activity else Inactivity
      })).
      mapValues(iterable => iterable.toList)
  }

  def trainModel(vectors: RDD[((String, String), List[Double])])(implicit ctx: Context): KMeansModel = {
    val features: RDD[Vector] = vectors.map(s => Vectors.dense(s._2.toArray)).cache()
    KMeans.train(data = features, k = 2, maxIterations = 10 )
  }


}
