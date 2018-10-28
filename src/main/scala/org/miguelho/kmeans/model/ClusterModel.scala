package org.miguelho.kmeans.model

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.Statistics
import org.miguelho.kmeans.model.dataTransformation._
import org.miguelho.kmeans.util.Context
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.miguelho.kmeans._

case class JoinedRow(clientId: String, antennaId: String, K: Int, age: Int, gender: String, nat: String, civil: String, socioEco:String){
  override def toString: String = s"$clientId,$antennaId,$K,$age,$gender,$nat,$civil,$socioEco"
}

class ClusterModel extends Serializable {

  def process(implicit ctx: Context): Unit = {
    import ctx.sparkSession.implicits._

    val events = ctx.parser.parseTelephoneEvents(DataReader.load("events"))

    val Array(train, validation) = events.randomSplit(Array[Double](0.8d,0.2d), 1)

    //Feature extraction
    val preparedEvents = extractFeatures(train).cache

    //Clustering model
    val model = trainModel(preparedEvents)

    //Print model
    println("******")
    model.clusterCenters.foldLeft(0)((k, vector ) => {println(k, vector); k+1})
    println("******")

    //Prediction
    /*val predictions = extractFeatures(test).
      mapValues( l => Vectors.dense(l.toArray)).
      mapValues( v => model.predict(v))*/
    println(model.summary)
    val validationDS = extractFeatures(validation)
    val predictions: Dataset[Example] = model.transform(validationDS).as[Example]

    //ClientId;Age;Gender;Nationality;CivilStatus;SocioeconomicLevel
    val clientsDS = ctx.parser.
      parseClients(DataReader.load("clients")).toDS()

    def parseJoinRow( r: Row): JoinedRow = {
      JoinedRow(r.getString(0), r.getString(1), r.getInt(3), r.getInt(4), r.getString(5), r.getString(6), r.getString(7),r.getString(8))
    }


    // Split tuple, join in with Clients DF, groupBy X, convert to JoinedRow
    val predictionsWithclients = predictions.
      join(clientsDS, "clientId").
      rdd.
      cache().map(parseJoinRow).toDS()

    val folder = "today"

    predictionsWithclients.toText.saveAsTextFile(s"/Users/miguelhalysortuno/Documents/Master/TFM/data/kmeans/output/$folder")

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

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

  def extractFeatures(events: RDD[TelephoneEvent])(implicit ctx: Context): Dataset[Sample] = {
    import ctx.sparkSession.implicits._

    events.groupBy(event => (event.clientId, event.antennaId)).
      flatMapValues(list =>
        list.map(evnt => evnt.getDayOfWeekAndHourIndex)).
      groupByKey.
      mapValues(t => List.tabulate(168)( i => {
        if(t.toSeq.contains(i)) Activity else Inactivity
      })).
      mapValues(iterable => iterable).map(s => Sample(s._1._1, s._1._2, Vectors.dense(s._2.toArray))).toDS
  }

  def trainModel(vectors: Dataset[Sample])(implicit ctx: Context): KMeansModel = {
    new KMeans().setK(2).fit(vectors)
  }
  implicit val encoder: ExpressionEncoder[Sample] = ExpressionEncoder[Sample]
  implicit val encoder2: ExpressionEncoder[Example] = ExpressionEncoder[Example]

  implicit class PostProcess(dataset: Dataset[_]){
    def toText: RDD[String] = {
      implicit val encoder: ExpressionEncoder[String] =  ExpressionEncoder[String]
      dataset.map( joinedRow => joinedRow.toString).coalesce(1).rdd
    }
  }
}

case class Sample(clientId: String, antennaId: String, features: org.apache.spark.ml.linalg.Vector)
case class Example(clientId: String, antennaId: String, features: org.apache.spark.ml.linalg.Vector, prediction: Int)