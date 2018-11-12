package org.miguelho.kmeans.model

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.miguelho.kmeans.model.dataTransformation._
import org.miguelho.kmeans.util.Context
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.miguelho.kmeans._
import org.miguelho.kmeans.model.analisis.Analytics.{printContingency,averageSquareRootDistance, printAntennaActivityComparision}
import org.miguelho.kmeans.util.io.Storage.save

case class JoinedRow(clientId: String, antennaId: String, K: Int, age: Int, gender: String, nat: String, civil: String, socioEco:String){
  override def toString: String = s"$clientId,$antennaId,$K,$age,$gender,$nat,$civil,$socioEco"
}

class ClusterModel extends Serializable {

  def process(implicit ctx: Context): Unit = {
    import ctx.sparkSession.implicits._

    // Load data
    val events = ctx.parser.parseTelephoneEvents(DataReader.load("events"))
    //ClientId;Age;Gender;Nationality;CivilStatus;SocioeconomicLevel
    val clientsDS = ctx.parser.
      parseClients(DataReader.load("clients")).toDS()

    //Feature extraction
    val preparedEvents = extractFeatures(events).cache

    //Clustering model
    val model: KMeansModel = trainModel(preparedEvents)

    //Print model
    println("Center, vector")
    model.clusterCenters.foldLeft(0)((k, vector ) => {println(k, vector); k+1})

    val predictions: Dataset[Example] = model.transform(preparedEvents).as[Example]

    def parseJoinRow( r: Row): JoinedRow = {
      JoinedRow(r.getString(0), r.getString(1), r.getInt(3), r.getInt(4), r.getString(5), r.getString(6), r.getString(7),r.getString(8))
    }

    // Split tuple, join in with Clients DF, groupBy X, convert to JoinedRow
    val predictionsWithclients = predictions.
      join(clientsDS, "clientId").
      rdd.
      cache().map(parseJoinRow).toDS()

    save(predictionsWithclients, ctx.commandLineArguments.output, "today")

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    println("Average distance for each point within the same cluster: ")
    println(averageSquareRootDistance(preparedEvents, model))

    printContingency(predictions.select("antennaId", "prediction"), 0 to 1)

    printAntennaActivityComparision(model.clusterCenters(0),model.clusterCenters(1))

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

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
      map(s => Sample(s._1._1, s._1._2, Vectors.dense(s._2.toArray))).toDS
  }

  def trainModel(vectors: Dataset[Sample])(implicit ctx: Context): KMeansModel = {
    new KMeans().setK(2).setMaxIter(1).setSeed(1L).fit(vectors)
  }

  implicit val encoder: ExpressionEncoder[Sample] = ExpressionEncoder[Sample]
  implicit val encoder2: ExpressionEncoder[Example] = ExpressionEncoder[Example]

}

case class Sample(clientId: String, antennaId: String, features: org.apache.spark.ml.linalg.Vector)
case class Example(clientId: String, antennaId: String, features: org.apache.spark.ml.linalg.Vector, prediction: Int)