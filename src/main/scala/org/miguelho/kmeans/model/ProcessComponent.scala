package org.miguelho.kmeans.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.miguelho.kmeans.model.dataTransformation._
import org.miguelho.kmeans.util.Context
import org.apache.spark.sql.{Dataset, Row}
import org.miguelho.kmeans.model.Example
import org.miguelho.kmeans.model.analisis.Analytics.{averageSquareRootDistance, printAntennaActivityComparision, printContingency}
import org.miguelho.kmeans.model.clustering.MntzcnEstimator
import org.miguelho.kmeans.util.io.Storage.save
import org.miguelho.kmeans.model.implicits._

case class JoinedRow(clientId: String, antennaId: String, K: Int, age: Int, gender: String, nat: String, civil: String, socioEco:String){
  override def toString: String = s"$clientId,$antennaId,$K,$age,$gender,$nat,$civil,$socioEco"
}

trait ProcessComponent{
  def process(implicit ctx: Context): Unit = {
    import ctx.sparkSession.implicits._

    // Load data
    val events = ctx.parser.parseTelephoneEvents(DataReader.load("events"))
    //ClientId;Age;Gender;Nationality;CivilStatus;SocioeconomicLevel
    val clientsDS = ctx.parser.
      parseClients(DataReader.load("clients")).toDS()


    val mntzcnModel = new MntzcnEstimator().
      setHyperParams(Map(
        "inputCol" -> "Date",
        "features" -> "features",
        "kmeans.numK" -> "2",
        "kmeans.maxIter" -> "1",
        "kmeans.seed" -> "1")).
      fit(events.toDS()

      )

    // Clustering model
    val predictions: Dataset[Example] = mntzcnModel.transform(events.toDS()).as

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
    printContingency(predictions.select("antennaId", "prediction"), 0 to 1)
    /*println(averageSquareRootDistance(preparedEvents, model))

    printAntennaActivityComparision(model.clusterCenters(0),model.clusterCenters(1))
    */
  }
}
