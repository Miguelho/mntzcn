package org.miguelho.kmeans.model.analisis

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Dataset
object Analytics extends Analytics {

  /**
  *
    * Prints a matrix with the real labels as rows and custer number as columns
  * */
  def printContingency(df:org.apache.spark.sql.DataFrame, labels:Seq[Int])
  {
    val rdd:RDD[(Int, Int)] = df.rdd.map(row => (row.getString(0).split("A0")(1).toInt-1, row.getInt(1))).cache()
    val numl = labels.size
    val tablew = 6*numl + 10
    var divider = "".padTo(10, '-')
    for(l <- labels)
      divider += "+---------"

    var sum:Long = 0
    print("orig.class")
    for(l <- labels)
      print("|Cluster "+l)
    println
    println(divider)
    val labelMap = scala.collection.mutable.Map[Int, (Int, Long)]()
    for(l <- labels)
    {
      //filtering by predicted labels
      val predCounts = rdd.filter(p => p._2 == l).countByKey().toList
      //get the cluster with most elements
      val topLabelCount = predCounts.max
      //if there are two (or more) clusters for the same label
      if(labelMap.contains(topLabelCount._1))
      {
        //and the other cluster has fewer elements, replace it
        if(labelMap(topLabelCount._1)._2 < topLabelCount._2)
        {
          sum -= labelMap(l)._2
          labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
          sum += topLabelCount._2
        }
        //else leave the previous cluster in
      } else
      {
        labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
        sum += topLabelCount._2
      }
      val predictions = predCounts.sortBy{_._1}.iterator
      var predcount = predictions.next()
      print("%6d".format(l.toInt+1)+"    ")
      for(predl <- labels)
      {
        if(predcount._1 == predl)
        {
          print("|%5d".format(predcount._2))
          if(predictions.hasNext)
            predcount = predictions.next()
        }
        else
          print("|        0")
      }
      println
      println(divider)
    }
    rdd.unpersist()
    println("Purity: "+sum.toDouble/rdd.count())
    println("Predicted->original label map: "+labelMap.mapValues(x => x._1))
  }

  override def averageSquareRootDistance(df: Dataset[_], model: KMeansModel): Double =
    math.sqrt(model.computeCost(df)/df.count())
}

trait Analytics {
  def averageSquareRootDistance(df: Dataset[_], model: KMeansModel): Double

  def printAntennaActivityComparision(antena: Vector, antenaB: Vector, by: Int = 24): Unit = {
    println("Antenna <-> Antenna")
    antena.toArray.zip(antenaB.toArray).zipWithIndex.foreach( tuple => {
      if(tuple._2 % by == 0) println
      println(s"${tuple._1._1} <-> ${tuple._1._2}")
    })
  }
}