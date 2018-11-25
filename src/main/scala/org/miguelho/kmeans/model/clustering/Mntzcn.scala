package org.miguelho.kmeans.model.clustering

import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.miguelho.kmeans.model.features.ActivityToVector

trait MonthlyEstimatorParams extends Params {
  final val featuresCol = new Param[String](this,"featuresCol","Time Series features Column to be used")
  final val tagCol = new Param[String](this,"tagCol","Time Series Tag to be used as feature")
  final val targetCol = new Param[String](this,"targetCol","Time Series target")
  final val modelList = new StringArrayParam(this,"modelList","The estimation model to be used in the selection")

  final val estimateCol = new Param[String](this,"estimateCol","The estimation for the next period")
  final val selectedModelCol = new Param[String](this,"selectedModel","The model selected for the estimation")

  final val occNTrees = new IntParam(this,"occurrenceNTrees","RF Numbers of Trees for Occurrence")
  final val occMaxDepth= new IntParam(this,"occMaxDepth","RF MaxDepth Occurrence")
  final val selNumPC= new IntParam(this,"selNumPC","Number of Principal Components on selection process")
  final val selNTrees = new IntParam(this,"selNTrees","RF Numbers of Trees for Selection")
  final val selMaxDepth= new IntParam(this,"selMaxDepth","RF MaxDepth Selection")
  final val selMaxBins= new IntParam(this,"selMaxBins","RF MaxBins Selection")

  final val tsDepth = new IntParam(this,"tsDepth","The Size of the Time Series used to train model")

  def setEstimateCol(value: String): this.type = set(estimateCol, value)
  def setSelectedModelCol(value: String): this.type = set(selectedModelCol, value)

  def getTSDepth: Int ={
    $(tsDepth)
  }

  protected def validateAndTransformSchema(schema: StructType): StructType ={
    schema.add(StructField($(estimateCol), DoubleType, nullable = true))
      .add(StructField($(selectedModelCol), StringType, nullable = true))
  }

  val postIndex = "_index"
  val postOHE = "_ohe"
  val postOccurrence = "_occurence"
  val postOFeatures = "_ofeatures"
  val postBestModel = "_best_model"
  val postBMError = "_bm_error"
  val postBMIndex = "_bm_index"
  val postMNorm = "_mnorm"
  val postACR = "_acr"
  val postEStats = "_estats"
  val postSFeatures = "_sfeatures"
  val postPCA = "_pca"
  val predictionField = "prediction"

}

trait MntzcnEstimatorParams extends Params{
  final val inputCol = new Param[String](this,"Date","Daily Activity to work with")
  final val featuresCol = new Param[String](this,"features","Vector representation of inputCol")
  final val numK = new IntParam(this,"numK","Number of Clusters, K")
  final val maxIter= new IntParam(this,"maxIter","Number of iterations for KMeans")
  final val kMeansSeed= new IntParam(this,"seed","Seed to guarantee reproducibility")
  final val estimateCol = new Param[String](this,"estimate","The resulting cluster for the record")

  val predictionField = "prediction"


  protected def validateAndTransformSchema(schema: StructType): StructType ={
    schema.add(StructField($(estimateCol), IntegerType, nullable = true))

  }
}

class MntzcnEstimator(override val uid: String) extends Estimator[MntzcnClusterModel]
  with DefaultParamsWritable
  with MntzcnEstimatorParams {
  def this() = this(Identifiable.randomUID("MntzcnEstimator"))

  def setHyperParams(params:Map[String,String]): this.type = {
    set(inputCol,params("inputCol"))
    .set(featuresCol,params("features"))
    .set(numK,params("kmeans.numK").toInt)
      .set(numK,params("kmeans.numK").toInt)
      .set(maxIter,params("kmeans.maxIter").toInt)
      .set(kMeansSeed,params("kmeans.seed").toInt)
  }

  override def fit(dataset: Dataset[_]): MntzcnClusterModel = {
    val actToVec = new ActivityToVector().setInputCol($(inputCol)).setOutputCol($(featuresCol))

    val kMeans = new KMeans().setFeaturesCol($(featuresCol)).setK($(numK)).setMaxIter($(maxIter)).setSeed($(kMeansSeed))

    val pipeline = new Pipeline().setStages(Array(actToVec,kMeans)).fit(dataset)

    copyValues(new MntzcnClusterModel(uid, pipeline)).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[MntzcnClusterModel] = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


class MntzcnClusterModel(override val uid: String,val baseModel: PipelineModel)
  extends Model[MntzcnClusterModel]
    with MntzcnEstimatorParams
    with MLWritable {

  import MntzcnClusterModel._

  override def copy(extra: ParamMap): MntzcnClusterModel = {
    val copied = new MntzcnClusterModel(uid, baseModel)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new MntzcnClusterModelWriter(this)

  override def transform(dataset: Dataset[_]): DataFrame = {
    baseModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object MntzcnClusterModel extends MLReadable[MntzcnClusterModel]{

  private class MntzcnClusterModelWriter(instance: MntzcnClusterModel) extends MLWriter{
    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriterExposer.t.saveMetadata(instance,path,sc)
      instance.baseModel.write.save(new Path(path,"base").toString)
    }
  }

  private class MntzcnClusterModelReader extends MLReader[MntzcnClusterModel]{

    private val className = classOf[MntzcnClusterModel].getName

    override def load(path: String): MntzcnClusterModel = {
      val metadata = DefaultParamsReaderExposer.t.loadMetadata(path, sc, className)
      val base = PipelineModel.read.load(new Path(path,"base").toString)
      val model=new MntzcnClusterModel(metadata.uid,base)
      DefaultParamsReaderExposer.t.getAndSetParams(model,metadata)
      model
    }
  }

  override def read: MLReader[MntzcnClusterModel] = new MntzcnClusterModelReader
}