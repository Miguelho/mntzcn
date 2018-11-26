package org.miguelho.kmeans.model.clustering

import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.miguelho.kmeans.model.features.ActivityToVector

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
