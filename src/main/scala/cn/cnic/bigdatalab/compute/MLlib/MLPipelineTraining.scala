package cn.cnic.bigdatalab.compute.MLlib
import cn.cnic.bigdatalab.compute.HiveSQLContextSingleton
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{DataFrame, Row}
/**
  * Created by xjzhu@cnic.cn on 2016/10/8.
  */
object MLPipelineTraining {

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("MLlib Test!!!")
    /*.setMaster("spark://10.0.71.32:7077")
    .set("spark.driver.memory", "1g")
    .set("spark.executor.memory", "2g")
    .set("spark.cores.max", "4")
    .set("spark.driver.allowMultipleContexts", "true")
    .setJars(List("E:\\Project\\DataChain\\out\\artifacts\\datachain_jar\\datachain.jar"))*/

    val sc = new SparkContext(conf)
    val sqlContext = HiveSQLContextSingleton.getInstance(sc)

    val trainingDataFrame = sqlContext.createDataFrame(Seq(
      (0L,"a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id","text","label")

    processDataFrame(trainingDataFrame)
  }

  def processDataFrame(trainingDataFrame:DataFrame): Unit ={
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(trainingDataFrame)

    model.save("/tmp/spark-logistic-regression-model")
  }

}
