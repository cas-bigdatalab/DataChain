package cn.cnic.bigdatalab.compute.MLlib
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

/**
  * Created by xjzhu@cnic.cn on 2016/10/8.
  */
object MLPipelineTesting {

  def processDataFrame(df:DataFrame): Unit ={
    val model = PipelineModel.load("/tmp/spark-logistic-regression-model")
    model.transform(df).
      select("id", "text", "probability", "prediction")
      .show()
  }
}
