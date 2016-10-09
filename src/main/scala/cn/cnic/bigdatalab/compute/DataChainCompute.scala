package cn.cnic.bigdatalab.compute

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by Flora on 2016/7/20.
  */
class DataChainCompute extends Serializable{
  def processLine(schema: String, line: String): Unit ={

  }

  def processRdd(schema: String, rdd: RDD[String]): Unit ={

  }

  def processDataFrame(df: DataFrame): Unit={

  }

}
