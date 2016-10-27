package cn.cnic.bigdatalab.compute

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

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

  def processParameters(sc: SparkContext, parameters:Map[String,String]): Unit ={

  }
}
