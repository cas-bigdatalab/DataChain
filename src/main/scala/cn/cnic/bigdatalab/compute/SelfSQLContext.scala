package cn.cnic.bigdatalab.compute

import cn.cnic.bigdatalab.utils.PropertyUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xjzhu on 16/6/24.
  */
object SelfSQLContext {

  def getInstance(sqlType : String, sparkContext : SparkContext): SQLContext = sqlType.toLowerCase() match {
    case "hive" => HiveSQLContextSingleton.getInstance(sparkContext)
    case _ => SQLContextSingleton.getInstance(sparkContext)
  }

}

object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object HiveSQLContextSingleton {

  @transient private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
      instance.sql("set hive.exec.dynamic.partition=true")
      instance.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      instance.sql("SET hive.merge.size.per.task=256000000")
      instance.sql("SET hive.merge.mapfiles=true")
      instance.sql("SET hive.merge.mapredfiles=true")
      instance.sql("SET hive.merge.sparkfiles=true")
    }
    instance
  }
}
