package cn.cnic.bigdatalab.compute

import cn.cnic.bigdatalab.utils.{FieldTypeUtil, StreamingLogLevels}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xjzhu on 16/6/24.
  */
object Offline {

  def run(createSrcTable : String, srcType:String, createDestTable : String, destType: String, execSql : String) {

    val conf = new SparkConf().setAppName("RealTime Compute")
      .setMaster("spark://10.0.71.1:7077")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "10g")
      .set("spark.cores.max", "24")
      .set("spark.driver.allowMultipleContexts", "true")
      .setJars(List("D:\\DataChain\\classes\\artifacts\\datachain_jar\\datachain.jar",
        "D:\\DataChain\\lib\\mongo-java-driver-2.13.0.jar",
        "D:\\DataChain\\lib\\casbah-commons_2.10-2.8.0.jar",
        "D:\\DataChain\\lib\\casbah-core_2.10-2.8.0.jar",
        "D:\\DataChain\\lib\\casbah-query_2.10-2.8.0.jar",
        "D:\\DataChain\\lib\\spark-mongodb_2.10-0.9.3-RC1-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    val sqlContextType:String = if(srcType == "hive" || destType == "hive") "hive" else ""
    val sqlContext = SQLContext.getInstance(sqlContextType,sc)


    //Execute SQL tasks
    sqlContext.sql(createSrcTable)
    sqlContext.sql(createDestTable)
    sqlContext.sql(execSql)

  }

  def main(args: Array[String]): Unit = {

    //mysql test
    //    val createDecTable = """
    //                           |CREATE TEMPORARY TABLE test
    //                           |USING org.apache.spark.sql.jdbc
    //                           |OPTIONS (
    //                           |  url    'jdbc:mysql://10.0.71.7:3306/test?user=root&password=root',
    //                           |  dbtable     'user1'
    //                           |)""".stripMargin
    // mongodb test
    //    val createDecTable = """
    //                           |CREATE TEMPORARY TABLE test(
    //                           | name String, age Int
    //                           |)USING com.stratio.datasource.mongodb
    //                           |OPTIONS (
    //                           |  host '*:27017',
    //                           |  database 'test',
    //                           |  collection 'age'
    //                           |)""".stripMargin

    //    //hive test
    //    val createDecTable = """
    //                           |CREATE TABLE IF NOT EXISTS test(
    //                           |name STRING, age INT
    //                           |)""".stripMargin

    //    //Hbase test
    //    val createDecTable = """
    //                           |CREATE TABLE IF NOT EXISTS hbase_table(key int, name string, age int)
    //                           |STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    //                           |WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:name,cf2:age")
    //                           |TBLPROPERTIES ("hbase.table.name" = "user", "hbase.mapred.output.outputtable" = "user")""".stripMargin

    assert(args.size == 5)
    val createSrcTable = args(1)
    val srcType = args(2)
    val createDecTable = args(3)
    val decType = args(4)
    val execSql = args(5)

    run(createSrcTable, srcType, createDecTable, decType, execSql)
  }


}
