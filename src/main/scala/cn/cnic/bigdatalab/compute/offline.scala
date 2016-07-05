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

    val conf = new SparkConf().setAppName("Offline Compute")

    val sc = new SparkContext(conf)
    val sqlContextType:String = if(srcType == "hive" || destType == "hive") "hive" else ""
    val sqlContext = SelfSQLContext.getInstance(sqlContextType,sc)


    //Execute SQL tasks
    sqlContext.sql(createSrcTable)
    sqlContext.sql(createDestTable)
    sqlContext.sql(execSql)

  }

  def main(args: Array[String]): Unit = {

    //    mysql test
    //        val createDecTable = """
    //                               |CREATE TEMPORARY TABLE test
    //                               |USING org.apache.spark.sql.jdbc
    //                               |OPTIONS (
    //                               |  url    'jdbc:mysql://10.0.71.7:3306/test?user=root&password=root',
    //                               |  dbtable     'user1'
    //                               |)""".stripMargin
    //     mongodb test
    //        val createDecTable = """
    //                               |CREATE TEMPORARY TABLE test(
    //                               | name String, age Int
    //                               |)USING com.stratio.datasource.mongodb
    //                               |OPTIONS (
    //                               |  host '*:27017',
    //                               |  database 'test',
    //                               |  collection 'age'
    //                               |)""".stripMargin

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

    val createSrcTable = args(0)
    val srcType = args(1)
    val createDestTable = args(2)
    val destType = args(3)
    val execSql = args(4)

    println("Create Src Table : " + createSrcTable)
    println("Src Table Type : " + srcType)
    println("Create Dest Table : " + createDestTable)
    println("Dest Table Type : " + destType)
    println("Sql statement: " + execSql)



//    val createSrcTable = """
//                           |CREATE TABLE IF NOT EXISTS test(
//                           |name STRING, age INT
//                           |)""".stripMargin
//
//    val createDestTable = """
//                           |CREATE TEMPORARY TABLE user
//                           |USING org.apache.spark.sql.jdbc
//                           |OPTIONS (
//                           |  url    'jdbc:mysql://172.16.106.3:3306/spark?user=root&password=root',
//                           |  dbtable     'user'
//                           |)""".stripMargin
//
//    val execSql = """Insert into user Select * from test"""


    run(createSrcTable, "hive", createDestTable, "mysql", execSql)
  }


}
