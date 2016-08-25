package cn.cnic.bigdatalab.compute.offline

import cn.cnic.bigdatalab.compute.HiveSQLContextSingleton
import cn.cnic.bigdatalab.compute.notification.KafkaMessagerProducer
import org.apache.spark.{SparkConf, SparkContext};

/**
  * Created by xjzhu on 16/6/24.
  */
object Offline {

  /*def run(createSrcTable : String, srcType:String, createDestTable : String, destType: String, execSql : String) {

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

    run(createSrcTable, "hive", createDestTable, "mysql", execSql)
  }*/


  val failStatus: String = "Failed"
  val succStatus: String = "Successful"

  def run(appName: String, temporaryTableDesc : String, execSql : String,
          notificationTopic : String = "", kafkaBrokerList:String = "") {

    val conf = new SparkConf().setAppName(appName)

    try{
      //get sql context
      val sc = new SparkContext(conf)
      val sqlContext = HiveSQLContextSingleton.getInstance(sc)

      //Execute SQL tasks
      val tableDescList = temporaryTableDesc.split("#-#")
      for( tableDesc <- tableDescList){
        sqlContext.sql(tableDesc)
      }

      sqlContext.sql(execSql)

    }catch {
      case runtime: RuntimeException => {
        if(!(notificationTopic.equals("") || kafkaBrokerList.equals(""))){
          val topic = notificationTopic.split(":")(0)
          //val partition = notificationTopic.split(":")(1)
          KafkaMessagerProducer.produce(topic, kafkaBrokerList,failStatus)
        }
      }

    }

    if(!(notificationTopic.equals("") || kafkaBrokerList.equals(""))){
      val topic = notificationTopic.split(":")(0)
      val partition = notificationTopic.split(":")(1)
      KafkaMessagerProducer.produce(topic, partition, kafkaBrokerList)

    }

  }

  def main(args: Array[String]): Unit = {

    val appname = args(0)
    val temporaryTableDesc = args(1)
    val execSql = args(2)

    var notificationTopic = ""
    var kafkaBrokerList = ""
    if (args.size == 5){
      notificationTopic = args(3)
      kafkaBrokerList = args(4)
    }
    println("TableDest : " + temporaryTableDesc)
    println("Sql statement: " + execSql)
    println("notificationTopic : " + notificationTopic)
    println("kafkaBrokerList: " + kafkaBrokerList)

    run(temporaryTableDesc , execSql, notificationTopic, kafkaBrokerList)
  }


}
